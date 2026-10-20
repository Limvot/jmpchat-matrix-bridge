package connector

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Inbound retractions (XEP-0424) and moderation (XEP-0425) become a simplevent.MessageRemove whose
// pre-handler resolves and validates the target in the portal event loop. Nothing here runs on the
// XMPP Serve goroutine except building the event.

// Channels a retraction can arrive on; they only matter for validation of 1:1 retractions.
const (
	retractChanLive           = "live"
	retractChanCarbonSent     = "carbon_sent"
	retractChanCarbonReceived = "carbon_received"
	retractChanMAM            = "mam"

	retractKindModeration = "moderation"
	retractKindRetraction = "retraction"

	retractedKeyPrefix = "jabber_retracted:"
)

// retraction is one inbound retraction stanza with everything validation needs.
type retraction struct {
	info       retractInfo
	channel    string // retractChan*
	groupchat  bool   // the stanza had type=groupchat (MUC handler)
	msgType    string // stanza type attribute (inner message for carbons/MAM)
	fromFull   string // sender of the retraction stanza (inner `from` for carbons/MAM)
	occupantID string // top-level XEP-0421 occupant-id of the retraction stanza
}

// isModeration reports whether the stanza is a XEP-0425 moderation: it names a <moderated> child, or
// it is a groupchat retraction from the bare room JID (some modules omit <moderated>).
func (r retraction) isModeration() bool {
	return r.info.Moderated || (r.groupchat && r.fromFull != "" && !strings.Contains(r.fromFull, "/"))
}

func (r retraction) kind() string {
	if r.isModeration() {
		return retractKindModeration
	}
	return retractKindRetraction
}

// refKind picks the lookup order (see refLookupPlan): moderation and MUC self-retraction name a room
// stanza-id, 1:1 retractions name the original's id.
func (r retraction) refKind() refKind {
	switch {
	case r.isModeration():
		return refMUCModeration
	case r.groupchat:
		return refMUCSelf
	}
	return refDM
}

// portalFacts is what validateRetraction needs to know about the portal.
type portalFacts struct {
	key             networkid.PortalKey
	isMUC           bool
	isGroupMMS      bool // Cheogram group MMS: senders are synthesized and cannot be validated
	loginID         networkid.UserLoginID
	ourOccupantFull string // our occupant JID in this MUC, "" when unknown
}

func targetMeta(m *database.Message) *MessageMetadata {
	if m == nil {
		return nil
	}
	meta, _ := m.Metadata.(*MessageMetadata)
	return meta
}

// validateRetraction decides whether the stanza described by r may remove target, a row that was
// resolved in portal p. reason is an enum string (never user content) when it may not.
func validateRetraction(r retraction, p portalFacts, target *database.Message) (ok bool, reason string) {
	if target == nil {
		return false, "not_found"
	}
	// bridgev2 resolves message IDs without a room check; never remove another portal's message.
	if target.Room != p.key {
		return false, "wrong_portal"
	}
	portalID := string(p.key.ID)
	switch {
	case r.isModeration():
		// Only the room itself (bare JID, type=groupchat) may moderate; the moderator identity is informational.
		switch {
		case !r.groupchat:
			return false, "moderation_not_groupchat"
		case r.fromFull == "" || strings.Contains(r.fromFull, "/"):
			return false, "moderation_not_from_room_bare"
		case !strings.EqualFold(r.fromFull, portalID):
			return false, "moderation_wrong_room"
		case !p.isMUC:
			return false, "moderation_portal_not_muc"
		}
		return true, ""
	case r.groupchat:
		if !p.isMUC {
			return false, "retraction_portal_not_muc"
		}
		if !strings.EqualFold(bareJID(r.fromFull), portalID) {
			return false, "retraction_wrong_room"
		}
		if meta := targetMeta(target); meta != nil && meta.OccupantID != "" {
			// Strict: with an occupant-id on the target only an equal one counts, never the nick
			// (a nick can be reused by someone else after the owner left).
			if r.occupantID == "" || r.occupantID != meta.OccupantID {
				return false, "occupant_id_mismatch"
			}
			return true, ""
		}
		if r.occupantID != "" {
			// The room stamps occupant-ids but the target has none (stored before occupant-ids were
			// persisted): a nick or our current occupant JID proves nothing, since another occupant may
			// hold it now (or held it while we were away and the retraction is a history replay).
			return false, "target_without_occupant_id"
		}
		if r.fromFull == string(target.SenderID) {
			return true, ""
		}
		if string(target.SenderID) == string(p.loginID) && p.ourOccupantFull != "" && r.fromFull == p.ourOccupantFull {
			return true, ""
		}
		return false, "occupant_mismatch"
	}
	// 1:1 retraction.
	switch r.msgType {
	case "", "normal", "chat":
	default:
		return false, "bad_message_type"
	}
	if p.isMUC {
		// e.g. a type=chat private message from room/nick lands in the MUC portal.
		return false, "dm_retraction_in_muc"
	}
	if p.isGroupMMS {
		return false, "group_mms"
	}
	fromBare := bareJID(r.fromFull)
	if r.channel == retractChanCarbonSent {
		if strings.EqualFold(fromBare, string(p.loginID)) && strings.EqualFold(string(target.SenderID), string(p.loginID)) {
			return true, ""
		}
		return false, "carbon_sent_not_own"
	}
	if fromBare != "" && strings.EqualFold(fromBare, string(target.SenderID)) {
		return true, ""
	}
	return false, "sender_mismatch"
}

// pickRetractionTarget returns the first candidate (lookup order) that validates, or the first
// rejection reason ("not_found" when there are no candidates).
func pickRetractionTarget(r retraction, p portalFacts, cands []*database.Message) (*database.Message, string) {
	reason := "not_found"
	for i, c := range cands {
		ok, why := validateRetraction(r, p, c)
		if ok {
			return c, ""
		}
		if i == 0 {
			reason = why
		}
	}
	return nil, reason
}

// retractionEnv is the seam between the pre/post handlers and bridgev2/the XMPP client, so the
// handlers can be tested without a bridge.
type retractionEnv interface {
	findByRef(ctx context.Context, key networkid.PortalKey, ref string, kind refKind) ([]*database.Message, error)
	facts(ctx context.Context, portal *bridgev2.Portal) portalFacts
	// removeWithReason redacts parts on Matrix with the reason set and deletes their rows.
	removeWithReason(ctx context.Context, portal *bridgev2.Portal, parts []*database.Message, reason string, ts time.Time, moderation bool) bridgev2.EventHandlingResult
	markRetracted(ctx context.Context, key networkid.PortalKey, parts []*database.Message)
	rowsGone(ctx context.Context, portal *bridgev2.Portal, id networkid.MessageID) bool
	log(ctx context.Context) *zerolog.Logger
}

// retractionState is shared between the pre and post handler of one event.
type retractionState struct {
	parts []*database.Message // rows to tombstone after the framework removed them
	hctx  context.Context     // ctx carrying the handling span started by the pre-handler (nil if it did not run)
}

// newRetractionEvent builds the remote event for a retraction. Its target is the raw reference until
// the pre-handler resolves and validates it and swaps in the bridgev2 message ID. CreatePortal is
// false (a retraction never creates a room); the sender stays zero so bridgev2 picks the sender
// ghost of the original message as intent.
func newRetractionEvent(env retractionEnv, portalKey networkid.PortalKey, r retraction, ts time.Time) *simplevent.MessageRemove {
	st := &retractionState{}
	evt := &simplevent.MessageRemove{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventMessageRemove,
			PortalKey: portalKey,
			Timestamp: ts,
			LogContext: func(c zerolog.Context) zerolog.Context {
				return c.Str("retract_target", r.info.Target).Str("retract_kind", r.kind())
			},
		},
		TargetMessage:   networkid.MessageID(r.info.Target),
		HidePlaceholder: false,
	}
	evt.PreHandleWithResultFunc = func(ctx context.Context, portal *bridgev2.Portal) *bridgev2.EventHandlingResult {
		return preHandleRetraction(ctx, env, evt, st, r, portal)
	}
	evt.PostHandleFunc = func(ctx context.Context, portal *bridgev2.Portal) {
		if st.hctx != nil {
			ctx = st.hctx // handling span and trace-annotated logger from the pre-handler
		}
		postHandleRetraction(ctx, env, st, portalKey, portal)
		telemetry.EndRemoteEvent(ctx)
	}
	return evt
}

func partsOfID(cands []*database.Message, id networkid.MessageID) []*database.Message {
	var out []*database.Message
	for _, c := range cands {
		if c.ID == id {
			out = append(out, c)
		}
	}
	return out
}

// preHandleRetraction resolves and validates the target. On success it sets evt.TargetMessage to the
// validated message ID. It returns nil to let bridgev2 redact (no reason), or a result when the event
// is finished here: Ignored (unknown/invalid target), Failed, or Success after redacting with the
// moderation reason ourselves. bridgev2 does not call PostHandle when a result is returned, so the
// handling span is ended here (it is started here too).
func preHandleRetraction(ctx context.Context, env retractionEnv, evt *simplevent.MessageRemove, st *retractionState, r retraction, portal *bridgev2.Portal) *bridgev2.EventHandlingResult {
	// The handling span starts here, not in MutateContext: bridgev2 ignores events of portals without
	// a room before any handler runs, which would leak a span started earlier.
	ctx = startLazyRemoteEvent(ctx)
	st.hctx = ctx
	rctx, span := telemetry.StartChild(ctx, "bridge.resolve_retract_target")
	defer span.End()
	done := func(res bridgev2.EventHandlingResult) *bridgev2.EventHandlingResult {
		telemetry.EndRemoteEvent(ctx)
		return &res
	}
	log := env.log(rctx)
	span.SetAttributes(attribute.String("bridge.retract.kind", r.kind()), attribute.String("bridge.retract.channel", r.channel))

	key := evt.GetPortalKey()
	cands, err := env.findByRef(rctx, key, r.info.Target, r.refKind())
	if err != nil {
		telemetry.Fail(span, err)
		log.Err(err).Msg("Failed to look up retraction target")
		return done(bridgev2.EventHandlingResultFailed.WithError(err))
	}
	target, reason := pickRetractionTarget(r, env.facts(rctx, portal), cands)
	span.SetAttributes(attribute.Bool("bridge.found", target != nil))
	if target == nil {
		span.SetAttributes(attribute.String("bridge.rejected_reason", reason))
		log.Debug().Str("reason", reason).Str("retract_kind", r.kind()).Msg("Ignoring retraction")
		return done(bridgev2.EventHandlingResultIgnored)
	}
	evt.TargetMessage = target.ID
	parts := partsOfID(cands, target.ID)
	if r.info.Reason != "" {
		// bridgev2 cannot attach a reason to a remote redaction, so send it ourselves.
		span.SetAttributes(attribute.Bool("bridge.retract.reason_path", true))
		res := env.removeWithReason(rctx, portal, parts, r.info.Reason, evt.GetTimestamp(), r.isModeration())
		if res.Success {
			env.markRetracted(rctx, key, parts)
		} else {
			log.Warn().Err(res.Error).Msg("Failed to redact retracted message with reason")
		}
		return done(res)
	}
	st.parts = parts
	return nil
}

// postHandleRetraction records the tombstone once bridgev2 really removed the rows.
func postHandleRetraction(ctx context.Context, env retractionEnv, st *retractionState, key networkid.PortalKey, portal *bridgev2.Portal) {
	if len(st.parts) == 0 {
		return
	}
	parts := st.parts
	st.parts = nil
	if env.rowsGone(ctx, portal, parts[0].ID) {
		env.markRetracted(ctx, key, parts)
	}
}

// ---- tombstones (retracted refs) ----

// Tombstone scopes. A tombstone is only valid for the sender of the retracted message, otherwise an
// occupant could pick the id attribute of a message to suppress that message (or a counter-id
// client's later ones) on replay.
const retractScopeStanza = "stanza" // room/archive-assigned stanza-id: unique and unspoofable, any sender

// retractedRef is one tombstone: a scope (retractScopeStanza, or a sender scope) and a reference.
type retractedRef struct{ Scope, Ref string }

// senderScopes returns the sender scopes of a message: always `snd=<sender id as stored>` (bare JID
// for 1:1, room/nick for MUC; only the bare part is lower-cased, MUC nicks are case-sensitive so
// "Bob" must not share tombstones with "bob") and, when present, `occ=<occupant-id>` (XEP-0421).
// Values are query-escaped so that the ':' separators of a key stay unambiguous.
func senderScopes(senderID, occupantID string) []string {
	norm := strings.ToLower(senderID)
	if i := strings.IndexByte(senderID, '/'); i >= 0 {
		norm = strings.ToLower(senderID[:i]) + senderID[i:]
	}
	out := []string{"snd=" + url.QueryEscape(norm)}
	if occupantID != "" {
		out = append(out, "occ="+url.QueryEscape(occupantID))
	}
	return out
}

// retractedKey is the KV key of one tombstone. Format:
// jabber_retracted:<login id>:<lower-cased portal id>:<scope>:<ref> with scope one of `stanza`,
// `snd=<escaped lower-cased sender id>` or `occ=<escaped occupant-id>`. The value is the unix time it
// was recorded. Entries are never garbage collected (a few tiny rows per retracted message).
func retractedKey(login networkid.UserLoginID, portal networkid.PortalID, scope, ref string) database.Key {
	return database.Key(retractedKeyPrefix + string(login) + ":" + strings.ToLower(string(portal)) + ":" + scope + ":" + ref)
}

// retractedRefsOf returns the tombstones of the message rows: every XMPP reference (their IDs and the
// id aliases in metadata, without the synthetic matrix:/empty ones) under each sender scope of the
// row, plus the room/archive-assigned stanza-id under the sender-independent stanza scope.
func retractedRefsOf(parts []*database.Message) []retractedRef {
	seen := map[retractedRef]struct{}{}
	var out []retractedRef
	add := func(scope, ref string) {
		if ref == "" || strings.HasPrefix(ref, "matrix:") {
			return
		}
		r := retractedRef{scope, ref}
		if _, ok := seen[r]; ok {
			return
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	for _, p := range parts {
		meta := targetMeta(p)
		occ := ""
		if meta != nil {
			occ = meta.OccupantID
		}
		scopes := senderScopes(string(p.SenderID), occ)
		addAll := func(ref string) {
			for _, sc := range scopes {
				add(sc, ref)
			}
		}
		addAll(string(p.ID))
		if meta != nil {
			addAll(meta.XMPPMessageID)
			addAll(meta.StanzaID)
			addAll(meta.OriginID)
			add(retractScopeStanza, meta.StanzaID)
		}
	}
	return out
}

func markRetractedRefs(ctx context.Context, kv *database.KVQuery, login networkid.UserLoginID, portal networkid.PortalID, refs []retractedRef) {
	now := strconv.FormatInt(time.Now().Unix(), 10)
	for _, r := range refs {
		kv.Set(ctx, retractedKey(login, portal, r.Scope, r.Ref), now)
	}
}

// retractedSender identifies the sender of an incoming message for tombstone checks.
type retractedSender struct {
	senderID   string // as it would be stored in the message row
	occupantID string
	stanzaID   string // trusted room/archive stanza-id, "" if none
}

func anyRetractedRef(ctx context.Context, kv *database.KVQuery, login networkid.UserLoginID, portal networkid.PortalID, who retractedSender, refs ...string) bool {
	if who.stanzaID != "" && kv.Get(ctx, retractedKey(login, portal, retractScopeStanza, who.stanzaID)) != "" {
		return true
	}
	for _, sc := range senderScopes(who.senderID, who.occupantID) {
		for _, ref := range refs {
			if ref == "" {
				continue
			}
			if kv.Get(ctx, retractedKey(login, portal, sc, ref)) != "" {
				return true
			}
		}
	}
	return false
}

func (x *xmppClient) kv() *database.KVQuery {
	if x == nil || x.conn == nil || x.conn.br == nil || x.conn.br.DB == nil {
		return nil
	}
	return x.conn.br.DB.KV
}

// isRetracted reports whether the stanza-id of who, or any of refs under the sender's scopes, was
// recorded as retracted in the portal. It does DB
// lookups: never call it on the Serve goroutine.
func (x *xmppClient) isRetracted(ctx context.Context, portal networkid.PortalID, who retractedSender, refs ...string) bool {
	kv := x.kv()
	if kv == nil {
		return false
	}
	return anyRetractedRef(ctx, kv, x.loginID, portal, who, refs...)
}

// ---- real environment ----

type xmppRetractionEnv struct{ x *xmppClient }

func (e xmppRetractionEnv) findByRef(ctx context.Context, key networkid.PortalKey, ref string, kind refKind) ([]*database.Message, error) {
	return findMessagesByXMPPRef(ctx, e.x.conn.br.DB, key, ref, kind)
}

func (e xmppRetractionEnv) facts(ctx context.Context, portal *bridgev2.Portal) portalFacts {
	p := portalFacts{key: portal.PortalKey, loginID: e.x.loginID}
	p.isMUC = e.x.portalIsMUC(ctx, portal)
	_, p.isGroupMMS = ParseCheogramGroupMMSJID(string(portal.ID))
	if p.isMUC {
		p.ourOccupantFull = e.x.getMUCOurOccupantJID(string(portal.ID))
	}
	return p
}

func (e xmppRetractionEnv) markRetracted(ctx context.Context, key networkid.PortalKey, parts []*database.Message) {
	if kv := e.x.kv(); kv != nil {
		markRetractedRefs(ctx, kv, e.x.loginID, key.ID, retractedRefsOf(parts))
	}
}

func (e xmppRetractionEnv) rowsGone(ctx context.Context, portal *bridgev2.Portal, id networkid.MessageID) bool {
	rows, err := e.x.conn.br.DB.Message.GetAllPartsByID(ctx, portal.Receiver, id)
	return err == nil && len(rows) == 0
}

func (e xmppRetractionEnv) log(ctx context.Context) *zerolog.Logger { return e.x.logFor(ctx) }

// senderIntent returns the intent bridgev2 would use to remove a message of this part's sender (the
// ghost, or the double puppet), or nil.
func senderIntent(ctx context.Context, br *bridgev2.Bridge, part *database.Message) bridgev2.MatrixAPI {
	mxid := part.SenderMXID
	switch {
	case mxid == "":
		return nil
	case mxid == br.Bot.GetMXID():
		return br.Bot
	}
	if ghost, err := br.GetGhostByMXID(ctx, mxid); err == nil && ghost != nil {
		return ghost.Intent
	}
	if user, err := br.GetExistingUserByMXID(ctx, mxid); err == nil && user != nil {
		return user.DoublePuppet(ctx)
	}
	return nil
}

// removeWithReason redacts every part with the reason and then deletes the rows. Moderation is
// attributed to the bridge bot (the room authority) and falls back to the sender's own intent when the
// bot may not redact; a retraction by the sender uses the sender's intent (what bridgev2 does) and falls
// back to the bot.
func (e xmppRetractionEnv) removeWithReason(ctx context.Context, portal *bridgev2.Portal, parts []*database.Message, reason string, ts time.Time, moderation bool) bridgev2.EventHandlingResult {
	if len(parts) == 0 {
		return bridgev2.EventHandlingResultIgnored
	}
	br := e.x.conn.br
	intents := func(p *database.Message) []bridgev2.MatrixAPI {
		own := senderIntent(ctx, br, p)
		var list []bridgev2.MatrixAPI
		if moderation {
			list = append(list, br.Bot)
			if own != nil && own != br.Bot {
				list = append(list, own)
			}
		} else {
			if own != nil {
				list = append(list, own)
			}
			list = append(list, br.Bot)
		}
		return list
	}
	if err := redactPartsWithReason(ctx, portal.MXID, parts, intents, reason, ts); err != nil {
		return bridgev2.EventHandlingResultFailed.WithError(err)
	}
	if err := br.DB.Message.DeleteAllParts(ctx, portal.Receiver, parts[0].ID); err != nil {
		return bridgev2.EventHandlingResultFailed.WithError(fmt.Errorf("delete retracted message: %w", err))
	}
	return bridgev2.EventHandlingResultSuccess
}

// redactPartsWithReason sends one m.room.redaction (with reason, default placeholder) per part that has a
// real Matrix event, trying each intent in order until one succeeds. Fake-MXID parts are skipped.
func redactPartsWithReason(ctx context.Context, room id.RoomID, parts []*database.Message, intents func(*database.Message) []bridgev2.MatrixAPI, reason string, ts time.Time) error {
	var errs []error
	for _, part := range parts {
		if part.HasFakeMXID() {
			continue
		}
		var lastErr error
		sent := false
		for _, intent := range intents(part) {
			_, err := intent.SendMessage(ctx, room, event.EventRedaction, &event.Content{
				Parsed: &event.RedactionEventContent{Redacts: part.MXID, Reason: reason},
			}, &bridgev2.MatrixSendExtra{Timestamp: ts, MessageMeta: part})
			if err == nil {
				sent = true
				break
			}
			lastErr = err
		}
		if !sent {
			if lastErr == nil {
				lastErr = errors.New("no intent available to redact message")
			}
			errs = append(errs, lastErr)
		}
	}
	return errors.Join(errs...)
}

// ---- queueing ----

// queueRetraction queues the retraction event through queueRemoteEvent (span context + handling span).
func (x *xmppClient) queueRetraction(ctx context.Context, portalKey networkid.PortalKey, r retraction, ts time.Time) bridgev2.EventHandlingResult {
	return x.queueRemoteEvent(ctx, newRetractionEvent(xmppRetractionEnv{x}, portalKey, r, ts))
}

// handleInboundRetraction is the shared live/carbon entry point (Serve goroutine: builds and queues
// the event only, no DB or IQ work). trustedBy is the entity whose <stanza-id> is believed (room bare
// JID or our own account) and only feeds the debug line. It never logs the reason or the fallback body.
func (x *xmppClient) handleInboundRetraction(r retraction, portalKey networkid.PortalKey, stanzaID string, ids messageIDs, trustedBy string) {
	ctx, span := startReceiveSpan(r.kind(), r.msgType, r.fromFull, string(portalKey.ID), stanzaID, false, false, false)
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			attribute.Bool("xmpp.retract.moderation", r.isModeration()),
			attribute.Bool("xmpp.retract.has_reason", r.info.HasReason),
			attribute.Bool("xmpp.retract.legacy_ns", r.info.Legacy || r.info.BothForms),
		)
	}
	x.logFor(ctx).Debug().
		Str("channel", r.channel).
		Str("form", r.info.form()).
		Str("kind", r.kind()).
		Str("target", r.info.Target).
		Bool("has_moderated", r.info.Moderated).
		Bool("moderated_by_present", r.info.ModeratedByPresent).
		Bool("has_reason", r.info.HasReason).
		Bool("has_fallback", r.info.HasFallback).
		Bool("occupant_id_present", r.occupantID != "").
		Bool("stanza_id_by_matched", ids.stanzaIDBy(trustedBy) != "").
		Msg("Inbound message retraction")
	x.queueRetraction(ctx, portalKey, r, time.Now())
}

// ---- MAM ----

// batchRetractions maps a retracted reference to the lower-cased bare JIDs that retracted it in one
// fetched MAM batch.
type batchRetractions map[string][]string

// collectBatchRetractions gathers the 1:1 retractions of a batch (moderation is MUC only and MUC MAM
// is not queried). Call it on ALL fetched items before any truncation.
func collectBatchRetractions(items []mamResultItem) batchRetractions {
	var out batchRetractions
	for _, item := range items {
		if item.Retract == nil || item.Retract.Target == "" || item.Retract.Moderated || item.Type == "groupchat" {
			continue
		}
		if out == nil {
			out = batchRetractions{}
		}
		out[item.Retract.Target] = append(out[item.Retract.Target], strings.ToLower(bareJID(item.From)))
	}
	return out
}

// hides reports whether the item is an original that a retraction in the same batch removes. The
// retraction must come from the same bare JID as the original, otherwise a contact could hide our
// messages from backfill.
func (b batchRetractions) hides(item mamResultItem) bool {
	if len(b) == 0 || item.Retract != nil {
		return false
	}
	from := strings.ToLower(bareJID(item.From))
	for _, ref := range []string{item.StanzaID, item.ArchiveID, item.OriginID} {
		for _, by := range b[ref] {
			if ref != "" && by == from {
				return true
			}
		}
	}
	return false
}

// mamItemRetracted reports whether a retraction that was already processed covers the item (see
// retractedRefsOf). DB lookups: not for the Serve goroutine.
func (x *xmppClient) mamItemRetracted(ctx context.Context, portalKey networkid.PortalKey, item mamResultItem) bool {
	who := retractedSender{senderID: bareJID(item.From), stanzaID: item.ArchiveID}
	return x.isRetracted(ctx, portalKey.ID, who, item.StanzaID, item.ArchiveID, item.OriginID)
}

// mamItemHidden reports whether the item must not be bridged because a retraction in the same batch
// or an earlier one covers it. An original hidden by the batch is tombstoned (its sender already
// matched the retraction's), so a later page without the retraction cannot resurrect it. Runs in the
// catch-up/backfill goroutines (DB writes), never on the Serve goroutine.
func (x *xmppClient) mamItemHidden(ctx context.Context, portalKey networkid.PortalKey, item mamResultItem, b batchRetractions) bool {
	if b.hides(item) {
		if kv := x.kv(); kv != nil {
			var refs []retractedRef
			for _, sc := range senderScopes(bareJID(item.From), "") {
				for _, ref := range []string{item.StanzaID, item.ArchiveID, item.OriginID} {
					if ref != "" {
						refs = append(refs, retractedRef{sc, ref})
					}
				}
			}
			if item.ArchiveID != "" {
				refs = append(refs, retractedRef{retractScopeStanza, item.ArchiveID})
			}
			markRetractedRefs(ctx, kv, x.loginID, portalKey.ID, refs)
		}
		return true
	}
	return x.mamItemRetracted(ctx, portalKey, item)
}

// queueMAMRetraction queues the retraction event of an archived retraction item (validated like a
// live one) and reports whether the item was a retraction. It never registers the portal for catch-up
// or triggers a backfill: a retraction must not create anything.
func (x *xmppClient) queueMAMRetraction(ctx context.Context, portalKey networkid.PortalKey, item mamResultItem) bool {
	if item.Retract == nil {
		return false
	}
	if item.Retract.Target == "" {
		return true
	}
	ts := item.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	x.queueRetraction(ctx, portalKey, retraction{
		info:     *item.Retract,
		channel:  retractChanMAM,
		msgType:  item.Type,
		fromFull: item.From,
	}, ts)
	return true
}

// mamItemBackfillable is the cheap (no DB) part of the FetchMessages filter: retractions themselves
// (their fallback text is dropped), server tombstones of retracted originals, body-less items (chat
// states) and originals removed by a retraction in the same batch are never backfilled.
func mamItemBackfillable(item mamResultItem, b batchRetractions) bool {
	return item.Retract == nil && !item.Retracted && item.Body != "" && !b.hides(item)
}
