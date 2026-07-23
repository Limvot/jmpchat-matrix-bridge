package connector

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"mellium.im/xmpp"
	"mellium.im/xmpp/disco"
	"mellium.im/xmlstream"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/roster"
	"mellium.im/xmpp/stanza"
	"mellium.im/xmpp/upload"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
)

var (
	_ bridgev2.NetworkAPI                        = (*xmppClient)(nil)
	_ bridgev2.IdentifierResolvingNetworkAPI   = (*xmppClient)(nil)
	_ bridgev2.ReactionHandlingNetworkAPI       = (*xmppClient)(nil)
	_ bridgev2.BackfillingNetworkAPI           = (*xmppClient)(nil)
	_ bridgev2.PersonalFilteringCustomizingNetworkAPI = (*xmppClient)(nil)
)

type xmppClient struct {
	loginID     networkid.UserLoginID
	meta        *JabberLoginMeta
	log         zerolog.Logger
	login       *bridgev2.UserLogin
	conn        *JabberConnector
	mu          sync.Mutex
	session     *xmpp.Session
	uploadJID   *jid.JID // Cached upload service JID (discovered via common subdomains)
	uploadJIDMu sync.Mutex
	// roster holds the latest fetched XMPP roster for this login, keyed by bare JID.
	// Used to map Jabber contact names to Matrix ghost display names and DM room names.
	roster   map[string]string
	rosterMu sync.RWMutex
	// mucCache caches disco-based MUC detection per JID so we don't re-query every time.
	mucCache   map[string]bool
	mucCacheMu sync.RWMutex
	// mucOurOccupantJID maps MUC room JID (bare) to our full occupant JID (room@service/nick) for self-echo filtering.
	mucOurOccupantJID   map[string]string
	mucOurOccupantJIDMu sync.RWMutex
	// mucPendingJoin maps MUC room JID to the nick we used when sending join presence; cleared when we get our presence or error.
	mucPendingJoin   map[string]string
	mucPendingJoinMu sync.Mutex
	// mam holds state for in-flight MAM (XEP-0313) queries for backfill/catch-up.
	mam mamState
	// lastMAMSync tracks the last MAM catch-up time per portal (key = portal ID) for periodic sync.
	lastMAMSync   map[string]time.Time
	lastMAMSyncMu sync.Mutex
	// knownPortalIDsForMAM holds portal IDs we've seen (from inbound or carbon) so we run MAM catch-up
	// for them even if they're not in the roster (e.g. Cheogram group MMS).
	knownPortalIDsForMAM   map[string]struct{}
	knownPortalIDsForMAMMu sync.Mutex
	// lastBackfillOnView tracks when we last ran mamBackfillOnView per portal (fallback when user sends message).
	lastBackfillOnView   map[string]time.Time
	lastBackfillOnViewMu sync.Mutex
	// carbonsEnableCh receives the result of the carbons enable IQ (nil = success, err = server rejected).
	carbonsEnableCh   chan error
	carbonsEnableID   string
	carbonsEnableMu   sync.Mutex
	// reconnectCancel stops ReconnectLoop when the user logs out; set in LoadUserLogin, called in LogoutRemote.
	reconnectCancel context.CancelFunc
	// callSessions tracks in-flight XEP-0353 Jingle Message Initiation calls, keyed by session id,
	// so accept/reject/finish (which only carry the sid) can be mapped back to the caller's portal.
	callSessions   map[string]*callSession
	callSessionsMu sync.Mutex
}

const (
	mucFeatureNS       = "http://jabber.org/protocol/muc"
	mucRoomInfoFormNS  = "http://jabber.org/protocol/muc#roominfo"
)

func (x *xmppClient) Connect(ctx context.Context) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.session != nil {
		return
	}
	if x.login != nil {
		x.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnecting})
	}
	origin, err := jid.Parse(string(x.loginID))
	if err != nil {
		if x.login != nil {
			x.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateTransientDisconnect, Message: "invalid JID"})
		}
		return
	}
	sess, err := dialXMPP(ctx, origin, x.meta.Password, &x.log)
	if err != nil {
		if x.login != nil {
			x.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateTransientDisconnect, Message: "connection failed"})
		}
		return
	}
	x.session = sess

	if x.login != nil {
		x.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
	}

	// Send initial presence so the server delivers messages to this resource.
	// Without this, many servers (including Snikket/Prosody) do not push messages to the client.
	if err := sess.Send(ctx, stanza.Presence{}.Wrap(nil)); err != nil {
		x.log.Warn().Err(err).Msg("Failed to send initial presence; message delivery may not work")
	} else {
		x.log.Debug().Msg("Sent initial presence (available)")
	}

	// #region agent log
	x.log.Debug().Str("hypothesis", "H1").Msg("DEBUG: Connect about to start serveMessages")
	// #endregion
	go x.serveMessages()
	// Release lock so serveMessages can acquire it and run Serve (processes carbons response).
	// Otherwise we deadlock: Connect holds lock, enableCarbons waits for response, but
	// serveMessages blocks on lock so Serve never runs and response is never processed.
	x.mu.Unlock()
	// Brief yield so Serve is reading before we send carbons enable.
	select {
	case <-ctx.Done():
		// #region agent log
		x.log.Debug().Str("hypothesis", "H5").Msg("DEBUG: Connect ctx.Done during sleep")
		// #endregion
		x.mu.Lock()
		return
	case <-time.After(100 * time.Millisecond):
	}
	// #region agent log
	x.log.Debug().Str("hypothesis", "H1").Msg("DEBUG: Connect sleep done, calling enableCarbons")
	// #endregion
	// Enable XEP-0280 Message Carbons so we receive copies of messages sent/received on other clients.
	if err := x.enableCarbons(ctx); err != nil {
		x.log.Warn().Err(err).Msg("Failed to enable message carbons; other-client sync may use MAM catch-up only")
	} else {
		// #region agent log
		x.log.Debug().Str("hypothesis", "H4").Msg("DEBUG: Connect enableCarbons succeeded")
		// #endregion
		x.log.Debug().Msg("Enabled message carbons (XEP-0280)")
	}
	// #region agent log
	x.log.Debug().Str("hypothesis", "H1").Msg("DEBUG: Connect after enableCarbons")
	// #endregion
	x.mu.Lock() // Re-acquire before return (defer will unlock)
	// Periodic MAM catch-up so messages sent from other clients appear within ~45s if carbons aren't used.
	go x.runMAMCatchUpLoop(context.Background())
}

// enableCarbons sends the XEP-0280 enable IQ and waits for the server response.
// Uses bare JID per XEP-0280. Logs and returns error if server rejects.
func (x *xmppClient) enableCarbons(ctx context.Context) error {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil
	}
	local := sess.LocalAddr()
	if local.String() == "" {
		return nil
	}
	x.carbonsEnableMu.Lock()
	x.carbonsEnableID = "carbons-enable-" + xid.New().String()
	x.carbonsEnableCh = make(chan error, 1)
	x.carbonsEnableMu.Unlock()
	// #region agent log
	x.log.Debug().Str("hypothesis", "H2").Str("carbonsEnableID", x.carbonsEnableID).Msg("DEBUG: enableCarbons sent IQ, waiting for response")
	// #endregion
	enableStart := xml.StartElement{
		Name: xml.Name{Space: carbonsNS, Local: "enable"},
	}
	payload := xmlstream.Wrap(nil, enableStart)
	iq := stanza.IQ{ID: x.carbonsEnableID, Type: stanza.SetIQ, To: local.Bare()}
	if err := sess.Send(ctx, iq.Wrap(payload)); err != nil {
		x.carbonsEnableMu.Lock()
		x.carbonsEnableID = ""
		x.carbonsEnableCh = nil
		x.carbonsEnableMu.Unlock()
		return err
	}
	select {
	case <-ctx.Done():
		// #region agent log
		x.log.Debug().Str("hypothesis", "H5").Msg("DEBUG: enableCarbons ctx.Done")
		// #endregion
		return ctx.Err()
	case err := <-x.carbonsEnableCh:
		// #region agent log
		x.log.Debug().Str("hypothesis", "H2").Bool("hasErr", err != nil).Msg("DEBUG: enableCarbons got response from channel")
		// #endregion
		return err
	case <-time.After(5 * time.Second):
		// #region agent log
		x.log.Debug().Str("hypothesis", "H4").Msg("DEBUG: enableCarbons timeout 5s")
		// #endregion
		x.carbonsEnableMu.Lock()
		x.carbonsEnableID = ""
		x.carbonsEnableCh = nil
		x.carbonsEnableMu.Unlock()
		return fmt.Errorf("carbons enable: no response from server within 5s")
	}
}

// tryHandleCarbonsEnableResponse returns true if the IQ was our carbons enable response (consumed and handled).
func (x *xmppClient) tryHandleCarbonsEnableResponse(t xmlstream.TokenReadEncoder, start *xml.StartElement, typeVal, idVal string) bool {
	x.carbonsEnableMu.Lock()
	expectID := x.carbonsEnableID
	ch := x.carbonsEnableCh
	x.carbonsEnableMu.Unlock()
	if expectID == "" || idVal != expectID {
		return false
	}
	drainIQPayload(t)
	var err error
	if typeVal == "error" {
		err = fmt.Errorf("server rejected carbons enable (IQ error)")
		x.log.Warn().Str("iq_id", idVal).Msg("Carbons enable failed: server returned IQ error")
	}
	x.carbonsEnableMu.Lock()
	x.carbonsEnableID = ""
	x.carbonsEnableCh = nil
	x.carbonsEnableMu.Unlock()
	select {
	case ch <- err:
	default:
	}
	return true
}

// drainIQPayload consumes tokens until the closing </iq> (caller has already read the <iq> start).
func drainIQPayload(t xmlstream.TokenReadEncoder) {
	depth := 1
	for depth > 0 {
		tok, err := t.Token()
		if err != nil {
			return
		}
		switch tok.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		}
	}
}

// ReconnectLoop keeps the XMPP session alive with exponential backoff.
// It mirrors the Signal bridge's tryConnect pattern in a simplified form.
func (x *xmppClient) ReconnectLoop(ctx context.Context) {
	backoff := 5 * time.Second
	const maxBackoff = 5 * time.Minute

	for {
		select {
		case <-ctx.Done():
			x.log.Debug().Msg("Stopping XMPP reconnect loop (context done)")
			return
		default:
		}
		if x.IsLoggedIn() {
			// Already connected: sleep a bit before checking again.
			time.Sleep(30 * time.Second)
			continue
		}

		x.log.Debug().Msg("XMPP reconnect loop: attempting connection")
		// Bound each connect attempt so we don't block forever on a dead server.
		connectCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		x.Connect(connectCtx)
		cancel()

		if x.IsLoggedIn() {
			// Sync roster once after a successful (re)connect so display names can be mapped
			// from the Jabber roster to Matrix ghosts and DM room names. When backfill.enabled,
			// also run full MAM backfill for roster DMs so portals get history before new messages.
			go x.runFullBackfillAfterRosterSync(context.Background())
			// Reset backoff on successful connection.
			backoff = 5 * time.Second
			continue
		}

		x.log.Debug().Dur("backoff", backoff).Msg("XMPP reconnect loop: connect failed, backing off")
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (x *xmppClient) Disconnect() {
	x.mu.Lock()
	hadSession := x.session != nil
	if hadSession {
		_ = x.session.Close()
		x.session = nil
	}
	x.mucCacheMu.Lock()
	x.mucCache = nil
	x.mucCacheMu.Unlock()
	x.lastMAMSyncMu.Lock()
	x.lastMAMSync = nil
	x.lastMAMSyncMu.Unlock()
	x.knownPortalIDsForMAMMu.Lock()
	x.knownPortalIDsForMAM = nil
	x.knownPortalIDsForMAMMu.Unlock()
	login := x.login
	x.mu.Unlock()
	if hadSession && login != nil {
		login.BridgeState.Send(status.BridgeState{StateEvent: status.StateTransientDisconnect})
	}
}

func (x *xmppClient) IsLoggedIn() bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.session != nil
}

func (x *xmppClient) LogoutRemote(ctx context.Context) {
	x.mu.Lock()
	cancel := x.reconnectCancel
	x.reconnectCancel = nil
	x.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	x.Disconnect()
}

func (x *xmppClient) IsThisUser(ctx context.Context, userID networkid.UserID) bool {
	return networkid.UserLoginID(userID) == x.loginID
}

// SyncRoster fetches the XMPP roster from the server and updates Matrix ghost/room names.
// It is safe to call concurrently. Use this after renaming contacts in another client, since
// many servers do not push roster updates for name-only changes.
func (x *xmppClient) SyncRoster(ctx context.Context) {
	x.syncRoster(ctx)
}

// RunFullBackfill runs MAM backfill for all roster DMs (skips MUC). Implements FullBackfillRunner.
// No-ops if backfill is disabled in config. Used by sync-roster command and optionally after login.
func (x *xmppClient) RunFullBackfill(ctx context.Context) {
	x.runFullBackfill(ctx)
}

// runFullBackfillAfterRosterSync syncs the roster, then runs full MAM backfill when backfill.enabled.
// Called after each successful connect so portals get history before new messages arrive.
func (x *xmppClient) runFullBackfillAfterRosterSync(ctx context.Context) {
	x.syncRoster(ctx)
	if x.conn.backfillConfig == nil || !x.conn.backfillConfig.Enabled {
		return
	}
	x.runFullBackfill(ctx)
}

// SetRosterItem adds or updates a contact in the Jabber roster with the given display name.
// The JID is normalized to lowercase. After the server confirms, we update our cache and
// refresh the ghost and DM portal so Matrix shows the new name.
func (x *xmppClient) SetRosterItem(ctx context.Context, jidStr, displayName string) error {
	jidStr = strings.TrimSpace(strings.ToLower(jidStr))
	if jidStr == "" || !strings.Contains(jidStr, "@") {
		return fmt.Errorf("invalid JID: use user@domain")
	}
	parsed, err := jid.Parse(jidStr)
	if err != nil {
		return fmt.Errorf("invalid JID format: %w", err)
	}
	key := parsed.Bare().String()
	if key == "" {
		return fmt.Errorf("invalid JID")
	}

	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return fmt.Errorf("not connected to Jabber")
	}

	item := roster.Item{JID: parsed, Name: strings.TrimSpace(displayName)}
	if err := roster.Set(ctx, sess, item); err != nil {
		return fmt.Errorf("roster set failed: %w", err)
	}

	// Update local cache and refresh Matrix so the user sees the new name without waiting for a push.
	x.rosterMu.Lock()
	if x.roster == nil {
		x.roster = make(map[string]string)
	}
	if item.Name != "" {
		x.roster[strings.ToLower(key)] = item.Name
	} else {
		delete(x.roster, strings.ToLower(key))
	}
	x.rosterMu.Unlock()

	x.log.Info().Str("jid", key).Str("name", item.Name).Msg("Roster item set from Matrix")
	x.refreshRosterGhostsAndPortals(ctx, map[string]string{strings.ToLower(key): item.Name})
	return nil
}

// syncRoster fetches the XMPP roster for this login and stores it in-memory.
// This allows mapping Jabber contact names to Matrix ghost display names and DM room names.
// It is safe to call concurrently; only one fetch will be in-flight per call.
func (x *xmppClient) syncRoster(ctx context.Context) {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return
	}

	iter := roster.Fetch(ctx, sess)
	defer func() {
		if err := iter.Close(); err != nil {
			x.log.Debug().Err(err).Msg("Failed to close roster iterator")
		}
	}()

	newRoster := make(map[string]string)
	itemCount := 0
	for iter.Next() {
		item := iter.Item()
		bare := item.JID.Bare().String()
		itemCount++
		x.log.Info().Str("jid", bare).Str("name", item.Name).Str("subscription", item.Subscription).Msg("Roster item from server")
		if bare == "" {
			continue
		}
		// Normalize to lowercase so lookups match bridge (portal/ghost IDs are typically lowercased).
		key := strings.ToLower(bare)
		if item.Name != "" {
			newRoster[key] = item.Name
		}
	}
	if err := iter.Err(); err != nil {
		x.log.Warn().Err(err).Msg("Failed to fetch XMPP roster")
		return
	}

	x.rosterMu.Lock()
	x.roster = newRoster
	x.rosterMu.Unlock()

	x.log.Info().Int("item_count", itemCount).Int("entry_count", len(newRoster)).Msg("Synced XMPP roster for login")

	// Push updated names to Matrix for all roster contacts that have ghosts/portals.
	// Otherwise Matrix keeps showing old display names until the user opens the chat.
	x.refreshRosterGhostsAndPortals(ctx, newRoster)
}

// refreshRosterGhostsAndPortals updates Matrix ghost display names and DM room names
// for each given roster JID so they match the current roster (e.g. after full sync or roster push).
// Keys in rosterMap are already lowercase from syncRoster/handleRosterPush.
func (x *xmppClient) refreshRosterGhostsAndPortals(ctx context.Context, rosterMap map[string]string) {
	if rosterMap == nil || x.login == nil {
		x.log.Debug().Msg("refreshRosterGhostsAndPortals skipped: no roster map or login")
		return
	}
	for bareJID, rosterName := range rosterMap {
		// Ensure we use the same form the bridge uses (lowercase JID).
		jidForLookup := strings.ToLower(bareJID)
		// Refresh ghost so Matrix shows the roster name.
		ghost, err := x.conn.br.GetExistingGhostByID(ctx, networkid.UserID(jidForLookup))
		if err != nil {
			x.log.Debug().Err(err).Str("bare_jid", bareJID).Msg("GetExistingGhostByID failed")
			continue
		}
		if ghost == nil {
			x.log.Debug().Str("bare_jid", jidForLookup).Msg("No existing ghost for roster JID")
		} else {
			info, err := x.GetUserInfo(ctx, ghost)
			if err != nil {
				x.log.Debug().Err(err).Str("bare_jid", jidForLookup).Msg("GetUserInfo failed")
				continue
			}
			if info != nil && info.Name != nil {
				ghost.UpdateInfo(ctx, info)
				x.log.Info().Str("bare_jid", jidForLookup).Str("display_name", *info.Name).Msg("Updated ghost from roster")
			}
		}
		// Refresh DM portal room name if it exists.
		portal, err := x.conn.br.GetDMPortal(ctx, x.login.ID, networkid.UserID(jidForLookup))
		if err != nil {
			x.log.Debug().Err(err).Str("bare_jid", bareJID).Msg("GetDMPortal failed")
			continue
		}
		if portal == nil || portal.MXID == "" {
			x.log.Debug().Str("bare_jid", jidForLookup).Msg("No DM portal for roster JID")
		} else {
			portal.UpdateBridgeInfo(ctx)
			// Queue ChatResync so the bridge re-fetches GetChatInfo and applies the new room name to Matrix.
			x.login.QueueRemoteEvent(&simplevent.ChatResync{
				EventMeta: simplevent.EventMeta{
					Type:      bridgev2.RemoteEventChatResync,
					PortalKey: portal.PortalKey,
				},
				GetChatInfoFunc: x.GetChatInfo,
			})
			x.log.Info().Str("bare_jid", jidForLookup).Str("room_id", portal.MXID.String()).Str("roster_name", rosterName).Msg("Updated portal from roster (queued room name sync)")
		}
	}
}

// lookupRosterName returns the roster name for the given bare JID, if available.
// Keys are stored in lowercase so we normalize the lookup.
func (x *xmppClient) lookupRosterName(bareJID string) string {
	x.rosterMu.RLock()
	defer x.rosterMu.RUnlock()
	if x.roster == nil {
		return ""
	}
	return x.roster[strings.ToLower(bareJID)]
}

// SearchRoster searches the roster for contacts matching the query string.
// Matches against both JID and display name (case-insensitive partial match).
func (x *xmppClient) SearchRoster(ctx context.Context, query string) ([]RosterContact, error) {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return nil, fmt.Errorf("query cannot be empty")
	}
	
	// Ensure roster is synced before searching
	x.ensureRosterSynced(ctx)
	
	x.rosterMu.RLock()
	defer x.rosterMu.RUnlock()
	
	if x.roster == nil || len(x.roster) == 0 {
		return []RosterContact{}, nil
	}
	
	var results []RosterContact
	for jid, name := range x.roster {
		jidLower := strings.ToLower(jid)
		nameLower := strings.ToLower(name)
		// Match if query appears in JID or name
		if strings.Contains(jidLower, query) || strings.Contains(nameLower, query) {
			results = append(results, RosterContact{
				JID:  jid,
				Name: name,
			})
		}
	}
	
	return results, nil
}

// ensureRosterSynced checks if the roster cache is empty and synchronously syncs it if needed.
// This ensures roster names are available when GetChatInfo builds room names (e.g. from sync-portal).
func (x *xmppClient) ensureRosterSynced(ctx context.Context) {
	x.rosterMu.RLock()
	rosterEmpty := x.roster == nil || len(x.roster) == 0
	x.rosterMu.RUnlock()
	
	if !rosterEmpty {
		return
	}
	
	// Check if we're connected before attempting sync
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return
	}
	
	// Sync roster synchronously so it's available for GetChatInfo
	x.syncRoster(ctx)
}

// handleRosterPush is called by the mellium roster handler when the server sends a roster push IQ
// (contact added, updated, or removed). We update the in-memory roster cache and refresh the
// affected ghost so Matrix display name and DM room name stay in sync without reconnect.
//
// Note: Rename-after-add often does not trigger a roster push. Many clients (e.g. Movim) and
// servers only push on add (with name) and remove; a name-only update may not be sent or may
// be sent with an omitted/empty name attribute. When we get a non-remove push with empty name,
// we keep the previous roster name so we don't clear it on updates that omit the attribute.
func (x *xmppClient) handleRosterPush(ver string, item roster.Item) error {
	bareJID := item.JID.Bare().String()
	if bareJID == "" {
		return nil
	}
	key := strings.ToLower(bareJID)

	x.rosterMu.Lock()
	if x.roster == nil {
		x.roster = make(map[string]string)
	}
	if item.Subscription == "remove" {
		delete(x.roster, key)
	} else {
		if item.Name != "" {
			x.roster[key] = item.Name
		}
		// Empty name: do not delete. Server may send an update (e.g. subscription/group) without
		// repeating the name; keep existing cache so rename-on-server still shows until we resync.
	}
	x.rosterMu.Unlock()

	x.log.Info().
		Str("bare_jid", key).
		Str("subscription", item.Subscription).
		Str("name", item.Name).
		Msg("Roster push applied")

	// Refresh the ghost so Matrix sees the new display name.
	ctx := context.Background()
	ghost, err := x.conn.br.GetExistingGhostByID(ctx, networkid.UserID(key))
	if err == nil && ghost != nil {
		if info, err := x.GetUserInfo(ctx, ghost); err == nil && info != nil {
			ghost.UpdateInfo(ctx, info)
		}
	}

	// Refresh the DM portal room name if it exists (so Matrix room name matches roster).
	if x.login != nil {
		portal, err := x.conn.br.GetDMPortal(ctx, x.login.ID, networkid.UserID(key))
		if err == nil && portal != nil && portal.MXID != "" {
			portal.UpdateBridgeInfo(ctx)
			x.login.QueueRemoteEvent(&simplevent.ChatResync{
				EventMeta: simplevent.EventMeta{
					Type:      bridgev2.RemoteEventChatResync,
					PortalKey: portal.PortalKey,
				},
				GetChatInfoFunc: x.GetChatInfo,
			})
		}
	}
	return nil
}

// isMUCJID is a fallback heuristic when disco is unavailable (offline or disco failed).
func isMUCJID(jidStr string) bool {
	return strings.Contains(jidStr, "@conference.") || strings.Contains(jidStr, "@muc.")
}

// portalIsMUC returns whether the portal should be treated as a MUC (groupchat, MUC join).
// Uses creation path / stored type first: portal.RoomType from the framework; only falls back
// to disco (isMUC) when RoomType is unset (e.g. legacy portals).
func (x *xmppClient) portalIsMUC(ctx context.Context, portal *bridgev2.Portal) bool {
	if portal == nil {
		return false
	}
	switch portal.RoomType {
	case database.RoomTypeGroupDM:
		return true
	case database.RoomTypeDM:
		return false
	}
	// Unset or other: fall back to disco so legacy portals keep working.
	return x.isMUC(ctx, string(portal.ID))
}

// isMUC returns whether the JID is a MUC room (XEP-0045), using service discovery (XEP-0030)
// when connected; result is cached per JID. Falls back to isMUCJID when offline or on disco error.
// Cheogram 1:1 (SMS) JIDs are never MUCs: we must send chat/normal and must not send MUC join.
func (x *xmppClient) isMUC(ctx context.Context, jidStr string) bool {
	if IsCheogramDMJID(jidStr) {
		return false
	}
	x.mucCacheMu.RLock()
	cached, ok := x.mucCache[jidStr]
	x.mucCacheMu.RUnlock()
	if ok {
		return cached
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return isMUCJID(jidStr)
	}
	toJID, err := jid.Parse(jidStr)
	if err != nil {
		return isMUCJID(jidStr)
	}
	discoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	info, err := disco.GetInfo(discoCtx, "", toJID, sess)
	if err != nil {
		x.log.Debug().Err(err).Str("jid", jidStr).Msg("Disco info failed; using MUC heuristic")
		return isMUCJID(jidStr)
	}
	isMUC := false
	for _, f := range info.Features {
		if f.Var == mucFeatureNS {
			isMUC = true
			break
		}
	}
	x.mucCacheMu.Lock()
	if x.mucCache == nil {
		x.mucCache = make(map[string]bool)
	}
	x.mucCache[jidStr] = isMUC
	x.mucCacheMu.Unlock()
	return isMUC
}

func (x *xmppClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	portalID := string(portal.ID)
	if portalID == "" {
		return nil, nil
	}

	// Prefer stored type (creation path): framework sets portal.RoomType from our previous ChatInfo.
	switch portal.RoomType {
	case database.RoomTypeDM:
		// Ensure roster is synced before building DM ChatInfo so roster names are available
		x.ensureRosterSynced(ctx)
		return x.getDMChatInfo(ctx, portal, portalID)
	case database.RoomTypeGroupDM:
		return x.getMUCChatInfo(ctx, portal, portalID)
	}

	// No stored type: use heuristics then disco (like other clients: creation path / JID pattern).
	if IsCheogramDMJID(portalID) {
		// Ensure roster is synced before building DM ChatInfo so roster names are available
		x.ensureRosterSynced(ctx)
		return x.getDMChatInfo(ctx, portal, portalID)
	}
	if isMUCJID(portalID) {
		return x.getMUCChatInfo(ctx, portal, portalID)
	}

	// Default to DM - ensure roster is synced before building ChatInfo
	x.ensureRosterSynced(ctx)
	return x.getDMChatInfo(ctx, portal, portalID)
}

// CustomizePersonalFilteringSpace implements bridgev2.PersonalFilteringCustomizingNetworkAPI.
// No customization: use standard bridge behavior for the personal filtering space room.
func (x *xmppClient) CustomizePersonalFilteringSpace(req *mautrix.ReqCreateRoom) {}

// getMUCRoomMetadata fetches MUC room name and topic from disco#info (XEP-0030) extended info (XEP-0128 muc#roominfo form).
// Returns name and topic; empty strings if unavailable or on error.
func (x *xmppClient) getMUCRoomMetadata(ctx context.Context, roomJID string) (name, topic string) {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return "", ""
	}
	toJID, err := jid.Parse(roomJID)
	if err != nil {
		return "", ""
	}
	discoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	info, err := disco.GetInfo(discoCtx, "", toJID, sess)
	if err != nil {
		x.log.Debug().Err(err).Str("room", roomJID).Msg("Disco info for MUC metadata failed")
		return "", ""
	}
	// Use first conference identity name as room name if present.
	for _, id := range info.Identity {
		if id.Category == "conference" && id.Name != "" {
			name = id.Name
			break
		}
	}
	// Parse XEP-0128 extended forms for muc#roominfo (description, subject, room name).
	for i := range info.Form {
		f := &info.Form[i]
		formType, _ := f.GetString("FORM_TYPE")
		if formType != mucRoomInfoFormNS {
			continue
		}
		// Prefer room name from form; then description; then subject as topic.
		if n, ok := f.GetString("muc#roominfo_roomname"); ok && n != "" {
			name = n
		}
		if n, ok := f.GetString("muc#roomconfig_roomname"); ok && n != "" && name == "" {
			name = n
		}
		if d, ok := f.GetString("muc#roominfo_description"); ok && d != "" {
			topic = d
		}
		if s, ok := f.GetString("muc#roominfo_subject"); ok && s != "" {
			if topic != "" {
				topic = topic + " | " + s
			} else {
				topic = s
			}
		}
		// Fallback field names used by some servers
		if name == "" {
			if n, ok := f.GetString("Room name"); ok && n != "" {
				name = n
			}
		}
		if topic == "" {
			if d, ok := f.GetString("Description"); ok && d != "" {
				topic = d
			}
		}
		if topic == "" {
			if s, ok := f.GetString("Subject"); ok && s != "" {
				topic = s
			}
		}
		break
	}
	return name, topic
}

// getMUCChatInfo returns ChatInfo for a MUC portal. Occupant list is not yet available until join (3.2).
// When connected, fetches room name and topic from disco#info (XEP-0128) and uses them for Matrix room metadata.
func (x *xmppClient) getMUCChatInfo(ctx context.Context, portal *bridgev2.Portal, roomJID string) (*bridgev2.ChatInfo, error) {
	// Defaults: local part + JID for name, generic topic.
	localPart, _, _ := strings.Cut(roomJID, "@")
	name := localPart
	if name == "" {
		name = roomJID
	}
	name = fmt.Sprintf("%s (%s)", name, roomJID)
	topic := fmt.Sprintf("Jabber MUC — %s", roomJID)
	// Override with disco room metadata when available.
	if metaName, metaTopic := x.getMUCRoomMetadata(ctx, roomJID); metaName != "" || metaTopic != "" {
		if metaName != "" {
			name = metaName
			// Keep JID in parentheses so room list still shows address.
			if !strings.Contains(name, "("+roomJID+")") {
				name = fmt.Sprintf("%s (%s)", name, roomJID)
			}
		}
		if metaTopic != "" {
			topic = metaTopic
		}
	}
	// Append login JID to room name to identify which bridged account it's associated with
	name = fmt.Sprintf("%s (%s)", name, string(x.loginID))
	rt := database.RoomTypeGroupDM
	meID := networkid.UserID(x.loginID)
	// Minimal members: just the bridge user so they get invited. Full occupant list comes with join (3.2).
	members := &bridgev2.ChatMemberList{
		IsFull:           false,
		TotalMemberCount: 1,
		MemberMap: bridgev2.ChatMemberMap{
			meID: {
				EventSender: x.makeEventSender(string(x.loginID)),
				Membership:  event.MembershipJoin,
			},
		},
	}
	return &bridgev2.ChatInfo{
		Name:    &name,
		Topic:   &topic,
		Type:    &rt,
		Members: members,
		ExtraUpdates: func(ctx context.Context, p *bridgev2.Portal) bool {
			meta, _ := p.Metadata.(*PortalMetadata)
			if meta != nil {
				meta.LastSync.Time = time.Now()
				return true
			}
			return false
		},
	}, nil
}

// getDMChatInfo returns ChatInfo for a DM portal.
// For Cheogram group MMS JIDs (e.g. +1a,+1b,+1c@cheogram.com), returns one member per number so each gets a ghost with roster display name.
func (x *xmppClient) getDMChatInfo(ctx context.Context, portal *bridgev2.Portal, bareJID string) (*bridgev2.ChatInfo, error) {
	name := bareJID
	rt := database.RoomTypeDM
	meID := networkid.UserID(x.loginID)
	memberMap := bridgev2.ChatMemberMap{
		meID: {
			EventSender: x.makeEventSender(string(x.loginID)),
			Membership:  event.MembershipJoin,
		},
	}
	var otherID networkid.UserID
	var totalCount int

	if individuals, ok := ParseCheogramGroupMMSJID(bareJID); ok {
		totalCount = 1 + len(individuals)
		// Build room display name from all participants: use roster name when available, otherwise JID.
		displayParts := make([]string, 0, len(individuals))
		for _, j := range individuals {
			uid := networkid.UserID(j)
			memberMap[uid] = bridgev2.ChatMember{
				EventSender: x.makeEventSender(j),
				Membership:  event.MembershipJoin,
			}
			if otherID == "" {
				otherID = uid
			}
			if rosterName := x.lookupRosterName(j); rosterName != "" {
				displayParts = append(displayParts, rosterName)
			} else {
				displayParts = append(displayParts, j)
			}
		}
		if len(displayParts) > 0 {
			name = strings.Join(displayParts, ", ")
		}
	} else {
		totalCount = 2
		otherID = networkid.UserID(portal.ID)
		if rosterName := x.lookupRosterName(bareJID); rosterName != "" {
			name = rosterName
		}
		memberMap[otherID] = bridgev2.ChatMember{
			EventSender: x.makeEventSender(string(portal.ID)),
			Membership:  event.MembershipJoin,
		}
	}

	// Append login JID to room name to identify which bridged account it's associated with
	name = fmt.Sprintf("%s (%s)", name, string(x.loginID))

	members := &bridgev2.ChatMemberList{
		IsFull:           true,
		TotalMemberCount: totalCount,
		OtherUserID:      otherID,
		MemberMap:        memberMap,
	}
	return &bridgev2.ChatInfo{
		Name:    &name,
		Type:    &rt,
		Members: members,
		ExtraUpdates: func(ctx context.Context, p *bridgev2.Portal) bool {
			meta, _ := p.Metadata.(*PortalMetadata)
			if meta != nil {
				meta.LastSync.Time = time.Now()
				return true
			}
			return false
		},
	}, nil
}

// decodeBridgeUserID reverses the bridge's encoding of JIDs in Matrix localpart (=40 → @, =2f → /).
// So we can treat ghost.ID as a JID when it's stored in encoded form.
func decodeBridgeUserID(encoded string) string {
	s := strings.ReplaceAll(encoded, "=2f", "/")
	s = strings.ReplaceAll(s, "=40", "@")
	return s
}

// withAppServiceIDSuffix appends " (appservice.id)" to the display name when appservice ID is configured.
// Ghost display names should show the appservice ID so users can distinguish ghosts from multiple bridges.
func (x *xmppClient) withAppServiceIDSuffix(name string) string {
	if x.conn == nil || x.conn.appServiceID == "" {
		return name
	}
	return name + " (" + x.conn.appServiceID + ")"
}

// ensureMUCGhostDisplayName ensures the ghost for the given MUC occupant (room@conf/nick) has the correct display name (nick).
// Call before queueing a MUC message so Matrix shows the short nick instead of the full JID.
func (x *xmppClient) ensureMUCGhostDisplayName(ctx context.Context, occupantFull string) {
	if x.conn == nil || x.conn.br == nil || occupantFull == "" || !strings.Contains(occupantFull, "/") {
		return
	}
	ghost, err := x.conn.br.GetGhostByID(ctx, networkid.UserID(occupantFull))
	if err != nil {
		x.log.Debug().Err(err).Str("occupant", occupantFull).Msg("Could not get ghost for MUC sender display name")
		return
	}
	if ghost == nil {
		return
	}
	info, err := x.GetUserInfo(ctx, ghost)
	if err != nil || info == nil || info.Name == nil {
		return
	}
	ghost.UpdateInfo(ctx, info)
	x.log.Info().Str("occupant", occupantFull).Str("display_name", *info.Name).Msg("Updated MUC ghost display name")
}

func (x *xmppClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	jidStr := string(ghost.ID)
	if jidStr == "" {
		return nil, nil
	}
	// Bridge may store JID with =40 for @ and =2f for / in ghost ID; decode so we can parse room/nick.
	jidStr = decodeBridgeUserID(jidStr)
	// Cheogram group MMS combined JID (rare: bridge usually only creates ghosts for individual numbers).
	if individuals, ok := ParseCheogramGroupMMSJID(jidStr); ok {
		displayName := "Group MMS (" + strconv.Itoa(len(individuals)) + ")"
		if len(individuals) <= 3 {
			displayName = strings.Join(individuals, ", ")
		}
		dn := x.withAppServiceIDSuffix(displayName)
		return &bridgev2.UserInfo{
			Identifiers: []string{jidStr},
			Name:        &dn,
		}, nil
	}
	// MUC occupant: ghost ID is room@conference.domain/nick — use only the nick as display name so Matrix shows the short nick, not the full JID.
	displayName := jidStr
	if idx := strings.LastIndex(jidStr, "/"); idx >= 0 && idx+1 < len(jidStr) {
		displayName = jidStr[idx+1:]
		x.log.Info().Str("ghost_id", string(ghost.ID)).Str("jid", jidStr).Str("display_name", displayName).Msg("GetUserInfo: MUC occupant, using nick as display name")
	} else if rosterName := x.lookupRosterName(jidStr); rosterName != "" {
		displayName = rosterName
	}
	displayName = x.withAppServiceIDSuffix(displayName)
	isBot := false
	// Aligned with Signal bridge: include ExtraUpdates for metadata tracking
	return &bridgev2.UserInfo{
		IsBot:       &isBot,
		Identifiers: []string{jidStr},
		Name:        &displayName,
		ExtraUpdates: func(ctx context.Context, g *bridgev2.Ghost) (changed bool) {
			meta, _ := g.Metadata.(*GhostMetadata)
			if meta != nil {
				// Update profile fetched timestamp
				meta.ProfileFetchedAt.Time = time.Now()
				return true
			}
			return false
		},
	}, nil
}

// ResolveIdentifier resolves a JID string (e.g. user@example.com) so the bridge can start a DM
// via the generic start-chat / resolve-identifier flow. When Cheogram config is enabled, phone
// numbers and short codes (e.g. +14169938000 or 33733) are resolved to Cheogram JIDs.
func (x *xmppClient) ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	trimmed := strings.TrimSpace(identifier)
	if trimmed == "" {
		warnIfDebugIdentifiers(&x.log)
		x.log.Debug().Str("identifier", identifier).Msg("ResolveIdentifier: empty identifier")
		return nil, fmt.Errorf("invalid JID: must be like user@domain.example")
	}
	jidStr := trimmed
	if !strings.Contains(trimmed, "@") {
		if CheogramEnabled(x.conn.br) {
			// Special case: allow cheogram.com bot (domain-only JID) when Cheogram is enabled
			if strings.ToLower(trimmed) == cheogramDomain {
				jidStr = cheogramDomain
			} else if strings.Contains(trimmed, ",") {
				if resolved, ok := ResolveCheogramMultiIdentifier(trimmed, true); ok {
					jidStr = strings.ToLower(resolved)
				} else {
					warnIfDebugIdentifiers(&x.log)
					x.log.Debug().Str("identifier", identifier).Msg("ResolveIdentifier: comma-separated list but Cheogram multi resolve failed")
					return nil, fmt.Errorf("invalid JID: use user@domain or comma-separated phone numbers when Cheogram is enabled")
				}
			} else if resolved, ok := ResolveCheogramIdentifier(trimmed, true); ok {
				jidStr = strings.ToLower(resolved)
			} else {
				warnIfDebugIdentifiers(&x.log)
				x.log.Debug().Str("identifier", identifier).Msg("ResolveIdentifier: not a JID and Cheogram resolution failed")
				return nil, fmt.Errorf("invalid JID: use user@domain or enable Cheogram for phone/short codes")
			}
		} else {
			warnIfDebugIdentifiers(&x.log)
			x.log.Debug().Str("identifier", identifier).Msg("ResolveIdentifier: invalid JID (missing @)")
			return nil, fmt.Errorf("invalid JID: must be like user@domain.example")
		}
	} else {
		jidStr = strings.ToLower(trimmed)
	}
	if !strings.Contains(jidStr, ",") {
		parsed, err := jid.Parse(jidStr)
		if err != nil {
			warnIfDebugIdentifiers(&x.log)
			x.log.Debug().Str("identifier", identifier).Msg("ResolveIdentifier: invalid JID format")
			return nil, fmt.Errorf("invalid JID format (use user@domain)")
		}
		jidStr = parsed.String()
	}

	userID := networkid.UserID(jidStr)
	baseName := jidStr
	// For group MMS, resolve to first individual so the bridge doesn't create a ghost for the combined JID.
	if individuals, ok := ParseCheogramGroupMMSJID(jidStr); ok && len(individuals) > 0 {
		userID = networkid.UserID(individuals[0])
		// Name for display can stay as combined JID or use first roster name; getDMChatInfo will refine.
		baseName = individuals[0]
	}
	if rosterName := x.lookupRosterName(baseName); rosterName != "" {
		baseName = rosterName
	}
	name := x.withAppServiceIDSuffix(baseName)
	resp := &bridgev2.ResolveIdentifierResponse{
		UserID: userID,
		UserInfo: &bridgev2.UserInfo{
			Identifiers: []string{jidStr},
			Name:        &name,
		},
	}
	if createChat {
		rt := database.RoomTypeDM
		meID := networkid.UserID(x.loginID)
		memberMap := bridgev2.ChatMemberMap{
			meID: {
				EventSender: x.makeEventSender(string(x.loginID)),
				Membership:  event.MembershipJoin,
			},
		}
		var otherID networkid.UserID
		var totalCount int
		if individuals, ok := ParseCheogramGroupMMSJID(jidStr); ok {
			totalCount = 1 + len(individuals)
			for _, j := range individuals {
				uid := networkid.UserID(j)
				memberMap[uid] = bridgev2.ChatMember{
					EventSender: x.makeEventSender(j),
					Membership:  event.MembershipJoin,
				}
				if otherID == "" {
					otherID = uid
				}
			}
		} else {
			totalCount = 2
			otherID = networkid.UserID(jidStr)
			memberMap[otherID] = bridgev2.ChatMember{
				EventSender: x.makeEventSender(jidStr),
				Membership:  event.MembershipJoin,
			}
		}
		resp.Chat = &bridgev2.CreateChatResponse{
			PortalKey: networkid.PortalKey{
				ID:       networkid.PortalID(jidStr),
				Receiver: x.loginID,
			},
			PortalInfo: &bridgev2.ChatInfo{
				Name: &jidStr,
				Type: &rt,
				Members: &bridgev2.ChatMemberList{
					IsFull:           true,
					TotalMemberCount: totalCount,
					OtherUserID:      otherID,
					MemberMap:        memberMap,
				},
				ExtraUpdates: func(ctx context.Context, p *bridgev2.Portal) bool {
					meta, _ := p.Metadata.(*PortalMetadata)
					if meta != nil {
						meta.LastSync.Time = time.Now()
						return true
					}
					return false
				},
			},
		}
		// Backfill on PM creation: run before room is created so history appears before first message.
		if x.conn.backfillConfig != nil && x.conn.backfillConfig.Enabled {
			backfillCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			x.mamBackfillOnView(x.log.WithContext(backfillCtx), jidStr)
			cancel()
		}
	}
	return resp, nil
}

// getPreferredMUCNick returns the nick to use for joining the given MUC: room-specific preferred,
// then default preferred, then login localpart / "bridge".
func (x *xmppClient) getPreferredMUCNick(ctx context.Context, roomJIDStr string) string {
	// Room-specific preferred nick (portal metadata)
	if x.conn != nil && x.conn.br != nil {
		portalKey := networkid.PortalKey{ID: networkid.PortalID(roomJIDStr), Receiver: x.loginID}
		if portal, err := x.conn.br.GetPortalByKey(ctx, portalKey); err == nil && portal != nil {
			if meta, _ := portal.Metadata.(*PortalMetadata); meta != nil && meta.MUCPreferredNick != "" {
				return meta.MUCPreferredNick
			}
		}
	}
	// Default preferred nick (login metadata)
	if x.login != nil {
		if meta, _ := x.login.Metadata.(*JabberLoginMeta); meta != nil && meta.DefaultMUCNick != "" {
			return meta.DefaultMUCNick
		}
	}
	// Fallback: localpart of login JID or "bridge"
	nick := string(x.loginID)
	if at := strings.IndexByte(nick, '@'); at > 0 {
		nick = nick[:at]
	}
	if nick == "" {
		nick = "bridge"
	}
	return nick
}

// JoinMUC sends a MUC join presence (XEP-0045) so the bridge user can send/receive groupchat.
// Nick is chosen from room-specific preferred, default preferred, or login localpart.
func (x *xmppClient) JoinMUC(ctx context.Context, roomJIDStr string) error {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return fmt.Errorf("not connected to XMPP")
	}
	roomJID, err := jid.Parse(roomJIDStr)
	if err != nil {
		return fmt.Errorf("invalid MUC JID %q: %w", roomJIDStr, err)
	}
	nick := x.getPreferredMUCNick(ctx, roomJIDStr)
	toJID, err := roomJID.WithResource(nick)
	if err != nil {
		return fmt.Errorf("MUC JID with resource: %w", err)
	}
	// Track pending join so presence handler can store our occupant JID (and detect 409).
	x.mucPendingJoinMu.Lock()
	if x.mucPendingJoin == nil {
		x.mucPendingJoin = make(map[string]string)
	}
	x.mucPendingJoin[roomJIDStr] = nick
	x.mucPendingJoinMu.Unlock()

	// XEP-0045: optional <x xmlns='http://jabber.org/protocol/muc'/> so servers treat this as a MUC join.
	mucX := xml.StartElement{Name: xml.Name{Space: "http://jabber.org/protocol/muc", Local: "x"}}
	payload := xmlstream.Wrap(nil, mucX)
	p := stanza.Presence{To: toJID}.Wrap(payload)
	if err := sess.Send(ctx, p); err != nil {
		x.mucPendingJoinMu.Lock()
		delete(x.mucPendingJoin, roomJIDStr)
		x.mucPendingJoinMu.Unlock()
		return fmt.Errorf("send MUC join presence: %w", err)
	}
	x.log.Info().
		Str("room", roomJIDStr).
		Str("nick", nick).
		Msg("Sent MUC join presence")
	return nil
}

// retryMUCJoinAfterConflict sends a MUC join with an alternative nick (triedNick+"_1") after a 409 conflict.
// Called from the presence handler; runs in a goroutine.
func (x *xmppClient) retryMUCJoinAfterConflict(roomBare, triedNick string) {
	ctx := context.Background()
	altNick := triedNick + "_1"
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		x.log.Warn().Str("room", roomBare).Msg("MUC 409 retry skipped: not connected")
		return
	}
	roomJID, err := jid.Parse(roomBare)
	if err != nil {
		x.log.Warn().Err(err).Str("room", roomBare).Msg("MUC 409 retry: invalid room JID")
		return
	}
	toJID, err := roomJID.WithResource(altNick)
	if err != nil {
		x.log.Warn().Err(err).Str("room", roomBare).Msg("MUC 409 retry: failed to build JID")
		return
	}
	x.mucPendingJoinMu.Lock()
	if x.mucPendingJoin == nil {
		x.mucPendingJoin = make(map[string]string)
	}
	x.mucPendingJoin[roomBare] = altNick
	x.mucPendingJoinMu.Unlock()

	mucX := xml.StartElement{Name: xml.Name{Space: "http://jabber.org/protocol/muc", Local: "x"}}
	payload := xmlstream.Wrap(nil, mucX)
	p := stanza.Presence{To: toJID}.Wrap(payload)
	if err := sess.Send(ctx, p); err != nil {
		x.mucPendingJoinMu.Lock()
		delete(x.mucPendingJoin, roomBare)
		x.mucPendingJoinMu.Unlock()
		x.log.Warn().Err(err).Str("room", roomBare).Str("nick", altNick).Msg("MUC 409 retry: send failed")
		return
	}
	x.log.Info().Str("room", roomBare).Str("nick", altNick).Msg("MUC 409 retry: sent join with alternative nick")
}

// getMUCOurOccupantJID returns our full occupant JID (room@service/nick) for the given MUC room if known.
func (x *xmppClient) getMUCOurOccupantJID(roomBare string) string {
	x.mucOurOccupantJIDMu.RLock()
	defer x.mucOurOccupantJIDMu.RUnlock()
	if x.mucOurOccupantJID == nil {
		return ""
	}
	return x.mucOurOccupantJID[roomBare]
}

// LeaveMUC sends a MUC leave presence (XEP-0045): unavailable to room/nick.
// Uses our stored occupant nick when known (e.g. if we joined with nick_1 due to 409), otherwise preferred/fallback.
func (x *xmppClient) LeaveMUC(ctx context.Context, roomJIDStr string) error {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return fmt.Errorf("not connected to XMPP")
	}
	roomJID, err := jid.Parse(roomJIDStr)
	if err != nil {
		return fmt.Errorf("invalid MUC JID %q: %w", roomJIDStr, err)
	}
	nick := ""
	if occupant := x.getMUCOurOccupantJID(roomJIDStr); occupant != "" {
		if parsed, err := jid.Parse(occupant); err == nil {
			nick = parsed.Resourcepart()
		}
	}
	if nick == "" {
		nick = x.getPreferredMUCNick(ctx, roomJIDStr)
	}
	toJID, err := roomJID.WithResource(nick)
	if err != nil {
		return fmt.Errorf("MUC JID with resource: %w", err)
	}
	p := stanza.Presence{To: toJID, Type: stanza.PresenceType("unavailable")}.Wrap(nil)
	if err := sess.Send(ctx, p); err != nil {
		return fmt.Errorf("send MUC leave presence: %w", err)
	}
	x.mucOurOccupantJIDMu.Lock()
	if x.mucOurOccupantJID != nil {
		delete(x.mucOurOccupantJID, roomJIDStr)
	}
	x.mucOurOccupantJIDMu.Unlock()
	x.log.Info().
		Str("room", roomJIDStr).
		Str("nick", nick).
		Msg("Sent MUC leave presence")
	return nil
}

// HandleMatrixViewingChat is called when a Matrix user views a portal room.
// Aligned with Signal bridge: syncs ghost info and resyncs stale portals.
func (x *xmppClient) HandleMatrixViewingChat(ctx context.Context, msg *bridgev2.MatrixViewingChat) error {
	if msg.Portal == nil || msg.Portal.MXID == "" {
		return nil
	}

	// For MUC portals, join the room on XMPP so we can send and receive groupchat.
	if x.portalIsMUC(ctx, msg.Portal) {
		if err := x.JoinMUC(ctx, string(msg.Portal.ID)); err != nil {
			x.log.Warn().Err(err).Str("portal_id", string(msg.Portal.ID)).Msg("Failed to join MUC on view; messages may not send until joined")
		}
	}

	// Ensure the user is joined to the portal room (like Signal bridge behavior)
	if x.login != nil && x.login.User != nil {
		userIntent := x.login.User.DoublePuppet(ctx)
		if userIntent == nil {
			// If double-puppet isn't available, try using the ghost intent
			ghost, err := x.conn.br.GetGhostByID(ctx, networkid.UserID(x.loginID))
			if err == nil && ghost != nil {
				userIntent = ghost.Intent
			}
		}
		if userIntent != nil {
			if err := userIntent.EnsureJoined(ctx, msg.Portal.MXID); err != nil {
				x.log.Warn().Err(err).Str("room_id", msg.Portal.MXID.String()).Msg("Failed to ensure user is joined to portal room")
			}
		}
	}

	// Sync the other user's ghost in DMs (aligned with Signal bridge)
	if msg.Portal.OtherUserID != "" {
		ghost, err := x.conn.br.GetExistingGhostByID(ctx, msg.Portal.OtherUserID)
		if err != nil {
			x.log.Warn().Err(err).Str("other_user_id", string(msg.Portal.OtherUserID)).Msg("Failed to get ghost for sync")
		} else if ghost != nil {
			meta, _ := ghost.Metadata.(*GhostMetadata)
			if meta != nil && meta.ProfileFetchedAt.Time.Add(5*time.Minute).Before(time.Now()) {
				// Reset profile fetched time for immediate sync
				meta.ProfileFetchedAt.Time = time.Time{}
				info, err := x.GetUserInfo(ctx, ghost)
				if err != nil {
					x.log.Warn().Err(err).Str("ghost_id", string(ghost.ID)).Msg("Failed to get user info for ghost sync")
				} else if info != nil {
					ghost.UpdateInfo(ctx, info)
				}
			}
		}
	}

	// Resync the portal if it's stale (aligned with Signal bridge: 24 hour threshold)
	portalMeta, _ := msg.Portal.Metadata.(*PortalMetadata)
	if portalMeta != nil && portalMeta.LastSync.Time.Add(24*time.Hour).Before(time.Now()) {
		// Queue a chat resync event (aligned with Signal bridge pattern)
		x.login.QueueRemoteEvent(&simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventChatResync,
				PortalKey: msg.Portal.PortalKey,
			},
			GetChatInfoFunc: x.GetChatInfo,
		})
	}

	// Backfill on view: the bridge framework may never call FetchMessages for appservice flows
	// (sync-portal, pm command, etc.). Run MAM historical fetch when user views a DM.
	// Run synchronously so history appears before they can send; respects config.backfill.enabled.
	if x.login != nil && x.conn.backfillConfig != nil && x.conn.backfillConfig.Enabled &&
		!x.portalIsMUC(ctx, msg.Portal) && msg.Portal.ID != "" {
		backfillCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		x.mamBackfillOnView(x.log.WithContext(backfillCtx), string(msg.Portal.ID))
		cancel()
	}

	return nil
}

func (x *xmppClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("not connected to XMPP")).WithSendNotice(true)
	}
	toJIDStr := string(msg.Portal.ID)
	if toJIDStr == "" {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("portal has no remote JID")).WithSendNotice(true)
	}
	toJID, err := jid.Parse(toJIDStr)
	if err != nil {
		warnIfDebugIdentifiers(&x.log)
		x.log.Debug().Str("portal_id", toJIDStr).Msg("HandleMatrixMessage: invalid portal JID format")
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("invalid portal JID format")).WithSendNotice(true)
	}
	// Ensure this portal is included in MAM catch-up (e.g. Cheogram groups not in roster).
	x.addPortalIDForMAMCatchUp(toJIDStr)
	// Fallback backfill: if HandleMatrixViewingChat is never called (e.g. appservice flow), run
	// MAM historical fetch when user sends first message in a DM. Cooldown 1h per portal.
	// Respects config.backfill.enabled.
	if x.conn.backfillConfig != nil && x.conn.backfillConfig.Enabled && !x.portalIsMUC(ctx, msg.Portal) {
		key := strings.ToLower(toJIDStr)
		x.lastBackfillOnViewMu.Lock()
		if x.lastBackfillOnView == nil {
			x.lastBackfillOnView = make(map[string]time.Time)
		}
		last := x.lastBackfillOnView[key]
		x.lastBackfillOnViewMu.Unlock()
		if time.Since(last) > time.Hour {
			x.lastBackfillOnViewMu.Lock()
			x.lastBackfillOnView[key] = time.Now()
			x.lastBackfillOnViewMu.Unlock()
			go x.mamBackfillOnView(x.log.WithContext(context.Background()), toJIDStr)
		}
	}

	// For MUC portals, ensure we're in the room before sending (join on view may not have run yet).
	if x.portalIsMUC(ctx, msg.Portal) {
		if joinErr := x.JoinMUC(ctx, toJIDStr); joinErr != nil {
			x.log.Warn().Err(joinErr).Str("portal_id", toJIDStr).Msg("Failed to join MUC before send; message may be rejected")
		} else {
			// Give the MUC server time to process our presence before sending (cross-server MUCs can be slow).
			select {
			case <-ctx.Done():
				return nil, bridgev2.WrapErrorInStatus(ctx.Err()).WithSendNotice(true)
			case <-time.After(1500 * time.Millisecond):
			}
		}
	}

	x.log.Debug().
		Str("content_body", msg.Content.Body).
		Str("content_body_len", fmt.Sprintf("%d", len(msg.Content.Body))).
		Str("content_formatted_body", msg.Content.FormattedBody).
		Str("content_formatted_body_len", fmt.Sprintf("%d", len(msg.Content.FormattedBody))).
		Str("content_filename", msg.Content.FileName).
		Str("content_msgtype", string(msg.Content.MsgType)).
		Str("content_url", string(msg.Content.URL)).
		Bool("has_file", msg.Content.File != nil).
		Msg("DEBUG: HandleMatrixMessage - initial Matrix content values")

	body := msg.Content.Body
	if body == "" && msg.Content.FormattedBody != "" {
		body = msg.Content.FormattedBody
		x.log.Debug().Str("using_formatted_body", body).Msg("DEBUG: Using FormattedBody instead of Body")
	}
	
	x.log.Debug().
		Str("resolved_body", body).
		Str("resolved_body_len", fmt.Sprintf("%d", len(body))).
		Msg("DEBUG: Body resolved from Matrix content")

	// Fill Content.File/URL from raw event when bridge left them unset (e.g. decrypted media).
	if msg.Event != nil {
		if mc := msg.Event.Content.AsMessage(); mc != nil {
			if msg.Content.File == nil && mc.File != nil {
				msg.Content.File = mc.File
			}
			if msg.Content.URL == "" {
				if mc.URL != "" {
					msg.Content.URL = mc.URL
				} else if mc.File != nil {
					msg.Content.URL = mc.File.URL
				}
			}
			// Fill FileName from raw event if Content.FileName is empty
			if msg.Content.FileName == "" && mc.FileName != "" {
				msg.Content.FileName = mc.FileName
				x.log.Debug().Str("event_filename", mc.FileName).Msg("DEBUG: Got filename from event content")
			}
		}
	}

	// Aligned with mautrix-signal/pkg/msgconv/from-matrix.go ToSignal: normalize from event type when needed
	// (Signal sets content.MsgType from evt.Type for stickers); then same switch and DownloadMedia(content.URL, content.File).
	if msg.Event.Type == event.EventSticker {
		msg.Content.MsgType = event.MessageType(event.EventSticker.Type)
	}

	// event.MessageEventContent.GetCapMsgType() treats empty MsgType with URL/File as media; normalize so switch sees it.
	// Decrypted events sometimes arrive as generic "m.message" instead of m.image/m.file; treat unknown+media as file.
	hasMedia := msg.Content.URL != "" || msg.Content.File != nil
	isKnownType := msg.Content.MsgType == event.MsgText || msg.Content.MsgType == event.MsgNotice ||
		msg.Content.MsgType == event.MsgEmote || msg.Content.MsgType == event.MsgImage ||
		msg.Content.MsgType == event.MsgVideo || msg.Content.MsgType == event.MsgAudio || msg.Content.MsgType == event.MsgFile
	if hasMedia && !isKnownType {
		msg.Content.MsgType = event.MsgFile
	}

	var messageBody, oobURL string
	switch msg.Content.MsgType {
	case event.MsgText, event.MsgNotice, event.MsgEmote:
		messageBody = body
	case event.MsgImage, event.MsgVideo, event.MsgAudio, event.MsgFile:
		mediaLabel := "file"
		switch msg.Content.MsgType {
		case event.MsgImage:
			mediaLabel = "image"
		case event.MsgVideo:
			mediaLabel = "video"
		case event.MsgAudio:
			mediaLabel = "audio"
		}
		x.log.Debug().
			Str("msg_type", string(msg.Content.MsgType)).
			Str("media_label", mediaLabel).
			Str("raw_body", body).
			Str("body_len", fmt.Sprintf("%d", len(body))).
			Str("content_filename", msg.Content.FileName).
			Str("content_url", string(msg.Content.URL)).
			Bool("has_file", msg.Content.File != nil).
			Msg("DEBUG: Processing media message - initial values")
		
		// Matrix encrypted file content has url in content.file.url only (no top-level url).
		mediaURL := msg.Content.URL
		if mediaURL == "" && msg.Content.File != nil {
			mediaURL = msg.Content.File.URL
		}
		if mediaURL == "" {
			return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("%w: %s has no URL", bridgev2.ErrUnsupportedMessageType, msg.Content.MsgType)).WithSendNotice(true)
		}
		x.log.Debug().Str("media_url", string(mediaURL)).Msg("DEBUG: Resolved media URL")
		
		// Extract user-provided caption (if any). For OOB messages, we only include actual user text.
		// Matrix often includes the filename in body for media messages; if body equals filename, treat as no caption.
		userCaption := strings.TrimSpace(body)
		
		// Get filename for comparison - check Content.FileName first, then try event content
		filename := msg.Content.FileName
		if filename == "" && msg.Event != nil {
			if mc := msg.Event.Content.AsMessage(); mc != nil {
				filename = mc.FileName
			}
		}
		
		x.log.Debug().
			Str("trimmed_body", userCaption).
			Str("content_filename", msg.Content.FileName).
			Str("resolved_filename", filename).
			Bool("body_equals_filename", userCaption == filename).
			Msg("DEBUG: After trimming body, before filename comparison")
		
		if filename != "" && userCaption == filename {
			// Body is just the filename (Matrix default), not a user caption
			x.log.Debug().Msg("DEBUG: Body equals filename, treating as empty caption")
			userCaption = ""
		} else if filename == "" && userCaption != "" {
			// If filename is still empty but body looks like a filename (ends with extension, no spaces),
			// treat it as filename, not caption
			if !strings.Contains(userCaption, " ") && (strings.HasSuffix(userCaption, ".png") ||
				strings.HasSuffix(userCaption, ".jpg") || strings.HasSuffix(userCaption, ".jpeg") ||
				strings.HasSuffix(userCaption, ".gif") || strings.HasSuffix(userCaption, ".webp") ||
				strings.HasSuffix(userCaption, ".mp4") || strings.HasSuffix(userCaption, ".mov") ||
				strings.HasSuffix(userCaption, ".mp3") || strings.HasSuffix(userCaption, ".ogg") ||
				strings.HasSuffix(userCaption, ".pdf") || strings.HasSuffix(userCaption, ".zip") ||
				strings.Contains(userCaption, ".")) {
				x.log.Debug().Str("body_looks_like_filename", userCaption).Msg("DEBUG: Body looks like filename (has extension, no spaces), treating as empty caption")
				userCaption = ""
			}
		}
		
		// Fallback caption for when upload fails (includes filename for context)
		fallbackCaption := userCaption
		if fallbackCaption == "" {
			fallbackCaption = "Sent a " + mediaLabel
			if msg.Content.FileName != "" {
				fallbackCaption += ": " + msg.Content.FileName
			}
		}
		x.log.Debug().
			Str("user_caption", userCaption).
			Str("user_caption_len", fmt.Sprintf("%d", len(userCaption))).
			Str("fallback_caption", fallbackCaption).
			Msg("DEBUG: Caption processing complete")
		// Download media and re-upload to our server (XEP-0363) so Jabber users get a usable HTTPS URL.
		data, err := x.conn.br.Bot.DownloadMedia(ctx, mediaURL, msg.Content.File)
		if err != nil {
			x.log.Warn().Err(err).Str("mxc", string(mediaURL)).Msg("Media download failed; sending MXC link anyway")
			messageBody = fallbackCaption + " " + string(mediaURL)
		} else {
			filename := msg.Content.FileName
			if filename == "" {
				filename = "file"
			}
			contentType := "application/octet-stream"
			if msg.Event != nil {
				if mc := msg.Event.Content.AsMessage(); mc != nil && mc.Info != nil && mc.Info.MimeType != "" {
					contentType = mc.Info.MimeType
				}
			}
			// Use sender's (our) domain for XEP-0363: we upload to our server, then send the GET URL.
			origin, err := jid.Parse(string(x.loginID))
			if err != nil {
				x.log.Warn().Err(err).Str("login_id", string(x.loginID)).Msg("Failed to parse loginID for upload JID; sending MXC link")
				messageBody = fallbackCaption + " " + string(mediaURL)
			} else {
				uploadJID := x.getUploadJID(ctx, sess, origin.Domain().String())
				if uploadJID == nil {
					x.log.Warn().Str("domain", origin.Domain().String()).Msg("Could not discover upload service; sending MXC link")
					messageBody = fallbackCaption + " " + string(mediaURL)
				} else {
					x.log.Debug().Str("upload_jid", uploadJID.String()).Str("filename", filename).Int("size", len(data)).Str("content_type", contentType).Msg("Requesting XEP-0363 upload slot")
					slot, upErr := upload.GetSlot(ctx, upload.File{Name: filename, Size: len(data), Type: contentType}, *uploadJID, sess)
					if upErr != nil {
						x.log.Warn().Err(upErr).Str("upload_jid", uploadJID.String()).Str("mxc", string(mediaURL)).Msg("XEP-0363 slot request failed; sending MXC link anyway")
						messageBody = fallbackCaption + " " + string(mediaURL)
					} else {
						x.log.Debug().Str("put_url", slot.PutURL.String()).Str("get_url", slot.GetURL.String()).Msg("Got XEP-0363 slot, uploading file")
						putReq, putErr := slot.Put(ctx, bytes.NewReader(data))
						if putErr != nil {
							x.log.Warn().Err(putErr).Str("mxc", string(mediaURL)).Msg("XEP-0363 PUT build failed; sending MXC link anyway")
							messageBody = fallbackCaption + " " + string(mediaURL)
						} else {
							client := &http.Client{Timeout: 60 * time.Second}
							resp, doErr := client.Do(putReq)
							if doErr != nil {
								x.log.Warn().Err(doErr).Str("put_url", slot.PutURL.String()).Str("mxc", string(mediaURL)).Msg("XEP-0363 upload PUT failed; sending MXC link anyway")
								messageBody = fallbackCaption + " " + string(mediaURL)
							} else {
								resp.Body.Close()
								if resp.StatusCode < 200 || resp.StatusCode >= 300 {
									x.log.Warn().Int("status", resp.StatusCode).Str("put_url", slot.PutURL.String()).Str("mxc", string(mediaURL)).Msg("XEP-0363 upload returned error status; sending MXC link anyway")
									messageBody = fallbackCaption + " " + string(mediaURL)
								} else {
									getURLStr := slot.GetURL.String()
									// When OOB is present, use user caption only (empty if none) so client displays media inline
									messageBody = userCaption
									oobURL = getURLStr
									x.log.Info().
										Str("get_url", getURLStr).
										Str("filename", filename).
										Str("user_caption", userCaption).
										Str("user_caption_len", fmt.Sprintf("%d", len(userCaption))).
										Str("message_body", messageBody).
										Str("message_body_len", fmt.Sprintf("%d", len(messageBody))).
										Str("oob_url", oobURL).
										Msg("DEBUG: XEP-0363 upload successful, final values before sending")
								}
							}
						}
					}
				}
			}
		}
	default:
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("%w: %s", bridgev2.ErrUnsupportedMessageType, msg.Content.MsgType)).WithSendNotice(true)
	}

	// Handle reply (XEP-0461): if msg.ReplyTo exists, extract XMPP message ID from it
	// msg.ReplyTo is already a *database.Message (the bridge framework resolves it for us)
	var replyID, replyToJID string
	if msg.ReplyTo != nil {
		// Extract XMPP message ID from the database message
		// For messages originally sent from XMPP: ID is stored in database.Message.ID
		// For messages originally sent from Matrix: XMPP ID is stored in MessageMetadata.XMPPMessageID
		var xmppMsgID string
		dbMsgID := string(msg.ReplyTo.ID)
		if strings.HasPrefix(dbMsgID, "matrix:") {
			// Matrix-originated message: check metadata for XMPP message ID
			if msg.ReplyTo.Metadata != nil {
				if meta, ok := msg.ReplyTo.Metadata.(*MessageMetadata); ok && meta != nil && meta.XMPPMessageID != "" {
					xmppMsgID = meta.XMPPMessageID
					x.log.Debug().
						Str("matrix_event_id", dbMsgID).
						Str("xmpp_msg_id", xmppMsgID).
						Msg("DEBUG: Found XMPP message ID in metadata for Matrix-originated message")
				} else {
					x.log.Debug().
						Str("matrix_event_id", dbMsgID).
						Msg("DEBUG: Matrix-originated message has no XMPP message ID in metadata; sending without XEP-0461 reply")
				}
			} else {
				x.log.Debug().
					Str("matrix_event_id", dbMsgID).
					Msg("DEBUG: Matrix-originated message has no metadata; sending without XEP-0461 reply")
			}
		} else {
			// XMPP-originated message: ID is stored directly in database.Message.ID
			xmppMsgID = dbMsgID
		}
		
		if xmppMsgID != "" {
			replyID = xmppMsgID
			// Get sender JID from the reply target message
			// SenderID is a networkid.UserID, which for Jabber is the JID
			senderIDStr := string(msg.ReplyTo.SenderID)
			if senderIDStr != "" {
				replyToJID = senderIDStr
				x.log.Debug().
					Str("reply_id", replyID).
					Str("reply_to_jid", replyToJID).
					Msg("DEBUG: Found reply target, will include XEP-0461 reply element")
			} else {
				x.log.Debug().Str("reply_id", replyID).Msg("DEBUG: Reply target has no sender ID; sending reply without 'to' attribute")
			}
		}
	}

	// Use groupchat for MUC portals; use chat for 1:1 DMs (matches Cheogram and standard 1:1).
	msgType := stanza.ChatMessage
	if x.portalIsMUC(ctx, msg.Portal) {
		msgType = stanza.MessageType("groupchat")
	}
	x.log.Debug().
		Str("final_message_body", messageBody).
		Str("final_message_body_len", fmt.Sprintf("%d", len(messageBody))).
		Str("final_oob_url", oobURL).
		Str("final_oob_url_len", fmt.Sprintf("%d", len(oobURL))).
		Str("reply_id", replyID).
		Str("reply_to_jid", replyToJID).
		Str("to_jid", toJID.String()).
		Str("msg_type", string(msgType)).
		Msg("DEBUG: About to call sendXMPPMessageWithOOB")
	
	xmppMsgID, err := sendXMPPMessageWithOOB(ctx, sess, toJID, messageBody, oobURL, replyID, replyToJID, msgType)
	if err != nil {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("failed to send XMPP message: %w", err)).WithSendNotice(true)
	}
	
	x.log.Debug().
		Str("xmpp_msg_id", xmppMsgID).
		Str("matrix_event_id", string(msg.Event.ID)).
		Msg("DEBUG: sendXMPPMessageWithOOB completed successfully, storing XMPP message ID in metadata")
	now := time.Now()
	id := networkid.MessageID(fmt.Sprintf("matrix:%s", msg.Event.ID))
	
	// Store XMPP message ID in metadata so replies to Matrix-originated messages work
	msgMeta := &MessageMetadata{
		XMPPMessageID: xmppMsgID,
	}
	x.log.Debug().
		Str("matrix_event_id", string(msg.Event.ID)).
		Str("xmpp_msg_id", xmppMsgID).
		Str("db_message_id", string(id)).
		Msg("DEBUG: Stored XMPP message ID in metadata for Matrix-originated message")

	// Send read receipt from bridge bot only when matrix.delivery_receipts is enabled.
	if x.conn.deliveryReceipts && msg.Portal != nil && msg.Portal.MXID != "" && msg.Event != nil && msg.Event.ID != "" {
		if err := x.conn.br.Bot.MarkRead(ctx, msg.Portal.MXID, msg.Event.ID, now); err != nil {
			x.log.Warn().Err(err).Str("room_id", msg.Portal.MXID.String()).Str("event_id", string(msg.Event.ID)).Msg("Failed to send read receipt for bridged message")
		} else {
			x.log.Debug().Str("event_id", string(msg.Event.ID)).Msg("Sent read receipt for bridged message")
		}
	}

	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			Room:       msg.Portal.PortalKey,
			SenderID:   networkid.UserID(x.loginID),
			ID:         id,
			Timestamp:  now,
			SendTxnID:  msg.InputTransactionID,
			Metadata:   msgMeta,
		},
	}, nil
}

// PreHandleMatrixReaction prepares the reaction for sending to XMPP.
// In Jabber, reactions are sent as replies with just the emoji character.
func (x *xmppClient) PreHandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error) {
	// Extract emoji from the reaction key (e.g., "👍" from the reaction event)
	emoji := msg.Content.RelatesTo.Key
	x.log.Debug().
		Str("emoji", emoji).
		Str("target_message_id", string(msg.TargetMessage.ID)).
		Msg("DEBUG: PreHandleMatrixReaction")
	return bridgev2.MatrixReactionPreResponse{
		SenderID: networkid.UserID(x.loginID),
		EmojiID:  "",
		Emoji:    emoji,
	}, nil
}

// HandleMatrixReaction sends a Matrix reaction to XMPP using XEP-0444 Message Reactions format.
// Reactions are sent as <reactions> elements with the target message ID and emoji.
func (x *xmppClient) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil, fmt.Errorf("not connected to XMPP")
	}
	
	toJIDStr := string(msg.Portal.ID)
	if toJIDStr == "" {
		return nil, fmt.Errorf("portal has no remote JID")
	}
	toJID, err := jid.Parse(toJIDStr)
	if err != nil {
		return nil, fmt.Errorf("invalid portal JID format: %w", err)
	}
	
	// Extract XMPP message ID from the target message
	// For messages originally sent from XMPP: ID is stored in database.Message.ID
	// For messages originally sent from Matrix: XMPP ID is stored in MessageMetadata.XMPPMessageID
	var targetMessageID string
	dbMsgID := string(msg.TargetMessage.ID)
	if strings.HasPrefix(dbMsgID, "matrix:") {
		// Matrix-originated message: check metadata for XMPP message ID
		if msg.TargetMessage.Metadata != nil {
			if meta, ok := msg.TargetMessage.Metadata.(*MessageMetadata); ok && meta != nil && meta.XMPPMessageID != "" {
				targetMessageID = meta.XMPPMessageID
				x.log.Debug().
					Str("matrix_event_id", dbMsgID).
					Str("xmpp_msg_id", targetMessageID).
					Msg("DEBUG: Found XMPP message ID in metadata for Matrix-originated reaction target")
			} else {
				x.log.Debug().
					Str("matrix_event_id", dbMsgID).
					Msg("DEBUG: Matrix-originated reaction target has no XMPP message ID in metadata")
				return nil, fmt.Errorf("target message has no XMPP message ID")
			}
		} else {
			return nil, fmt.Errorf("target message has no metadata")
		}
	} else {
		// XMPP-originated message: ID is stored directly in database.Message.ID
		targetMessageID = dbMsgID
		x.log.Debug().
			Str("xmpp_msg_id", targetMessageID).
			Msg("DEBUG: Using XMPP message ID directly for XMPP-originated reaction target")
	}
	
	if targetMessageID == "" {
		return nil, fmt.Errorf("could not determine XMPP message ID for reaction target")
	}
	
	// Send the reaction using XEP-0444 format. Multiple reactions per user per message:
	// we only send the new emoji here, so Jabber shows one reaction per user (consistent with
	// Jabber→Matrix). Future: see README "To-do (unimplemented work)" — multiple reactions per user per message.
	emoji := msg.PreHandleResp.Emoji
	x.log.Debug().
		Str("emoji", emoji).
		Str("target_message_id", targetMessageID).
		Msg("DEBUG: Sending Matrix reaction to XMPP using XEP-0444 format")
	
	// Use groupchat for MUC portals; use chat for 1:1 DMs (matches regular messages and standard 1:1).
	msgType := stanza.ChatMessage
	if x.portalIsMUC(ctx, msg.Portal) {
		msgType = stanza.MessageType("groupchat")
	}
	_, err = sendXMPPReaction(ctx, sess, toJID, targetMessageID, emoji, msgType)
	if err != nil {
		return nil, fmt.Errorf("failed to send reaction to XMPP: %w", err)
	}
	
	return &database.Reaction{}, nil
}

// HandleMatrixReactionRemove removes a Matrix reaction by sending an empty reply or deleting the reaction message.
// In Jabber, we can't easily "remove" a reaction, but we could send an empty reply or just log it.
// For now, we'll just return success (the reaction message in Jabber will remain).
func (x *xmppClient) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error {
	x.log.Debug().
		Str("target_message_id", string(msg.TargetReaction.MessageID)).
		Str("emoji", msg.TargetReaction.Emoji).
		Msg("DEBUG: HandleMatrixReactionRemove - Jabber doesn't support removing reactions, ignoring")
	// Jabber doesn't have a standard way to remove reactions, so we just return success
	// The original reaction message will remain in Jabber
	return nil
}

// getUploadJID discovers the XEP-0363 upload service JID for the given domain.
// It tries common subdomains (upload, share, files) and caches the successful result.
func (x *xmppClient) getUploadJID(ctx context.Context, sess *xmpp.Session, domain string) *jid.JID {
	x.uploadJIDMu.Lock()
	defer x.uploadJIDMu.Unlock()
	// Return cached result if available
	if x.uploadJID != nil {
		return x.uploadJID
	}
	// Try common subdomains used by XMPP servers for file upload
	commonSubdomains := []string{"upload", "share", "files", "http_upload"}
	for _, subdomain := range commonSubdomains {
		candidate := subdomain + "." + domain
		uploadJID, err := jid.Parse(candidate)
		if err != nil {
			continue
		}
		// Try a quick slot request to see if this service exists
		// Use a minimal test request (1 byte file) with a short timeout
		testCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err = upload.GetSlot(testCtx, upload.File{Name: "test", Size: 1, Type: "application/octet-stream"}, uploadJID, sess)
		cancel()
		if err == nil {
			// Success! Cache this JID
			x.uploadJID = &uploadJID
			x.log.Info().Str("upload_jid", uploadJID.String()).Msg("Discovered XEP-0363 upload service")
			return x.uploadJID
		}
		// Log debug info but continue trying other subdomains
		x.log.Debug().Err(err).Str("candidate", candidate).Msg("Upload service discovery attempt failed")
	}
	// None of the common subdomains worked
	x.log.Warn().Str("domain", domain).Msg("Could not discover upload service via common subdomains")
	return nil
}
