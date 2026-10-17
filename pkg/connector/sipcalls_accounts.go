//go:build sipcalls

package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/mrtc-sip-bridge/pkg/core"
	"github.com/williamkray/mrtc-sip-bridge/pkg/obs"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipaccount"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipua"
)

// sipExtraKey is the JabberLoginMeta.Extra key holding the login's SIP credentials.
const sipExtraKey = "sip"

// sipCreds returns the SIP credentials stored in meta; ok=false if absent, invalid or incomplete.
func sipCreds(meta *JabberLoginMeta) (*sipaccount.Credentials, bool) {
	if meta == nil || meta.Extra == nil {
		return nil, false
	}
	raw, ok := meta.Extra[sipExtraKey]
	if !ok {
		return nil, false
	}
	var c sipaccount.Credentials
	if err := json.Unmarshal(raw, &c); err != nil || !c.Complete() {
		return nil, false
	}
	return &c, true
}

// setSIPCreds stores c in meta (other Extra keys are kept); nil deletes the key (and Extra if empty).
func setSIPCreds(meta *JabberLoginMeta, c *sipaccount.Credentials) {
	if meta == nil {
		return
	}
	if c == nil {
		delete(meta.Extra, sipExtraKey)
		if len(meta.Extra) == 0 {
			meta.Extra = nil
		}
		return
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return // cannot happen for a struct of strings
	}
	if meta.Extra == nil {
		meta.Extra = make(map[string]json.RawMessage)
	}
	meta.Extra[sipExtraKey] = raw
}

// loginMeta returns ul's jabber metadata (nil if none).
func loginMeta(ul *bridgev2.UserLogin) *JabberLoginMeta {
	if ul == nil || ul.UserLogin == nil {
		return nil
	}
	meta, _ := ul.Metadata.(*JabberLoginMeta)
	return meta
}

// startReq is one pending asynchronous account start.
type startReq struct {
	lc   *loginCalls
	acct *sipaccount.Account
	gen  int
}

// loginLoaded attaches a (new) client of a login. A running account is taken over by the replacement
// client; otherwise the stored credentials are started (by start when the runtime does not exist yet).
func (x *callsExt) loginLoaded(ul *bridgev2.UserLogin, client *xmppClient) {
	if !x.enabled || ul == nil {
		return
	}
	meta := loginMeta(ul)
	x.mu.Lock()
	lc := x.accounts[ul.ID]
	if lc == nil {
		lc = &loginCalls{id: ul.ID}
		x.accounts[ul.ID] = lc
	}
	lc.ul, lc.client = ul, client
	// bridgev2 replaces the metadata of an existing login on a jabber re-login: keep the SIP credentials.
	if cc, ok := x.carry[ul.ID]; ok {
		delete(x.carry, ul.ID)
		if _, has := sipCreds(meta); !has && cc.ul == ul && lc.acct == nil {
			setSIPCreds(meta, &cc.creds)
		}
	}
	if lc.acct != nil {
		if _, has := sipCreds(meta); !has {
			creds := lc.acct.Creds()
			setSIPCreds(meta, &creds)
		}
		acct := lc.acct
		x.mu.Unlock()
		if acct.Running() {
			return // takeover: the running UA stays
		}
		// The account is not running (failed or still starting): start it for the new client. This
		// cancels a pending retry; a concurrent Start of the same account is a no-op.
		var req *startReq
		x.mu.Lock()
		if x.accounts[ul.ID] == lc && lc.acct == acct && x.rt != nil {
			lc.gen++
			req = &startReq{lc: lc, acct: acct, gen: lc.gen}
		}
		x.mu.Unlock()
		if req != nil {
			x.startAsync(*req)
		}
		return
	}
	creds, ok := sipCreds(meta)
	if !ok || x.rt == nil {
		x.mu.Unlock()
		return
	}
	req := x.installAccountLocked(lc, x.newAccountLocked(ul, *creds))
	x.mu.Unlock()
	x.startAsync(req)
}

// loginStopped stops the login's account when the stopped client owns it (Disconnect is terminal:
// bridge stop, client replacement, logout). An old client of a replaced login is ignored.
func (x *callsExt) loginStopped(client *xmppClient) {
	if !x.enabled || client == nil {
		return
	}
	x.mu.Lock()
	lc := x.accounts[client.loginID]
	if lc == nil || lc.client != client {
		x.mu.Unlock()
		return
	}
	delete(x.accounts, client.loginID)
	lc.gen++
	acct, ul := lc.acct, lc.ul
	lc.acct = nil
	if creds, ok := sipCreds(loginMeta(ul)); ok {
		x.carry[client.loginID] = carriedCreds{ul: ul, creds: *creds}
	}
	if acct == nil {
		x.mu.Unlock()
		return
	}
	// bridgev2 calls Disconnect under its login cache lock: the (slow) shutdown must not run there.
	// After stop() it runs synchronously (stop no longer waits for new teardowns).
	async := !x.stopped
	if async {
		x.teardown.Add(1)
	}
	x.mu.Unlock()
	loginID := client.loginID
	run := func() {
		ctx, span := obs.Start(context.Background(), "bridge.sip_disconnect", trace.WithAttributes(sipSpanAttrs(loginID, acct.Creds())...))
		defer span.End()
		x.shutdownAccount(ctx, loginID, acct)
	}
	if !async {
		run()
		return
	}
	go func() {
		defer x.teardown.Done()
		run()
	}()
}

// newLoginAccount creates a stopped SIP account for ul whose callbacks act only while it is the login's
// attached account. With no runtime yet its Start fails.
func (x *callsExt) newLoginAccount(ul *bridgev2.UserLogin, creds sipaccount.Credentials) *sipaccount.Account {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.newAccountLocked(ul, creds)
}

func (x *callsExt) newAccountLocked(ul *bridgev2.UserLogin, creds sipaccount.Credentials) *sipaccount.Account {
	loginID := ul.ID
	lg := x.bridgeLog().With().Stringer("user_mxid", ul.UserMXID).Str("login", string(loginID)).
		Str("sip_user", sipua.MaskUsername(creds.Username)).Logger()
	var a *sipaccount.Account
	// a is assigned before Start can run any callback (callbacks only fire from a started UA).
	a = x.rt.NewAccount(sipaccount.AccountConfig{
		Creds:    creds,
		Log:      lg, // no component key: sipua adds its own, the account adds component=sip
		OnState:  func(ev sipua.StateEvent) { x.onState(loginID, a, ev) },
		OnInvite: func(call sipua.InboundCall) { x.onInvite(loginID, a, call) },
		NewUA:    x.newUA,
	})
	return a
}

// installAccountLocked makes acct the login's attached account and returns its start request.
func (x *callsExt) installAccountLocked(lc *loginCalls, acct *sipaccount.Account) startReq {
	lc.acct = acct
	lc.registered = false
	lc.authNoted = false
	lc.gen++
	return startReq{lc: lc, acct: acct, gen: lc.gen}
}

// attachAccount makes acct (already registered, e.g. by sip-login) the login's attached account and
// starts the call power-level sweep. A different previously attached account is stopped. It refuses
// (false, acct untouched) when the bridge is stopping or the login's current client is not ul's.
func (x *callsExt) attachAccount(ul *bridgev2.UserLogin, acct *sipaccount.Account) bool {
	xc, _ := ul.Client.(*xmppClient)
	x.mu.Lock()
	lc := x.accounts[ul.ID]
	if x.stopped || lc == nil || lc.client == nil || lc.client != xc {
		x.mu.Unlock()
		return false
	}
	old := lc.acct
	x.installAccountLocked(lc, acct)
	lc.registered = true // sip-login attaches only after the registrar accepted the account
	x.mu.Unlock()
	ctx := context.Background()
	if old != nil && old != acct {
		x.shutdownAccount(ctx, ul.ID, old)
	}
	x.sweepCallPowerLevels(ctx, ul.ID)
	return true
}

// detachAccount stops the login's attached account (ending its calls first) and detaches it. The stored
// credentials are not touched. Reports whether an account was attached.
func (x *callsExt) detachAccount(ctx context.Context, loginID networkid.UserLoginID) bool {
	x.mu.Lock()
	lc := x.accounts[loginID]
	var acct *sipaccount.Account
	if lc != nil {
		acct = lc.acct
		lc.acct = nil
		lc.gen++
	}
	x.mu.Unlock()
	if acct == nil {
		return false
	}
	x.shutdownAccount(ctx, loginID, acct)
	return true
}

// shutdownAccount ends the login's calls (EndLogin, 8 s) and then stops the account's UA. ctx only
// carries the caller's span.
func (x *callsExt) shutdownAccount(ctx context.Context, loginID networkid.UserLoginID, acct *sipaccount.Account) {
	ectx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
	x.endLogin(ectx, core.LoginRef(loginID))
	cancel()
	acct.Stop(ctx)
}

func (x *callsExt) endLogin(ctx context.Context, ref core.LoginRef) {
	if x.endLoginFn != nil {
		x.endLoginFn(ctx, ref)
		return
	}
	if m := x.manager(); m != nil {
		m.EndLogin(ctx, ref)
	}
}

// isCurrent reports whether acct is still lc's attached account at generation gen.
func (x *callsExt) isCurrent(r startReq) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.accounts[r.lc.id] == r.lc && r.lc.acct == r.acct && r.lc.gen == r.gen
}

// owns reports whether acct is still lc's attached account (any generation).
func (x *callsExt) owns(lc *loginCalls, acct *sipaccount.Account) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.accounts[lc.id] == lc && lc.acct == acct
}

// startAsync starts the account in a goroutine under a bridge.sip_connect root span, retrying once.
func (x *callsExt) startAsync(r startReq) {
	go func() {
		ctx, span := obs.Start(context.Background(), "bridge.sip_connect", trace.WithAttributes(sipSpanAttrs(r.lc.id, r.acct.Creds())...))
		defer span.End()
		x.connect(ctx, span, r, true)
	}()
}

func (x *callsExt) connect(ctx context.Context, span trace.Span, r startReq, retry bool) {
	if !x.isCurrent(r) {
		return
	}
	if err := r.acct.Start(ctx); err != nil {
		obs.Fail(span, err)
		x.logFor(ctx).Err(err).Str("login", string(r.lc.id)).Msg("Failed to start SIP account")
		if retry {
			// The timer goroutine never gets the live span: the retry is a linked root.
			from := obs.CarrySpan(context.Background(), ctx)
			time.AfterFunc(x.retryDelay, func() {
				if !x.isCurrent(r) {
					return
				}
				rctx, rspan := obs.StartLinkedRoot(context.Background(), "bridge.sip_connect", from,
					trace.WithAttributes(sipSpanAttrs(r.lc.id, r.acct.Creds())...), trace.WithAttributes(attribute.Bool("retry", true)))
				defer rspan.End()
				x.connect(rctx, rspan, r, false)
			})
		}
		return
	}
	if !x.owns(r.lc, r.acct) {
		// Stopped or replaced while starting: do not leave a second registration running.
		r.acct.Stop(ctx)
		return
	}
	x.sweepCallPowerLevels(ctx, r.lc.id)
}

// onState runs on the UA goroutine and must not block.
func (x *callsExt) onState(loginID networkid.UserLoginID, acct *sipaccount.Account, ev sipua.StateEvent) {
	x.mu.Lock()
	lc := x.accounts[loginID]
	if lc == nil || lc.acct != acct {
		x.mu.Unlock()
		return
	}
	var notify *bridgev2.UserLogin
	switch ev.State {
	case sipua.StateRegistered:
		lc.registered = true
		lc.authNoted = false
	case sipua.StateAuthFailed:
		lc.registered = false
		if !lc.authNoted {
			lc.authNoted = true
			notify = lc.ul
		}
	case sipua.StateIdle, sipua.StateTransientError, sipua.StateStopped:
		lc.registered = false
	}
	x.mu.Unlock()
	if notify != nil {
		go x.sendAuthNotice(notify, acct.Creds().DisplayNumber, ev.StatusCode)
	}
}

// sendAuthNotice tells the login's owner (management room) that the registrar rejected the account.
func (x *callsExt) sendAuthNotice(ul *bridgev2.UserLogin, number string, code int) {
	if ul == nil || ul.User == nil || ul.User.User == nil || ul.User.ManagementRoom == "" {
		return
	}
	text := fmt.Sprintf("SIP registration for %s was rejected (SIP %d). Fix the password with `%s sip-login`.",
		number, code, x.commandPrefix())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := x.notify(ctx, ul.User.ManagementRoom, text); err != nil {
		x.log.Warn().Err(err).Str("login", string(ul.ID)).Msg("Failed to send SIP auth failure notice")
	}
}

func (x *callsExt) notify(ctx context.Context, roomID id.RoomID, text string) error {
	if x.notifyFn != nil {
		return x.notifyFn(ctx, roomID, text)
	}
	if x.br == nil || x.br.Bot == nil {
		return fmt.Errorf("no bridge bot")
	}
	content := format.RenderMarkdown(text, true, false)
	content.MsgType = event.MsgNotice
	_, err := x.br.Bot.SendMessage(ctx, roomID, event.EventMessage, &event.Content{Parsed: &content}, nil)
	return err
}

// onInvite hands an inbound call to the call manager (diago handler goroutine, returns quickly).
func (x *callsExt) onInvite(loginID networkid.UserLoginID, acct *sipaccount.Account, call sipua.InboundCall) {
	x.mu.Lock()
	lc := x.accounts[loginID]
	attached := lc != nil && lc.acct == acct
	mgr := x.calls
	x.mu.Unlock()
	if !attached || mgr == nil || !acct.Running() {
		call.Reject(480, "Temporarily Unavailable")
		return
	}
	mgr.HandleInvite(core.LoginRef(loginID), acct.Creds().DisplayNumber, call)
}

// sipSpanAttrs are the attributes of the account spans (no secrets: masked user, registrar host).
func sipSpanAttrs(loginID networkid.UserLoginID, c sipaccount.Credentials) []attribute.KeyValue {
	host := c.Registrar
	if h, _, err := net.SplitHostPort(c.Registrar); err == nil {
		host = h
	}
	transport := c.Transport
	if transport == "" {
		transport = "udp"
	}
	return []attribute.KeyValue{
		obs.AttrSIPUserMasked.String(sipua.MaskUsername(c.Username)),
		obs.AttrServerAddress.String(host),
		obs.AttrNetworkTransport.String(transport),
		attribute.String("xmpp.login.jid", string(loginID)),
		obs.AttrCallLocalE164.String(c.DisplayNumber),
	}
}

// logFor returns x.log annotated with ctx's span (our own lines under spans).
func (x *callsExt) logFor(ctx context.Context) *zerolog.Logger {
	return obs.Logger(ctx, &x.log)
}
