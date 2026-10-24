//go:build sipcalls

package connector

// Jingle call transport: wires mrtc-sip-bridge's jingleleg (ADR-014) onto this bridge's XMPP
// session, so JMP/Cheogram native Jingle calls reach the MatrixRTC call core without SIP.
// This is the entire fork-side media/signalling delta: the session stays with xmppClient and
// everything call-shaped lives in mrtc-sip-bridge's pkg/jingleleg.

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"sync"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/mrtc-sip-bridge/pkg/core"
	"github.com/williamkray/mrtc-sip-bridge/pkg/jingleleg"
	"github.com/williamkray/mrtc-sip-bridge/pkg/leg"
)

// errNoSession is returned by the transport while the XMPP session is down.
var errNoSession = errors.New("jingle transport: no XMPP session")

// jingleTransport sends Jingle stanzas over the login's XMPP session.
type jingleTransport struct{ cl *xmppClient }

var _ jingleleg.Transport = jingleTransport{}

func (t jingleTransport) session() *xmpp.Session {
	t.cl.mu.Lock()
	defer t.cl.mu.Unlock()
	return t.cl.session
}

func (t jingleTransport) SendIQ(ctx context.Context, to, iqType, id string, payload []byte) error {
	sess := t.session()
	if sess == nil {
		return errNoSession
	}
	toJID, err := jid.Parse(to)
	if err != nil {
		return err
	}
	typ := stanza.SetIQ
	switch iqType {
	case "result":
		typ = stanza.ResultIQ
	case "error":
		typ = stanza.ErrorIQ
	}
	iq := stanza.IQ{ID: id, Type: typ, To: toJID}
	return sess.Send(ctx, iq.Wrap(xml.NewDecoder(bytes.NewReader(payload))))
}

func (t jingleTransport) SendMessage(ctx context.Context, to, msgType, id string, payload []byte) error {
	sess := t.session()
	if sess == nil {
		return errNoSession
	}
	toJID, err := jid.Parse(to)
	if err != nil {
		return err
	}
	msg := stanza.Message{ID: id, Type: stanza.ChatMessage, To: toJID}
	return sess.Send(ctx, msg.Wrap(xml.NewDecoder(bytes.NewReader(payload))))
}

// --- extension hooks (connectorExtension) ---

// feedCallJMI routes a parsed XEP-0353 JMI action to the login's Jingle leg.
func (x *callsExt) feedCallJMI(ctx context.Context, cl *xmppClient, fromBare string, fromSelf bool, jmi *jingleMessageInfo) bool {
	if cl == nil || jmi == nil || x.cfg.Transport != callsTransportJingle {
		return false
	}
	ua := x.jingleUA(cl.loginID)
	if ua == nil {
		return false
	}
	j := jingleleg.JMI{Action: jmi.action, ID: jmi.sid}
	if jmi.action == jingleleg.JMIPropose && jmi.media != "" {
		j.Descriptions = []jingleleg.Description{{Media: jmi.media}}
	}
	return ua.HandleJMI(ctx, fromBare, fromSelf, j)
}

// feedCallIQ routes a raw <jingle/> IQ payload to the login's Jingle leg.
func (x *callsExt) feedCallIQ(ctx context.Context, cl *xmppClient, from, iqID string, payload []byte) bool {
	if cl == nil || x.cfg.Transport != callsTransportJingle {
		return false
	}
	ua := x.jingleUA(cl.loginID)
	if ua == nil {
		return false
	}
	return ua.HandleIQ(ctx, from, "set", iqID, payload)
}

// --- per-login Jingle legs ---

// jingleRuntime holds the per-login Jingle legs (one per XMPP session).
type jingleRuntime struct {
	mu  sync.Mutex
	uas map[networkid.UserLoginID]*jingleleg.UA
}

// jingleUA returns the login's running Jingle leg, or nil.
func (x *callsExt) jingleUA(loginID networkid.UserLoginID) *jingleleg.UA {
	x.jingle.mu.Lock()
	defer x.jingle.mu.Unlock()
	return x.jingle.uas[loginID]
}

// startJingleFor starts one Jingle leg for a login (no SIP credentials needed: the leg rides
// on the login's own XMPP session). Calls ring through the existing call core unchanged.
func (x *callsExt) startJingleFor(ul *bridgev2.UserLogin, cl *xmppClient) error {
	loginID := cl.loginID
	ua, err := jingleleg.New(jingleleg.Config{
		LocalBare:  string(loginID),
		Transport:  jingleTransport{cl: cl},
		PeerDomain: cheogramDomain,
		NewEngine: jingleleg.NewWebRTCEngine(jingleleg.WebRTCEngineConfig{
			ICEServers: jingleICEServers(x.cfg),
			PortMin:    uint16(clampPort(x.cfg.RTPPortMin)),
			PortMax:    uint16(clampPort(x.cfg.RTPPortMax)),
			ExternalIP: x.cfg.ExternalIP,
			Log:        x.log,
		}),
		OnStateChange: func(ev leg.StateEvent) {
			x.log.Info().Str("login", string(loginID)).Str("state", ev.State.String()).
				Msg("Jingle call leg state")
		},
		OnInvite: func(call leg.InboundCall) {
			// Same entry point the SIP accounts use (sipcalls_accounts.go onInvite tail):
			// ring every Matrix client through MatrixRTC. Roster-only gating is handled by
			// the SIP path via account prefs; TODO: map it onto call.Info() for Jingle too.
			if x.calls != nil {
				x.calls.HandleInvite(core.LoginRef(loginID), loginNumberFromJID(string(loginID)), call)
			} else {
				call.Reject(480, "Temporarily Unavailable")
			}
		},
	})
	if err != nil {
		return err
	}
	if err := ua.Start(context.Background()); err != nil {
		return err
	}
	x.jingle.mu.Lock()
	if x.jingle.uas == nil {
		x.jingle.uas = map[networkid.UserLoginID]*jingleleg.UA{}
	}
	x.jingle.uas[loginID] = ua
	x.jingle.mu.Unlock()
	return nil
}

// stopJingleFor stops and forgets the login's Jingle leg.
func (x *callsExt) stopJingleFor(loginID networkid.UserLoginID) {
	x.jingle.mu.Lock()
	ua := x.jingle.uas[loginID]
	delete(x.jingle.uas, loginID)
	x.jingle.mu.Unlock()
	if ua != nil {
		_ = ua.Stop(context.Background())
	}
}

// jingleDial places an outbound call from the login's Jingle leg (callsExt.dial).
func (x *callsExt) jingleDial(ref core.LoginRef, req leg.DialRequest) (leg.OutboundCall, error) {
	ua := x.jingleUA(networkid.UserLoginID(ref))
	if ua == nil {
		return nil, leg.ErrNotRegistered
	}
	return ua.Dial(req)
}

// jingleICEServers merges the configured jingle ice_servers with the shared stun_server.
func jingleICEServers(cfg CallsConfig) []string {
	servers := append([]string(nil), cfg.ICEServers...)
	if cfg.STUNServer != "" {
		servers = append(servers, cfg.STUNServer)
	}
	return servers
}

// clampPort bounds a configured UDP port to the valid range (0 = default).
func clampPort(p int) int {
	if p < 0 || p > 65535 {
		return 0
	}
	return p
}

// loginNumberFromJID returns the E.164 a "+E.164@…" login JID stands for, "" otherwise.
func loginNumberFromJID(loginJID string) string {
	user := loginJID
	if i := indexByte(user, '/'); i >= 0 {
		user = user[:i]
	}
	if i := indexByte(user, '@'); i >= 0 {
		user = user[:i]
	}
	if len(user) > 1 && user[0] == '+' {
		return user
	}
	return ""
}

// indexByte avoids importing strings just for this.
func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// keep sync referenced for the future per-login map growth.
var _ = sync.Mutex{}

// ensureJingleRuntime initialises the runtime map (called from callsExt init paths).
func (x *callsExt) ensureJingleRuntime() {
	x.jingle.mu.Lock()
	if x.jingle.uas == nil {
		x.jingle.uas = map[networkid.UserLoginID]*jingleleg.UA{}
	}
	x.jingle.mu.Unlock()
}
