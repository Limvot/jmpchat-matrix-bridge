//go:build sipcalls

package connector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/mrtc-sip-bridge/pkg/calls"
	"github.com/williamkray/mrtc-sip-bridge/pkg/core"
	"github.com/williamkray/mrtc-sip-bridge/pkg/hostkit"
	"github.com/williamkray/mrtc-sip-bridge/pkg/rtc"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipaccount"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipua"
	"github.com/williamkray/mrtc-sip-bridge/pkg/widget"
)

func init() {
	registerExtension = func(c *JabberConnector) connectorExtension { return newCallsExt(c) }
}

// registerCallsCommands registers the bot commands of the calls extension (sip-login, sip-logout,
// sip-status and the call commands). It is set by an init() in sipcalls_commands.go and called from
// postInit when the config decoded (enabled or not; the commands check x.enabled themselves).
var registerCallsCommands func(x *callsExt, proc *commands.Processor)

// callsExt embeds the mrtc-sip-bridge call core in jmpchat-matrix-bridge: one SIP account per jabber login,
// calls ring in the caller's Cheogram portal.
type callsExt struct {
	c   *JabberConnector
	br  *bridgev2.Bridge
	log zerolog.Logger // br.Log with component=calls-host (clean)

	cfg     CallsConfig
	cfgErr  error
	enabled bool         // cfg.Enabled after postInit
	rtc     *core.Ingest // created in postInit when enabled

	// filteredMu guards filteredSeen: login+caller -> last filtered INVITE, used to send one notice
	// per burst (the provider retries a rejected call with a new INVITE several times a second).
	filteredMu   sync.Mutex
	filteredSeen map[string]time.Time

	mu       sync.Mutex
	rt       *sipaccount.Runtime     // set in start (guarded by mu)
	calls    *calls.Manager          // set in start (guarded by mu; use manager() / callAPI())
	lkURL    *rtc.ServiceURLResolver // set in start (guarded by mu)
	accounts map[networkid.UserLoginID]*loginCalls
	// carry keeps the SIP credentials of a login whose client was stopped, keyed by login ID, so that a
	// jabber re-login of the same *UserLogin (whose metadata bridgev2 replaces wholesale) keeps them.
	carry map[networkid.UserLoginID]carriedCreds
	// loginBusy marks logins with a sip-login submit in flight (guarded by mu).
	loginBusy map[networkid.UserLoginID]bool
	// stopped is set by stop (guarded by mu): no account may be attached or torn down asynchronously after.
	stopped bool
	// teardown tracks the asynchronous account shutdowns started by loginStopped.
	teardown sync.WaitGroup

	// Test seams (nil = real implementation).
	newRuntime   func(ctx context.Context, cfg sipaccount.NetConfig, log zerolog.Logger) (*sipaccount.Runtime, error)
	newCallStack func(br *bridgev2.Bridge, cfg *hostkit.Config, deps hostkit.CallStackDeps) (*calls.Manager, *rtc.ServiceURLResolver)
	newUA        func(sipua.Config) (sipua.UA, error)
	endLoginFn   func(ctx context.Context, login core.LoginRef)
	notifyFn     func(ctx context.Context, roomID id.RoomID, text string) error
	listPortals  func(ctx context.Context) ([]*bridgev2.Portal, error)
	ensurePL     func(ctx context.Context, roomID id.RoomID) (bool, error)
	// Widget features (sipcalls_widgetfeatures.go).
	openChatRoom func(ctx context.Context, ref core.LoginRef, e164 string) (*core.CallRoom, error) // nil = callHost().ResolveCallRoom
	setRoster    func(ctx context.Context, xc *xmppClient, jid, name string) error                 // nil = xc.SetRosterItem
	backfillChat func(from context.Context, xc *xmppClient, portalID string)                       // nil = xc.backfillChatAsync
	retryDelay   time.Duration
	// handleInviteFn replaces the call manager's HandleInvite (nil = the manager's).
	handleInviteFn func(login core.LoginRef, localE164 string, call sipua.InboundCall)

	// Dialpad widget (sipcalls_widget.go).
	widgetSrv  *widget.Server  // set in start when network.calls.widget.enabled (guarded by mu)
	widgetInst widgetInstaller // the server's installer, or a test fake (guarded by mu)
	widgetStop context.CancelFunc
	widgetWG   sync.WaitGroup
	wseam      widgetSeams
}

// loginCalls is the calls state of one jabber login.
type loginCalls struct {
	id         networkid.UserLoginID
	ul         *bridgev2.UserLogin
	client     *xmppClient         // owner token: only this client may stop the account
	acct       *sipaccount.Account // nil when no credentials (or not started yet)
	registered bool                // the attached account's last known state is Registered (guarded by x.mu)
	authNoted  bool                // auth_failed notice already sent for the current account
	gen        int                 // bumped when acct is (re)started, replaced or stopped; cancels pending retries
	prefs      callPrefs           // cached call preferences (guarded by x.mu; written by setRosterOnly after a save)
}

// carriedCreds are the credentials of a stopped login, valid only for the same *UserLogin.
type carriedCreds struct {
	ul    *bridgev2.UserLogin
	creds sipaccount.Credentials
}

func newCallsExt(c *JabberConnector) *callsExt {
	return &callsExt{
		c:          c,
		log:        zerolog.Nop(),
		accounts:   make(map[networkid.UserLoginID]*loginCalls),
		carry:      make(map[networkid.UserLoginID]carriedCreds),
		loginBusy:  make(map[networkid.UserLoginID]bool),
		retryDelay: 30 * time.Second,
	}
}

func (x *callsExt) init(br *bridgev2.Bridge) {
	x.br = br
	x.log = br.Log.With().Str("component", "calls-host").Logger()
}

// postInit decodes network.calls, wires the MatrixRTC event hook (before the event processor starts)
// and registers the bot commands. A decode error is reported by start.
func (x *callsExt) postInit(proc *commands.Processor, network *yaml.Node) {
	cfg, err := decodeCallsConfig(network)
	if err != nil {
		x.cfgErr = err
		return
	}
	x.cfg = cfg
	x.enabled = cfg.Enabled
	if x.enabled {
		// Defaults are needed now: the commands read the country code and the DTMF redaction flag.
		x.cfg.ApplyDefaults()
		lg := x.bridgeLog().With().Str("component", "matrixrtc").Logger()
		x.rtc = core.NewIngest(core.LogSink{Log: &lg})
		x.rtc.SetLocalNumber(x.loginNumber)
		if mc, ok := x.matrixConnector(); ok && mc.EventProcessor != nil {
			core.RegisterMatrixRTCHandlers(mc.EventProcessor, hostkit.ResolveRTCLogin(x.br, x.hasAccount), x.rtc)
		} else {
			x.log.Warn().Msg("Matrix connector has no event processor: MatrixRTC events are not routed")
		}
	}
	if registerCallsCommands != nil && proc != nil {
		registerCallsCommands(x, proc)
	}
}

// start validates the config and, when calls are enabled, creates the SIP runtime and the call stack,
// then starts every account of a login loaded before the runtime existed.
func (x *callsExt) start(ctx context.Context) error {
	if x.cfgErr != nil {
		return fmt.Errorf("network.calls: %w", x.cfgErr)
	}
	if !x.enabled {
		x.log.Info().Msg("SIP calls disabled")
		x.startWidgetRemoval()
		return nil
	}
	if err := validateCallsConfig(&x.cfg, CheogramEnabled(x.br)); err != nil {
		return err
	}
	if encryptionDefault(x.br) {
		x.log.Warn().Msg("encryption.default is true: calls only ring in unencrypted portals")
	}
	newRuntime := x.newRuntime
	if newRuntime == nil {
		newRuntime = sipaccount.NewRuntime
	}
	// telemetry.Init (obs.Use) already ran in PostInit, so the registrations gauge lands on the
	// current instruments.
	netCfg := x.cfg.NetConfig()
	netCfg.KeyPrefix = "network.calls."
	rt, err := newRuntime(ctx, netCfg, x.bridgeLog())
	if err != nil {
		return fmt.Errorf("start SIP runtime: %w", err)
	}
	newStack := x.newCallStack
	if newStack == nil {
		newStack = hostkit.NewCallStack
	}
	mgr, lk := newStack(x.br, &x.cfg.Config, hostkit.CallStackDeps{
		Host:        x.callHost(),
		Dial:        x.dial,
		LoginNumber: x.loginNumber,
		Log:         x.bridgeLog(),
	})
	if x.rtc != nil && mgr != nil {
		x.rtc.SetSink(mgr)
	}
	if err := x.startWidget(); err != nil {
		if mgr != nil {
			mgr.Stop(ctx)
		}
		rt.Close()
		return err
	}

	x.mu.Lock()
	x.calls, x.lkURL, x.rt = mgr, lk, rt
	var pending []startReq
	for _, lc := range x.accounts {
		if lc.acct != nil || lc.ul == nil {
			continue
		}
		creds, ok := sipCreds(loginMeta(lc.ul))
		if !ok {
			continue
		}
		pending = append(pending, x.installAccountLocked(lc, x.newAccountLocked(lc.ul, *creds)))
	}
	x.mu.Unlock()
	for _, p := range pending {
		x.startAsync(p)
	}
	x.log.Info().Int("accounts", len(pending)).Msg("SIP calls enabled")
	return nil
}

// stop ends all calls and closes the runtime. Logins were already disconnected by the framework; any
// account still attached is stopped first (Runtime.Close requires that).
func (x *callsExt) stop() {
	x.mu.Lock()
	x.stopped = true
	mgr, rt := x.calls, x.rt
	x.mu.Unlock()
	// Asynchronous teardowns of disconnected logins finish first (bounded).
	done := make(chan struct{})
	go func() { x.teardown.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		x.log.Warn().Msg("Timed out waiting for SIP account teardown")
	}
	x.mu.Lock()
	var left []*loginCalls
	for id, lc := range x.accounts {
		if lc.acct != nil {
			left = append(left, &loginCalls{id: id, acct: lc.acct})
		}
		lc.gen++
		lc.acct = nil
	}
	x.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	x.stopWidget()
	if mgr != nil {
		mgr.Stop(ctx)
	}
	for _, lc := range left {
		lc.acct.Stop(ctx)
	}
	rt.Close()
}

// startAttrs are the extra bridge.start attributes.
func (x *callsExt) startAttrs() []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.Bool("calls.enabled", x.enabled)}
	if !x.enabled {
		return attrs
	}
	x.mu.Lock()
	lk := x.lkURL
	x.mu.Unlock()
	lkSource := ""
	if lk != nil {
		lkSource = lk.Source()
	}
	cfg := &x.cfg
	return append(attrs,
		attribute.Int("rtp.port_range_size", cfg.RTPPortMax-cfg.RTPPortMin+1),
		attribute.Int("sip.port_range_size", cfg.SIPPortMax-cfg.SIPPortMin+1),
		attribute.Int("ring_timeout_s", cfg.RingTimeoutSeconds),
		attribute.String("livekit.service_url.source", lkSource),
	)
}

// hasAccount reports whether the login has an attached SIP account (map read only, no I/O).
func (x *callsExt) hasAccount(loginID networkid.UserLoginID) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	lc := x.accounts[loginID]
	return lc != nil && lc.acct != nil
}

// account returns the login's attached SIP account, nil if none.
func (x *callsExt) account(loginID networkid.UserLoginID) *sipaccount.Account {
	x.mu.Lock()
	defer x.mu.Unlock()
	if lc := x.accounts[loginID]; lc != nil {
		return lc.acct
	}
	return nil
}

// loginNumber returns the E.164 of the login's SIP account, "" if none (no I/O).
func (x *callsExt) loginNumber(ref core.LoginRef) string {
	if a := x.account(networkid.UserLoginID(ref)); a != nil {
		return a.Creds().DisplayNumber
	}
	return ""
}

// dial places an outbound call on the login's SIP account.
func (x *callsExt) dial(ref core.LoginRef, req sipua.DialRequest) (sipua.OutboundCall, error) {
	a := x.account(networkid.UserLoginID(ref))
	if a == nil {
		return nil, sipua.ErrNotRegistered
	}
	return a.Dial(req)
}

// manager returns the call manager, nil before start (or when calls are disabled).
func (x *callsExt) manager() *calls.Manager {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.calls
}

// callAPI returns the call manager as a hostkit.CallAPI, or a nil interface (never a typed nil) while
// it does not exist. Suitable for hostkit.CommandDeps.Calls.
func (x *callsExt) callAPI() hostkit.CallAPI {
	if m := x.manager(); m != nil {
		return m
	}
	return nil
}

// runtime returns the SIP runtime, nil before start.
func (x *callsExt) runtime() *sipaccount.Runtime {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.rt
}

// commandPrefix returns the bridge's bot command prefix.
func (x *callsExt) commandPrefix() string {
	if x.br != nil && x.br.Config != nil && x.br.Config.CommandPrefix != "" {
		return x.br.Config.CommandPrefix
	}
	return "!jabber"
}

func (x *callsExt) bridgeLog() zerolog.Logger {
	if x.br == nil {
		return zerolog.Nop()
	}
	return x.br.Log
}

func (x *callsExt) matrixConnector() (*matrix.Connector, bool) {
	if x.br == nil {
		return nil, false
	}
	mc, ok := x.br.Matrix.(*matrix.Connector)
	return mc, ok && mc != nil
}
