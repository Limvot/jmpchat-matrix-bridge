package connector

import (
	"context"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/configupgrade"
	"mellium.im/sasl"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/mautrix/jabber/pkg/telemetry"
)

// JabberConfig is the connector-specific config under network.jabber (framework nests it by network ID).
type JabberConfig struct {
	ResourcePrefix string        `yaml:"resource_prefix"`
	Cheogram       bool          `yaml:"cheogram"` // resolve phone numbers and short codes to Cheogram JIDs (SMS/MMS)
	CardDAV        CardDAVConfig `yaml:"carddav"`
}

// BackfillConfig holds backfill settings from config.backfill, used by MAM backfill-on-view and full backfill on login / sync-roster.
type BackfillConfig struct {
	Enabled            bool `yaml:"enabled"`
	MaxInitialMessages int  `yaml:"max_initial_messages"`
}

// JabberConnector implements the bridgev2 NetworkConnector interface for
// the Matrix-Jabber (XMPP) bridge.
type JabberConnector struct {
	br                  *bridgev2.Bridge
	networkConfig       *JabberConfig  // set from main after LoadConfig; bridge only has BridgeConfig
	backfillConfig      *BackfillConfig // set from main from config.backfill
	deliveryReceipts bool // whether to send read receipts when messages are bridged (matrix.delivery_receipts)
	appServiceID     string // appservice.id from config, used in ghost display names (e.g. "user@example.com (cheogram)")

	extnOnce sync.Once
	extn     connectorExtension // optional build-tagged extension (nil in the default build)
}

// Ensure we implement the interface at compile time.
var _ bridgev2.NetworkConnector = (*JabberConnector)(nil)

// RosterSyncer is implemented by the real XMPP client (not the stub). Use it to force a
// roster fetch so display names and room names pick up renames done in another client.
type RosterSyncer interface {
	SyncRoster(ctx context.Context)
}

// FullBackfillRunner is implemented by the real XMPP client. RunFullBackfill runs MAM backfill
// for all roster DMs (skips MUC), so portals are created and history filled before new messages arrive.
// When backfill is enabled, this is triggered on sync-roster and optionally after login (full_backfill_on_login).
type FullBackfillRunner interface {
	RunFullBackfill(ctx context.Context)
}

// RosterManager is implemented by the real XMPP client. Use it to add or rename contacts
// in the Jabber roster from Matrix (e.g. !jabber add-to-roster <jid> <name>).
type RosterManager interface {
	SetRosterItem(ctx context.Context, jidStr, displayName string) error
}

// RosterRemover is implemented by the real XMPP client. Use it to remove a contact from the
// Jabber roster from Matrix (e.g. !jabber remove-from-roster <jid>).
type RosterRemover interface {
	RemoveRosterItem(ctx context.Context, jidStr string) error
}

// RosterSearcher is implemented by the real XMPP client. Use it to search the roster
// for contacts matching a JID or name pattern.
type RosterSearcher interface {
	SearchRoster(ctx context.Context, query string) ([]RosterContact, error)
}

// RosterContact represents a contact in the roster.
type RosterContact struct {
	JID  string
	Name string
}

// MUCJoiner is implemented by the real XMPP client. Use it to join or leave MUC rooms
// from bridge commands (e.g. !jabber join, !jabber leave).
type MUCJoiner interface {
	JoinMUC(ctx context.Context, roomJID string) error
	LeaveMUC(ctx context.Context, roomJID string) error
}

// onceWarnDebugIdentifiers ensures we only log the debug-identifiers warning once per process.
var onceWarnDebugIdentifiers sync.Once

// warnIfDebugIdentifiers logs a one-time warning that debug log output may contain
// JIDs and other potentially private data. Call before emitting Debug-level logs
// that include identifiers, so operators know to protect log output when using debug.
func warnIfDebugIdentifiers(log *zerolog.Logger) {
	onceWarnDebugIdentifiers.Do(func() {
		log.Warn().Msg("Debug logging may include JIDs and other potentially private data; ensure log output is protected")
	})
}

// Init stores the bridge reference.
func (c *JabberConnector) Init(br *bridgev2.Bridge) {
	c.br = br
	if e := c.extension(); e != nil {
		e.init(br)
	}
}

// SetNetworkConfig sets the decoded network section config. Called from main after LoadConfig
// because the bridge only receives BridgeConfig, not the full config with Network.
func (c *JabberConnector) SetNetworkConfig(cfg *JabberConfig) {
	c.networkConfig = cfg
}

// SetDeliveryReceipts sets whether the bridge should send read receipts after successfully
// bridging a message. Called from main from matrix.delivery_receipts in the full config.
func (c *JabberConnector) SetDeliveryReceipts(enabled bool) {
	c.deliveryReceipts = enabled
}

// SetBackfillConfig sets backfill settings from config.backfill. Called from main after LoadConfig.
func (c *JabberConnector) SetBackfillConfig(cfg *BackfillConfig) {
	c.backfillConfig = cfg
}

// SetAppServiceID sets the appservice ID from config. Used in ghost display names (e.g. "user@example.com (cheogram)").
func (c *JabberConnector) SetAppServiceID(id string) {
	c.appServiceID = id
}

// CheogramEnabled returns true if network.cheogram is true in the config,
// or if MAUTRIX_JABBER_CHEOGRAM is true/1 (fallback when config is unavailable).
func CheogramEnabled(br *bridgev2.Bridge) bool {
	if br != nil {
		if cfg := getJabberConfigFromBridge(br); cfg != nil && cfg.Cheogram {
			return true
		}
	}
	if b, err := strconv.ParseBool(strings.TrimSpace(strings.ToLower(os.Getenv("MAUTRIX_JABBER_CHEOGRAM")))); err == nil && b {
		return true
	}
	return false
}

// getJabberConfigFromBridge returns the connector's network config. The bridge's Config is only
// BridgeConfig (no Network field); main decodes the network section and sets it via SetNetworkConfig.
func getJabberConfigFromBridge(br *bridgev2.Bridge) *JabberConfig {
	if br == nil {
		return nil
	}
	v := reflect.ValueOf(br).Elem()
	netField := v.FieldByName("Network")
	if !netField.IsValid() || netField.IsNil() {
		return nil
	}
	if jc, ok := netField.Interface().(*JabberConnector); ok && jc != nil {
		return jc.networkConfig
	}
	return nil
}

// Start is called when the bridge is starting. In the scaffold it is a no-op;
// later it will load UserLogins and spawn XMPP reconnect loops.
func (c *JabberConnector) Start(ctx context.Context) error {
	if e := c.extension(); e != nil {
		return e.start(ctx)
	}
	return nil
}

// GetName returns bridge metadata for m.bridge and example config.
func (c *JabberConnector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:         "Jabber",
		NetworkURL:          "https://xmpp.org/",
		NetworkID:           "jabber",
		BeeperBridgeType:    "github.com/mautrix/jabber",
		DefaultPort:         29318, // mau.fi/ports or any free port
		DefaultCommandPrefix: "!jabber",
	}
}

// GetDBMetaTypes is now in dbmeta.go to keep metadata types organized.

// GetCapabilities returns general network capabilities. ImplicitReadReceipts is true so that
// when matrix.delivery_receipts is enabled, the bridge sends a read receipt when a message
// is successfully bridged to Jabber. Actual sending is gated by deliveryReceipts in HandleMatrixMessage.
func (c *JabberConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{
		ImplicitReadReceipts: true,
	}
}

// GetConfig returns the example config snippet for network, the struct to unmarshal into, and an upgrader.
// The upgrader must Copy our keys (e.g. cheogram) from the user's config into the base during upgrade,
// otherwise the framework writes the example value (false) and overwrites the user's true on restart.
func (c *JabberConnector) GetConfig() (example string, data any, upgrader configupgrade.Upgrader) {
	example = `# Connector config (under network by the framework).
# The XMPP server is derived from the JID's domain at login; there is no server setting here.
# resource_prefix: "mautrix"
# Resolve phone numbers and short codes to Cheogram JIDs (SMS/MMS). Set to true to enable.
cheogram: false
`
	e := c.extension()
	if e != nil {
		example += e.exampleConfig()
	}
	example += cardDAVExampleConfig
	upgrader = configupgrade.SimpleUpgrader(func(helper configupgrade.Helper) {
		// Copy user's cheogram value into base so config save preserves it (framework adds "network" prefix).
		helper.Copy(configupgrade.Bool, "cheogram")
		upgradeCardDAVConfig(helper)
		if e != nil {
			e.upgradeConfig(helper)
		}
	})
	return example, &JabberConfig{}, upgrader
}

// LoadUserLogin is called when a UserLogin is loaded (e.g. after login or on bridge start).
// It must set login.Client to a NetworkAPI. If metadata has credentials, we connect to XMPP.
func (c *JabberConnector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	meta, _ := login.Metadata.(*JabberLoginMeta)
	if meta == nil || meta.Password == "" {
		login.Client = &stubJabberClient{loginID: login.ID}
		return nil
	}
	client := &xmppClient{loginID: login.ID, meta: meta, log: login.Log, login: login, conn: c}
	login.Client = client
	if e := c.extension(); e != nil {
		e.loginLoaded(login, client)
	}
	// Start a background reconnect loop so the XMPP session is kept alive.
	// Use a cancellable context so LogoutRemote can stop the loop and prevent reconnecting after logout.
	bgCtx, cancel := context.WithCancel(login.Log.WithContext(context.Background()))
	client.mu.Lock()
	client.reconnectCancel = cancel
	client.mu.Unlock()
	go client.ReconnectLoop(bgCtx)
	return nil
}

// GetLoginFlows returns supported login flows. We support JID+password.
func (c *JabberConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{{
		ID:          "password",
		Name:        "Password",
		Description: "Log in with your Jabber ID (JID) and password",
	}}
}

// CreateLogin returns a login process for the given flow (e.g. "password").
func (c *JabberConnector) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	if flowID != "password" {
		return nil, fmt.Errorf("unknown flow %q", flowID)
	}
	return &jabberLoginProcess{conn: c, user: user}, nil
}

// GetBridgeInfoVersion returns versions for bridge info and room capabilities.
func (c *JabberConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 1
}

// jabberLoginProcess implements LoginProcess and LoginProcessUserInput for JID+password auth.
type jabberLoginProcess struct {
	conn *JabberConnector
	user *bridgev2.User
}

func (j *jabberLoginProcess) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	prefix := "!jabber"
	if j.conn.br != nil && j.conn.br.Config != nil && j.conn.br.Config.CommandPrefix != "" {
		prefix = j.conn.br.Config.CommandPrefix
	}
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       "fi.mau.jabber.password",
		Instructions: fmt.Sprintf("Enter your Jabber ID (e.g. user@jabber.org) and password. You can send them in one message: `%s login password <jid> <password>` or follow the prompts.\n\nAfter you submit, the bot will connect to your XMPP server; this can take 1–2 minutes—please wait for a response.", prefix),
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{
				{
					Type:        bridgev2.LoginInputFieldTypeUsername,
					ID:          "jid",
					Name:        "Jabber ID (JID)",
					Description: "Your full JID, e.g. user@jabber.org",
				},
				{
					Type:        bridgev2.LoginInputFieldTypePassword,
					ID:          "password",
					Name:        "Password",
					Description: "Your Jabber account password",
				},
			},
		},
	}, nil
}

func (j *jabberLoginProcess) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	jidStr := strings.TrimSpace(input["jid"])
	password := input["password"]

	if jidStr == "" {
		return nil, fmt.Errorf("JID is required")
	}
	if !strings.Contains(jidStr, "@") {
		return nil, fmt.Errorf("JID must be in the form user@domain (e.g. user@jabber.org)")
	}
	if password == "" {
		return nil, fmt.Errorf("password is required")
	}

	ctx, span := telemetry.Start(ctx, "bridge.login.submit",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String("login.jid", jidStr)))
	defer span.End()

	log := j.conn.br.Log.With().Logger()
	log = *telemetry.Logger(ctx, &log)
	warnIfDebugIdentifiers(&log)
	log.Debug().Str("jid", jidStr).Msg("XMPP login: attempting connection (this may take 1–2 minutes)")

	// Verify XMPP credentials by connecting once before creating the login.
	dialCtx, cancel := context.WithTimeout(ctx, 50*time.Second)
	defer cancel()
	origin, err := parseJID(jidStr)
	if err != nil {
		telemetry.Fail(span, err)
		return nil, fmt.Errorf("invalid JID format (use user@domain)")
	}
	session, err := dialXMPP(dialCtx, origin, password, &log)
	if err != nil {
		telemetry.Fail(span, err)
		log.Debug().Str("jid", jidStr).Msg("XMPP login: connection or auth failed")
		if dialCtx.Err() != nil {
			return nil, fmt.Errorf("connection timed out (is the server reachable?): %w", err)
		}
		// Do not wrap or echo err to avoid leaking server messages or credentials into logs.
		return nil, fmt.Errorf("XMPP authentication failed")
	}
	_ = session.Close()
	log.Debug().Str("jid", jidStr).Msg("XMPP login: verification succeeded, creating bridge login")

	loginID := networkid.UserLoginID(strings.ToLower(jidStr))
	data := &database.UserLogin{
		ID:         loginID,
		RemoteName: jidStr,
		Metadata:   &JabberLoginMeta{Password: password},
	}

	ul, err := j.user.NewLogin(ctx, data, nil)
	if err != nil {
		telemetry.Fail(span, err)
		return nil, fmt.Errorf("failed to create login: %w", err)
	}

	prefix := "!jabber"
	if j.conn.br != nil && j.conn.br.Config != nil && j.conn.br.Config.CommandPrefix != "" {
		prefix = j.conn.br.Config.CommandPrefix
	}
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       "fi.mau.jabber.complete",
		Instructions: fmt.Sprintf("Successfully connected to XMPP as %s. You can use `%s help` for commands (e.g. `%s pm` or `%s start-chat` with an optional login JID to open a DM from a specific account, `%s join <room-jid>` for MUC rooms).", jidStr, prefix, prefix, prefix, prefix),
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: loginID,
			UserLogin:   ul,
		},
	}, nil
}

func (j *jabberLoginProcess) Cancel() {}

// parseJID parses a JID string into a jid.JID (bare JID for dialing).
func parseJID(s string) (jid.JID, error) {
	return jid.Parse(s)
}

// saslMechanisms lists SASL mechanisms in preferred order (stronger first).
// Many servers do not advertise PLAIN; SCRAM-SHA-1/256 are widely supported.
var saslMechanisms = []sasl.Mechanism{
	sasl.ScramSha256Plus, sasl.ScramSha256,
	sasl.ScramSha1Plus, sasl.ScramSha1,
	sasl.Plain,
}

const xmppDefaultPort = "5222"

// sortIPsPreferIPv6 returns a copy of the slice with IPv6 addresses first, then IPv4.
// Used for Phase 2.2 dual-stack: we try IPv6 when the resolver returns both families.
func sortIPsPreferIPv6(ips []string) []string {
	var v6, v4 []string
	for _, ip := range ips {
		if net.ParseIP(ip).To4() == nil {
			v6 = append(v6, ip)
		} else {
			v4 = append(v4, ip)
		}
	}
	out := make([]string, 0, len(ips))
	out = append(out, v6...)
	out = append(out, v4...)
	return out
}

// dialXMPP opens an XMPP client session with the given origin and password.
// Caller must close the session when done.
// Logs DNS vs TCP vs TLS/SASL timing when log is non-nil so slow steps can be identified.
func dialXMPP(ctx context.Context, origin jid.JID, password string, log *zerolog.Logger) (*xmpp.Session, error) {
	t0 := time.Now()
	domain := origin.Domainpart()

	ctx, dialSpan := telemetry.StartChild(ctx, "xmpp.dial",
		trace.WithAttributes(attribute.String("server.address", domain)))
	defer dialSpan.End()

	// Step 1: resolve domain with a bounded timeout
	_, dnsSpan := telemetry.StartChild(ctx, "xmpp.dial.dns")
	resolveCtx, resolveCancel := context.WithTimeout(ctx, 15*time.Second)
	ips, err := net.DefaultResolver.LookupHost(resolveCtx, domain)
	resolveCancel()
	if err != nil {
		telemetry.Fail(dnsSpan, err)
		dnsSpan.End()
		telemetry.Fail(dialSpan, err)
		return nil, fmt.Errorf("DNS lookup %s: %w", domain, err)
	}
	dnsSpan.SetAttributes(attribute.Int("ip_count", len(ips)))
	dnsSpan.End()
	if len(ips) == 0 {
		err = fmt.Errorf("DNS lookup %s: no addresses", domain)
		telemetry.Fail(dialSpan, err)
		return nil, err
	}
	// Phase 2.2: dual-stack — try IPv6 first when both families are present
	ips = sortIPsPreferIPv6(ips)
	if log != nil {
		log.Debug().
			Dur("dns_ms", time.Since(t0)).
			Str("domain", domain).
			Strs("ips", ips).
			Msg("XMPP login: DNS resolved (dual-stack, IPv6 preferred)")
	}

	// Step 2: TCP connect (try each resolved IP in order until one works)
	t1 := time.Now()
	dialer := net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	var conn net.Conn
	_, tcpSpan := telemetry.StartChild(ctx, "xmpp.dial.tcp")
	attempts := 0
	for _, ip := range ips {
		attempts++
		addr := net.JoinHostPort(ip, xmppDefaultPort)
		conn, err = dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			if log != nil {
				log.Debug().
					Dur("tcp_ms", time.Since(t1)).
					Str("addr", addr).
					Msg("XMPP login: TCP connected, negotiating TLS and SASL")
			}
			break
		}
	}
	tcpSpan.SetAttributes(attribute.Int("tcp.attempts", attempts))
	if conn == nil {
		telemetry.Fail(tcpSpan, err)
		tcpSpan.End()
		telemetry.Fail(dialSpan, err)
		return nil, fmt.Errorf("TCP connect %s (tried %d IPs): %w", domain, len(ips), err)
	}
	tcpSpan.End()

	// Step 3: XMPP stream (TLS + SASL + bind)
	features := []xmpp.StreamFeature{
		xmpp.StartTLS(nil),
		xmpp.SASL("", password, saslMechanisms...),
		xmpp.BindResource(),
	}
	streamCtx, streamSpan := telemetry.StartChild(ctx, "xmpp.dial.stream")
	sess, err := xmpp.NewClientSession(streamCtx, origin, conn, features...)
	if err != nil {
		telemetry.Fail(streamSpan, err)
		streamSpan.End()
		telemetry.Fail(dialSpan, err)
		conn.Close()
		return nil, err
	}
	streamSpan.End()
	if log != nil {
		log.Debug().
			Dur("stream_ms", time.Since(t1)).
			Dur("total_ms", time.Since(t0)).
			Msg("XMPP login: session established")
	}
	return sess, nil
}

// SetMUCPreferredNickForRoom sets the preferred MUC nickname for a room and persists it in portal metadata.
// It is used by the !jabber muc-nick command.
func SetMUCPreferredNickForRoom(ctx context.Context, br *bridgev2.Bridge, loginID networkid.UserLoginID, roomJID, nick string) error {
	portalKey := networkid.PortalKey{ID: networkid.PortalID(roomJID), Receiver: loginID}
	portal, err := br.GetPortalByKey(ctx, portalKey)
	if err != nil {
		return err
	}
	meta, _ := portal.Metadata.(*PortalMetadata)
	if meta == nil {
		meta = &PortalMetadata{}
	}
	meta.MUCPreferredNick = nick
	portal.Metadata = meta
	return portal.Save(ctx)
}

// SetDefaultMUCNick sets the default MUC nickname for the login and persists it in user login metadata.
// It is used by the !jabber muc-nick-default command.
func SetDefaultMUCNick(ctx context.Context, br *bridgev2.Bridge, login *bridgev2.UserLogin, nick string) error {
	extraWriteMu.Lock() // the save marshals the whole metadata, including Extra written by other commands
	defer extraWriteMu.Unlock()
	meta, _ := login.Metadata.(*JabberLoginMeta)
	if meta == nil {
		meta = &JabberLoginMeta{}
	}
	meta.DefaultMUCNick = nick
	login.Metadata = meta
	return login.Save(ctx)
}
