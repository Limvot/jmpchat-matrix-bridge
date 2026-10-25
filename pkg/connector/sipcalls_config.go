//go:build sipcalls

package connector

import (
	"errors"
	"strings"

	"go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix"

	"github.com/williamkray/mrtc-sip-bridge/pkg/hostkit"
)

// CallsConfig is the `network.calls` section: the runtime switch plus exactly the standalone
// mrtc-sip-bridge `network:` keys (same names, defaults and validation).
type CallsConfig struct {
	Enabled bool `yaml:"enabled"`
	// Transport selects the call leg: "" or "sip" (default, the JMP SIP account) or "jingle"
	// (native XMPP calling on the login's own session; no SIP credentials needed, ADR-014).
	Transport string `yaml:"transport"`
	// ICEServers are STUN/TURN URLs for the jingle transport's WebRTC engine (ignored for
	// sip). The media port range and external IP are the shared rtp_port_min/max and
	// external_ip keys above.
	ICEServers []string `yaml:"ice_servers"`
	// BotLeavesPortals (default true) makes the bridge bot and the login's own-account ghost
	// leave 1:1 number portals so Element X's outbound-call pickup detection finds the number's
	// ghost as the ring recipient (ADR-010; Element Call does not respect functional_members).
	BotLeavesPortals *bool `yaml:"bot_leaves_portals"`
	hostkit.Config   `yaml:",inline"`
}

// BotLeavesPortalsEnabled reports whether the pickup trim is active (nil = true).
func (c *CallsConfig) BotLeavesPortalsEnabled() bool {
	return c.BotLeavesPortals == nil || *c.BotLeavesPortals
}

// callsTransportJingle selects the Jingle call leg in CallsConfig.Transport.
const callsTransportJingle = "jingle"

// networkCallsSection is the part of the `network:` node the calls extension decodes.
type networkCallsSection struct {
	Calls CallsConfig `yaml:"calls"`
}

// callsConfigPrefix is the key path used in validation messages.
const callsConfigPrefix = "network.calls."

// errCallsNeedCheogram is returned by start when calls are enabled without Cheogram mapping.
var errCallsNeedCheogram = errors.New("network.calls.enabled requires network.cheogram: true")

// callsExampleConfig is appended to the jabber `network:` example: the `calls:` block with the shared
// example indented by 4 spaces (comments included).
func callsExampleConfig() string {
	var b strings.Builder
	b.WriteString("# Calls for Cheogram numbers (transport: sip = JMP SIP account, jingle = native XMPP calls).\n# Requires cheogram: true. For transport: jingle the WebRTC media path reuses rtp_port_min/max\n# and external_ip; ice_servers adds STUN/TURN.\ncalls:\n    enabled: false\n    transport: sip\n    ice_servers: []\n    # Make the bridge bot and your own-account puppet leave 1:1 number rooms (the number's ghost\n    # manages them). Needed for calls you start in Element X: with extra members in the room,\n    # Element X keeps \"ringing\" client-side and gives up after 30 s.\n    bot_leaves_portals: true\n")
	for _, l := range strings.SplitAfter(hostkit.ExampleYAML, "\n") {
		if l == "" {
			continue
		}
		b.WriteString("    ")
		b.WriteString(l)
	}
	if !strings.HasSuffix(b.String(), "\n") {
		b.WriteString("\n")
	}
	return b.String()
}

func (x *callsExt) exampleConfig() string { return callsExampleConfig() }

// upgradeConfig copies every `calls` key from the user's file into the upgrade base (keys that are not
// copied would be reset to the example value on every start).
func (x *callsExt) upgradeConfig(h configupgrade.Helper) {
	h.Copy(configupgrade.Bool, "calls", "enabled")
	h.Copy(configupgrade.Str, "calls", "transport")
	h.Copy(configupgrade.List, "calls", "ice_servers")
	h.Copy(configupgrade.Bool, "calls", "bot_leaves_portals")
	hostkit.CopyKeys(h, "calls")
}

// validateCallsTransport rejects unknown transport values (empty = sip).
func validateCallsTransport(s string) error {
	switch s {
	case "", "sip", callsTransportJingle:
		return nil
	}
	return errors.New("network.calls.transport must be 'sip' or 'jingle'")
}

// decodeCallsConfig decodes `calls` from the network node. A missing node or section means disabled.
func decodeCallsConfig(network *yaml.Node) (CallsConfig, error) {
	var sec networkCallsSection
	if network == nil || network.Kind == 0 {
		return sec.Calls, nil
	}
	if err := network.Decode(&sec); err != nil {
		return CallsConfig{}, err
	}
	return sec.Calls, nil
}

// validateCallsConfig applies the defaults and checks cfg against the bridge (only for enabled
// configs): shared validation with the network.calls. prefix, then the Cheogram requirement.
func validateCallsConfig(cfg *CallsConfig, cheogram bool) error {
	cfg.ApplyDefaults()
	if err := validateCallsTransport(cfg.Transport); err != nil {
		return err
	}
	if err := cfg.Validate(callsConfigPrefix); err != nil {
		return err
	}
	if !cheogram {
		return errCallsNeedCheogram
	}
	return nil
}

// encryptionDefault reports encryption.default of the bridge config (false when unknown).
func encryptionDefault(br *bridgev2.Bridge) bool {
	if br == nil {
		return false
	}
	mc, ok := br.Matrix.(*matrix.Connector)
	if !ok || mc == nil || mc.Config == nil {
		return false
	}
	return mc.Config.Encryption.Default
}
