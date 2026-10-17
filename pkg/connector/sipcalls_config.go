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
	Enabled        bool `yaml:"enabled"`
	hostkit.Config `yaml:",inline"`
}

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
	b.WriteString("# SIP calls for Cheogram numbers. Requires cheogram: true.\ncalls:\n    enabled: false\n")
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
	hostkit.CopyKeys(h, "calls")
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
