package connector

import (
	"strings"
	"time"

	"go.mau.fi/util/configupgrade"
	"maunium.net/go/mautrix/bridgev2"
)

// CardDAVConfig is the `network.carddav` section. The limits are constants, not config.
type CardDAVConfig struct {
	// AllowInsecureHTTP permits http:// CardDAV URLs (local testing only: the password is sent in clear).
	AllowInsecureHTTP bool `yaml:"allow_insecure_http"`
	// AllowPrivateHosts permits loopback, private (RFC1918/ULA), link-local and CGNAT targets.
	AllowPrivateHosts bool `yaml:"allow_private_hosts"`
	// DefaultCountryCode is used for contact numbers written without a leading + (default "1").
	DefaultCountryCode string `yaml:"default_country_code"`
}

const (
	cardDAVMaxContacts        = 5000 // abort the sync (nothing is added) above this many cards
	cardDAVMaxAddsPerRun      = 500  // roster entries added by one `carddav-sync apply`
	cardDAVAddsPerSecond      = 5    // roster add rate
	cardDAVMaxResponseBytes   = 32 << 20
	cardDAVRequestTimeout     = 30 * time.Second  // discovery requests
	cardDAVReportTimeout      = 120 * time.Second // the address-book REPORT
	cardDAVDefaultCountryCode = "1"
)

// cardDAVExampleConfig is appended to the jabber `network:` example.
const cardDAVExampleConfig = `# CardDAV contact import (adds phone numbers from a CardDAV address book to your Jabber roster as
# Cheogram contacts). Requires cheogram: true. Users set it up with the carddav-login command.
carddav:
    # Allow http:// CardDAV URLs (local testing only; sends the password unencrypted).
    allow_insecure_http: false
    # Allow CardDAV servers on loopback/private/link-local/CGNAT addresses (home-lab servers).
    allow_private_hosts: false
    # Country code for contact numbers written without a leading + (digits only).
    default_country_code: "1"
`

// upgradeCardDAVConfig copies every carddav key from the user's file into the upgrade base (keys that
// are not copied would be reset to the example value on every start).
func upgradeCardDAVConfig(h configupgrade.Helper) {
	h.Copy(configupgrade.Bool, "carddav", "allow_insecure_http")
	h.Copy(configupgrade.Bool, "carddav", "allow_private_hosts")
	h.Copy(configupgrade.Str, "carddav", "default_country_code")
}

// cc returns the configured default country code ("1" when unset or not all digits).
func (c CardDAVConfig) cc() string {
	cc := strings.TrimPrefix(strings.TrimSpace(c.DefaultCountryCode), "+")
	if cc == "" || strings.Trim(cc, "0123456789") != "" {
		return cardDAVDefaultCountryCode
	}
	return cc
}

// cardDAVConfigOf returns the carddav config of the running bridge (zero value when unavailable).
func cardDAVConfigOf(br *bridgev2.Bridge) CardDAVConfig {
	if cfg := getJabberConfigFromBridge(br); cfg != nil {
		return cfg.CardDAV
	}
	return CardDAVConfig{}
}
