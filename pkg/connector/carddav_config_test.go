package connector

import (
	"testing"

	"go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"
)

func TestCardDAVConfigUpgradePreservesUserValues(t *testing.T) {
	c := &JabberConnector{}
	example, _, upgrader := c.GetConfig()
	user := "cheogram: true\ncarddav:\n    allow_insecure_http: true\n    allow_private_hosts: true\n    default_country_code: \"44\"\n"
	out, err := configupgrade.DoBytes([]byte(user), &configupgrade.StructUpgrader{
		SimpleUpgrader: func(h configupgrade.Helper) { upgrader.DoUpgrade(h) },
		Base:           example,
	})
	if err != nil {
		t.Fatal(err)
	}
	var cfg JabberConfig
	if err = yaml.Unmarshal(out, &cfg); err != nil {
		t.Fatal(err)
	}
	want := CardDAVConfig{AllowInsecureHTTP: true, AllowPrivateHosts: true, DefaultCountryCode: "44"}
	if !cfg.Cheogram || cfg.CardDAV != want {
		t.Fatalf("upgrade lost values: %+v\n%s", cfg, out)
	}
}

func TestCardDAVConfigExampleDefaults(t *testing.T) {
	c := &JabberConnector{}
	example, _, _ := c.GetConfig()
	var cfg JabberConfig
	if err := yaml.Unmarshal([]byte(example), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.CardDAV.AllowInsecureHTTP || cfg.CardDAV.AllowPrivateHosts || cfg.CardDAV.cc() != "1" {
		t.Fatalf("example defaults: %+v", cfg.CardDAV)
	}
}

func TestCardDAVConfigCC(t *testing.T) {
	for in, want := range map[string]string{"": "1", "44": "44", "+44": "44", " 49 ": "49", "abc": "1", "4 4": "1"} {
		if got := (CardDAVConfig{DefaultCountryCode: in}).cc(); got != want {
			t.Errorf("cc(%q) = %q, want %q", in, got, want)
		}
	}
}
