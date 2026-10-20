package connector

import "testing"

func TestCheogramEnvFallback(t *testing.T) {
	cases := []struct {
		name, newV, legacy string
		want               bool
	}{
		{"neither set", "", "", false},
		{"legacy 1", "", "1", true},
		{"new 1", "1", "", true},
		{"new 0 beats legacy 1", "0", "1", false},
		{"new 1 beats legacy 0", "1", "0", true},
		{"new garbage falls through to legacy", "garbage", "1", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("JMPCHAT_BRIDGE_CHEOGRAM", tc.newV)
			t.Setenv("MAUTRIX_JABBER_CHEOGRAM", tc.legacy)
			if got := CheogramEnabled(nil); got != tc.want {
				t.Fatalf("CheogramEnabled(nil) = %v, want %v", got, tc.want)
			}
		})
	}
}
