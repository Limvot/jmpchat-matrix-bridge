package connector

import "testing"

func TestNormalizeCardPhone(t *testing.T) {
	for _, tc := range []struct {
		in, cc, want, reason string
	}{
		{"(555) 123-4567", "1", "+15551234567", ""},
		{"555-123-4567", "1", "+15551234567", ""},
		{"1 555 123 4567", "1", "+15551234567", ""},
		{"+1 (555) 123-4567", "1", "+15551234567", ""},
		{"tel:+1-555-123-4567", "1", "+15551234567", ""},
		{"+44 20 7946 0958", "1", "+442079460958", ""},
		{"0044 20 7946 0958", "1", "+442079460958", ""},
		{"011 44 20 7946 0958", "1", "+442079460958", ""},
		{"020 7946 0958", "44", "+442079460958", ""},
		{"‪+1 555 123‑4567‬", "1", "+15551234567", ""}, // bidi marks, NBSP, U+2011
		{"+1–555–123–4567", "1", "+15551234567", ""},
		{"611", "1", "", reasonShortCode},
		{"12345", "1", "", reasonShortCode},
		{"+1 555 123 4567;ext=12", "1", "", reasonExtension},
		{"tel:+15551234567;postd=12", "1", "", reasonExtension},
		{"555 123 4567 x12", "1", "", reasonExtension},
		{"555 123 4567 ext. 9", "1", "", reasonExtension},
		{"555-123-4567,,1", "1", "", reasonExtension},
		{"555*123#", "1", "", reasonExtension},
		{"+1555123p4567", "1", "", reasonExtension},
		{"tel:711;phone-context=example.com", "1", "", reasonInvalid},
		{"1-800-FLOWERS", "1", "", reasonInvalid},
		{"garbage", "1", "", reasonInvalid},
		{"", "1", "", reasonInvalid},
		{"+123456789012345678", "1", "", reasonInvalid}, // too long
		{"+0123456789", "1", "", reasonInvalid},
		{"5551234", "1", "", reasonInvalid}, // 7 digits, not 10 or 11
	} {
		got, reason := normalizeCardPhone(tc.in, tc.cc)
		if got != tc.want || reason != tc.reason {
			t.Errorf("normalizeCardPhone(%q, %q) = %q, %q; want %q, %q", tc.in, tc.cc, got, reason, tc.want, tc.reason)
		}
	}
}
