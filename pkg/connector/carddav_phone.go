package connector

import (
	"errors"
	"regexp"
	"strings"
	"unicode"

	"github.com/williamkray/mrtc-sip-bridge/pkg/phone"
)

// Skip reasons counted by the CardDAV import (the closed set shown in the summary).
const (
	reasonNoNumber  = "no_number"
	reasonNoName    = "no_name"
	reasonInvalid   = "invalid"
	reasonShortCode = "short_code"
	reasonExtension = "extension"
	reasonFax       = "fax"
	reasonDuplicate = "duplicate"
	reasonBadCard   = "bad_card"
)

// cardDAVReasonOrder fixes the order reasons appear in summaries.
var cardDAVReasonOrder = []string{reasonNoNumber, reasonNoName, reasonInvalid, reasonShortCode, reasonExtension, reasonFax, reasonDuplicate, reasonBadCard}

// extensionMarker matches pause/extension syntax followed by digits ("x123", "ext. 4", "p2", "w5").
var extensionMarker = regexp.MustCompile(`(?i)(ext\.?|x|p|w)\s*\d`)

// stripPhoneFormat removes Unicode format (Cf: bidi marks), space separator (Zs: NBSP) and dash
// punctuation (Pd: U+2011, U+2013, ...) characters, which phone.Normalize does not strip.
func stripPhoneFormat(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zs, r) || unicode.Is(unicode.Pd, r) {
			return -1
		}
		return r
	}, s)
}

// normalizeCardPhone turns a vCard TEL value into E.164. It returns ("", reason) for numbers that
// cannot be a plain Cheogram number: reasonExtension, reasonShortCode or reasonInvalid.
func normalizeCardPhone(raw, defaultCC string) (e164, reason string) {
	if strings.Contains(strings.ToLower(raw), "phone-context") {
		return "", reasonInvalid
	}
	s := stripPhoneFormat(strings.TrimSpace(raw))
	if len(s) >= 4 && strings.EqualFold(s[:4], "tel:") {
		s = s[4:]
	}
	if strings.ContainsAny(s, ",;#*") || extensionMarker.MatchString(s) {
		return "", reasonExtension
	}
	e164, err := phone.Normalize(s, defaultCC)
	switch {
	case err == nil:
		return e164, ""
	case errors.Is(err, phone.ErrShortCode):
		return "", reasonShortCode
	default:
		return "", reasonInvalid
	}
}
