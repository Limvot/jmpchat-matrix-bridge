package connector

import (
	"sort"
	"strings"
	"unicode"

	"github.com/williamkray/mrtc-sip-bridge/pkg/phone"
)

const (
	cheogramDomain       = "cheogram.com"
	cheogramShortCodeCtx = "ca-us.phone-context.soprani.ca"
	cheogramShortCodeMax = 6
	cheogramDefaultCC    = "1" // default country code when no leading + (North America)
)

// ResolveCheogramIdentifier converts a phone number or short code into a Cheogram JID
// when enabled is true. Returns (jid, true) if the input was resolved, otherwise ("", false).
//
// Phone numbers: digits with optional + and hyphens → +<digits>@cheogram.com. If no leading +
// is supplied, default country code +1 (North America) is prepended.
// Short codes: digit-only, length <= 6, no leading + → <digits>;phone-context=...@cheogram.com
func ResolveCheogramIdentifier(input string, enabled bool) (jid string, ok bool) {
	if !enabled {
		return "", false
	}

	// Strip spaces and hyphens, keep digits and leading +
	trimmed := strings.TrimSpace(input)
	var b strings.Builder
	for i, r := range trimmed {
		if r == '+' && i == 0 {
			b.WriteRune(r)
			continue
		}
		if r == '-' || r == ' ' || r == '\t' {
			continue
		}
		if !unicode.IsDigit(r) {
			return "", false
		}
		b.WriteRune(r)
	}
	digitsOnly := b.String()
	if digitsOnly == "" || digitsOnly == "+" {
		return "", false
	}

	hasPlus := strings.HasPrefix(digitsOnly, "+")
	digits := strings.TrimPrefix(digitsOnly, "+")
	if digits == "" {
		return "", false
	}

	// Short code: only digits, length <= max, no leading +
	if len(digits) <= cheogramShortCodeMax && !hasPlus {
		return digits + ";phone-context=" + cheogramShortCodeCtx + "@" + cheogramDomain, true
	}

	// Phone number: use +digits as given, or default +1 when no leading + supplied
	local := "+" + digits
	if !hasPlus {
		local = "+" + cheogramDefaultCC + digits
	}
	return local + "@" + cheogramDomain, true
}

// ParseCheogramGroupMMSSenderFromBody extracts the sender JID from a Cheogram group MMS body prefix.
// Cheogram embeds the sender in the body as "<xmpp:+1234567890@cheogram.com> " before the actual message.
// Returns (senderJID, bodyWithPrefixRemoved, true) if the prefix is present and valid; otherwise ("", body, false).
func ParseCheogramGroupMMSSenderFromBody(body string) (senderJID string, trimmedBody string, ok bool) {
	const prefix = "<xmpp:"
	if !strings.HasPrefix(body, prefix) {
		return "", body, false
	}
	end := strings.Index(body, ">")
	if end < len(prefix) {
		return "", body, false
	}
	senderJID = body[len(prefix):end]
	if senderJID == "" || !strings.Contains(senderJID, "@") {
		return "", body, false
	}
	trimmedBody = strings.TrimSpace(body[end+1:])
	return senderJID, trimmedBody, true
}

// IsCheogramDMJID reports whether the JID is a Cheogram 1:1 (SMS/MMS) address.
// Such JIDs must be treated as DMs, not MUCs: we must send type "chat" (or "normal"), not "groupchat",
// and must not send MUC join presence. Returns true for +digits@cheogram.com and short-code style JIDs.
// Also returns true for group MMS JIDs (comma-separated locals, e.g. +1a,+1b,+1c@cheogram.com).
func IsCheogramDMJID(jidStr string) bool {
	if !strings.HasSuffix(strings.ToLower(jidStr), "@"+cheogramDomain) {
		return false
	}
	at := strings.LastIndex(jidStr, "@")
	local := jidStr[:at]
	return strings.HasPrefix(local, "+") || strings.Contains(local, ";phone-context=") || strings.Contains(local, ",")
}

// ResolveCheogramGroupMMSSenderForMAM resolves the sender JID and body for a MAM item in a Cheogram group MMS.
// from is the MAM item's From (may include /resource), body is the raw body.
// Strips <xmpp:JID> prefix from body when present. Returns (senderJID, bodyOut).
// For non-group MMS, returns (fromBare, body) unchanged.
func ResolveCheogramGroupMMSSenderForMAM(from, body string) (senderJID string, bodyOut string) {
	fromBare := from
	if idx := strings.Index(from, "/"); idx >= 0 {
		fromBare = from[:idx]
	}
	senderJID = fromBare
	bodyOut = body
	individuals, ok := ParseCheogramGroupMMSJID(fromBare)
	if !ok || len(individuals) == 0 {
		return senderJID, bodyOut
	}
	if idx := strings.Index(from, "/"); idx >= 0 {
		afterSlash := from[idx+1:]
		if afterSlash != "" {
			if strings.Contains(afterSlash, "@") {
				senderJID = afterSlash
			} else if at := strings.LastIndex(fromBare, "@"); at > 0 {
				senderJID = afterSlash + "@" + fromBare[at+1:]
			} else {
				senderJID = individuals[0]
			}
		}
	}
	if senderJID == fromBare {
		if parsedSender, bodyWithoutPrefix, okBody := ParseCheogramGroupMMSSenderFromBody(body); okBody && parsedSender != "" {
			senderJID = parsedSender
			bodyOut = bodyWithoutPrefix
		} else {
			senderJID = individuals[0]
		}
	}
	return senderJID, bodyOut
}

// ParseCheogramGroupMMSJID parses a Cheogram group MMS JID (e.g. +16576662298,+17145551234@cheogram.com)
// into individual bare JIDs. Returns (nil, false) if jidStr is not @cheogram.com or the local part has no comma.
// Each segment is normalized to a single JID (+digits@cheogram.com or short-code form); invalid segments are skipped.
func ParseCheogramGroupMMSJID(jidStr string) (individualJIDs []string, ok bool) {
	if !strings.HasSuffix(strings.ToLower(jidStr), "@"+cheogramDomain) {
		return nil, false
	}
	at := strings.LastIndex(jidStr, "@")
	local := jidStr[:at]
	if local == "" || !strings.Contains(local, ",") {
		return nil, false
	}
	var out []string
	seen := make(map[string]bool)
	for _, seg := range strings.Split(local, ",") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		// Normalize to a single JID: +digits or short-code form. Build +digits@cheogram.com or short@cheogram.com.
		var b strings.Builder
		for i, r := range seg {
			if r == '+' && i == 0 {
				b.WriteRune(r)
				continue
			}
			if r == '-' || r == ' ' || r == '\t' {
				continue
			}
			if !unicode.IsDigit(r) {
				b.Reset()
				break
			}
			b.WriteRune(r)
		}
		digitsOnly := b.String()
		if digitsOnly == "" || digitsOnly == "+" {
			continue
		}
		hasPlus := strings.HasPrefix(digitsOnly, "+")
		digits := strings.TrimPrefix(digitsOnly, "+")
		if digits == "" {
			continue
		}
		var single string
		if len(digits) <= cheogramShortCodeMax && !hasPlus {
			single = digits + ";phone-context=" + cheogramShortCodeCtx + "@" + cheogramDomain
		} else {
			if !hasPlus {
				digits = cheogramDefaultCC + digits
			}
			single = "+" + digits + "@" + cheogramDomain
		}
		if !seen[single] {
			seen[single] = true
			out = append(out, single)
		}
	}
	if len(out) < 2 {
		return nil, false
	}
	return out, true
}

// ResolveCheogramMultiIdentifier converts a comma-separated list of phone numbers into a single
// Cheogram group MMS JID when enabled is true. Each segment is normalized (E.164-style +digits),
// deduplicated, and sorted before joining. Example: "949-555-7266, 7148385454, 19499971909" →
// "+17148385454,+19495557266,+19499971909@cheogram.com". Returns ("", false) if disabled or any segment fails to resolve.
func ResolveCheogramMultiIdentifier(input string, enabled bool) (jid string, ok bool) {
	if !enabled {
		return "", false
	}
	parts := strings.Split(input, ",")
	var jids []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		resolved, ok := ResolveCheogramIdentifier(p, true)
		if !ok {
			return "", false
		}
		// Resolved is full JID; we need local part only for sorting/joining.
		at := strings.LastIndex(resolved, "@")
		if at <= 0 {
			return "", false
		}
		jids = append(jids, resolved[:at])
	}
	if len(jids) < 2 {
		return "", false
	}
	// Sort for stable, canonical JID (e.g. +17148385454,+19495557266,+19499971909).
	sort.Strings(jids)
	return strings.Join(jids, ",") + "@" + cheogramDomain, true
}

// cheogramE164 returns the E.164 of a Cheogram 1:1 JID ("+15551234567@cheogram.com", domain compared
// case-insensitively, resource ignored); ok=false for group MMS (comma), short codes (;phone-context),
// other domains and anything phone.IsE164 rejects.
func cheogramE164(jidStr string) (string, bool) {
	if i := strings.IndexByte(jidStr, '/'); i >= 0 {
		jidStr = jidStr[:i]
	}
	at := strings.LastIndexByte(jidStr, '@')
	if at <= 0 || !strings.EqualFold(jidStr[at+1:], cheogramDomain) {
		return "", false
	}
	local := jidStr[:at]
	if strings.ContainsAny(local, ",;") || !phone.IsE164(local) {
		return "", false
	}
	return local, true
}

// cheogramJID returns the Cheogram 1:1 JID of an E.164 number.
func cheogramJID(e164 string) string { return e164 + "@" + cheogramDomain }
