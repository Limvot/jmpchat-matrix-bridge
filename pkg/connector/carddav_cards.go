package connector

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/emersion/go-vcard"
)

// maxContactLabelRunes bounds a custom phone label shown in a contact name.
const maxContactLabelRunes = 24

// cardNumber is one importable number of a card.
type cardNumber struct {
	E164  string
	Label string // "mobile", "work", "home", "main", "other" or a sanitized custom label
}

// cardContact is what the planner needs from one vCard: a cleaned display name and the distinct
// importable numbers in preference order. ID orders contacts deterministically (UID or href).
type cardContact struct {
	ID      string
	Name    string
	Numbers []cardNumber
}

// cleanCardText removes control and Unicode format characters (RTL override etc.), collapses whitespace
// and trims.
func cleanCardText(s string) string {
	s = strings.Map(func(r rune) rune {
		switch {
		case unicode.IsSpace(r) || unicode.Is(unicode.Zs, r):
			return ' '
		case unicode.IsControl(r) || unicode.Is(unicode.Cf, r):
			return -1
		}
		return r
	}, s)
	return strings.Join(strings.Fields(s), " ")
}

// truncateRunes cuts s to at most n runes and trims trailing space.
func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return strings.TrimSpace(string([]rune(s)[:n]))
}

// cardDisplayName picks the first non-empty of FN, N ("Given Family"), NICKNAME, first ORG component.
func cardDisplayName(card vcard.Card) string {
	if n := cleanCardText(card.PreferredValue(vcard.FieldFormattedName)); n != "" {
		return n
	}
	if n := card.Name(); n != nil {
		if s := cleanCardText(n.GivenName + " " + n.FamilyName); s != "" {
			return s
		}
	}
	if n := cleanCardText(card.PreferredValue(vcard.FieldNickname)); n != "" {
		return n
	}
	if org := card.PreferredValue(vcard.FieldOrganization); org != "" {
		return cleanCardText(strings.SplitN(org, ";", 2)[0])
	}
	return ""
}

// telLabel classifies a TEL field. fax reports fax/pager numbers that are not also cell numbers.
func telLabel(card vcard.Card, f *vcard.Field, salvageKey string) (label string, fax bool) {
	var cell, work, home, faxy bool
	mark := func(t string) {
		switch strings.ToLower(t) {
		case "cell", "mobile", "iphone":
			cell = true
		case "work":
			work = true
		case "home":
			home = true
		case "fax", "pager":
			faxy = true
		}
	}
	for _, t := range f.Params.Types() {
		for _, p := range strings.Split(t, ",") {
			mark(p)
		}
	}
	if salvageKey != "" {
		mark(salvageKey)
	}
	if cell {
		return "mobile", false
	}
	custom := ""
	if f.Group != "" {
		for _, l := range card["X-ABLABEL"] {
			if strings.EqualFold(l.Group, f.Group) {
				std, isFax, c := abLabel(l.Value)
				switch {
				case isFax:
					faxy = true
				case std != "":
					if std == "mobile" {
						return "mobile", false
					}
					if !work && !home {
						custom = std
					}
				case c != "":
					custom = c
				}
				break
			}
		}
	}
	if faxy {
		return "", true
	}
	switch {
	case work:
		return "work", false
	case home:
		return "home", false
	case custom != "":
		return custom, false
	}
	return "other", false
}

// abLabel interprets an Apple X-ABLabel value: a standard label ("_$!<Mobile>!$_"), a fax kind, or a
// sanitized custom text.
func abLabel(v string) (std string, fax bool, custom string) {
	v = cleanCardText(v)
	if strings.HasPrefix(v, "_$!<") && strings.HasSuffix(v, ">!$_") {
		v = v[4 : len(v)-4]
	}
	switch strings.ToLower(strings.ReplaceAll(v, " ", "")) {
	case "mobile", "iphone", "cell":
		return "mobile", false, ""
	case "work":
		return "work", false, ""
	case "home":
		return "home", false, ""
	case "main":
		return "main", false, ""
	case "other":
		return "other", false, ""
	case "homefax", "workfax", "otherfax", "fax", "pager":
		return "", true, ""
	}
	v = strings.NewReplacer("(", "", ")", "").Replace(v)
	return "", false, truncateRunes(cleanCardText(v), maxContactLabelRunes)
}

// looksLikePhone reports whether s consists only of phone characters with at least 3 digits.
func looksLikePhone(s string) bool {
	digits := 0
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			digits++
		case strings.ContainsRune("+-.() \t", r):
		default:
			return false
		}
	}
	return digits >= 3
}

// telValue returns the number text of a TEL field. vCard 2.1 bare parameters (`TEL;CELL:+1555...`)
// are mis-parsed by go-vcard as an empty value with the number as a parameter value; when exactly one
// parameter value looks like a phone number it is salvaged (salvageKey is that parameter's name, used as
// a type hint).
func telValue(f *vcard.Field) (value, salvageKey string) {
	if strings.TrimSpace(f.Value) != "" {
		return f.Value, ""
	}
	var found, key string
	n := 0
	for k, vs := range f.Params {
		for _, v := range vs {
			if looksLikePhone(v) {
				found, key = v, k
				n++
			}
		}
	}
	if n == 1 {
		return found, key
	}
	return "", ""
}

// extractContact turns a decoded card into a cardContact. skips counts what was left out, by reason
// (per number, except no_name and no_number which count per card). The contact is nil when nothing is
// importable. defaultCC is the country code for numbers without +.
func extractContact(card vcard.Card, id, defaultCC string) (c *cardContact, skips map[string]int) {
	skips = map[string]int{}
	if card.Kind() != vcard.KindIndividual {
		skips[reasonNoNumber]++
		return nil, skips
	}
	tels := card[vcard.FieldTelephone]
	if len(tels) == 0 {
		skips[reasonNoNumber]++
		return nil, skips
	}
	// Preferred numbers first (stable otherwise).
	ordered := orderByPref(tels)
	var nums []cardNumber
	seen := map[string]struct{}{}
	for _, f := range ordered {
		val, key := telValue(f)
		if val == "" {
			skips[reasonNoNumber]++
			continue
		}
		label, fax := telLabel(card, f, key)
		if fax {
			skips[reasonFax]++
			continue
		}
		e164, reason := normalizeCardPhone(val, defaultCC)
		if reason != "" {
			skips[reason]++
			continue
		}
		if _, dup := seen[e164]; dup {
			continue // the same number twice in one card collapses; the first label wins
		}
		seen[e164] = struct{}{}
		nums = append(nums, cardNumber{E164: e164, Label: label})
	}
	if len(nums) == 0 {
		return nil, skips
	}
	name := cardDisplayName(card)
	if name == "" {
		skips[reasonNoName]++
		return nil, skips
	}
	if uid := strings.TrimSpace(card.Value(vcard.FieldUID)); uid != "" && id == "" {
		id = uid
	}
	return &cardContact{ID: id, Name: name, Numbers: nums}, skips
}

// orderByPref returns fields with PREF=n (or TYPE=pref, as n=1) first, ascending, else in card order.
func orderByPref(fields []*vcard.Field) []*vcard.Field {
	rank := func(f *vcard.Field) int {
		if p := f.Params.Get(vcard.ParamPreferred); p != "" {
			n := 0
			for _, r := range p {
				if r < '0' || r > '9' {
					return 100
				}
				n = n*10 + int(r-'0')
				if n > 100 {
					return 100
				}
			}
			return n
		}
		if f.Params.HasType("pref") {
			return 1
		}
		return 100
	}
	out := make([]*vcard.Field, len(fields))
	copy(out, fields)
	// insertion sort keeps it stable
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && rank(out[j]) < rank(out[j-1]); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
