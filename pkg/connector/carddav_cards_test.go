package connector

import (
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/emersion/go-vcard"
)

func decodeCard(t *testing.T, s string) vcard.Card {
	t.Helper()
	card, err := vcard.NewDecoder(strings.NewReader(strings.ReplaceAll(s, "\n", "\r\n"))).Decode()
	if err != nil {
		t.Fatalf("decode: %v\n%s", err, s)
	}
	return card
}

func TestExtractContact(t *testing.T) {
	for _, tc := range []struct {
		name  string
		vcf   string
		cc    string
		want  *cardContact
		skips map[string]int
	}{
		{
			name: "v3 TYPE tokens",
			vcf: `BEGIN:VCARD
VERSION:3.0
FN:John Doe
TEL;TYPE=CELL,VOICE:(555) 123-4567
TEL;TYPE=WORK:+1 555 765 4321
END:VCARD`,
			want: &cardContact{Name: "John Doe", Numbers: []cardNumber{{"+15551234567", "mobile"}, {"+15557654321", "work"}}},
		},
		{
			name: "pref ordering and type=pref;type=CELL",
			vcf: `BEGIN:VCARD
VERSION:3.0
FN:Pat
TEL;TYPE=HOME:555 000 1111
TEL;TYPE=pref;type=CELL:555 000 2222
END:VCARD`,
			want: &cardContact{Name: "Pat", Numbers: []cardNumber{{"+15550002222", "mobile"}, {"+15550001111", "home"}}},
		},
		{
			name: "v4 PREF and tel: URI",
			vcf: `BEGIN:VCARD
VERSION:4.0
FN:Quinn
TEL;VALUE=uri;PREF=2;TYPE="work":tel:+1-555-000-3333
TEL;VALUE=uri;PREF=1;TYPE="cell":tel:+44-20-7946-0958
END:VCARD`,
			want: &cardContact{Name: "Quinn", Numbers: []cardNumber{{"+442079460958", "mobile"}, {"+15550003333", "work"}}},
		},
		{
			name: "apple group standard label",
			vcf: `BEGIN:VCARD
VERSION:3.0
FN:Ann
item1.TEL;type=pref:555 111 2222
item1.X-ABLabel:_$!<Mobile>!$_
item2.TEL:555 111 3333
item2.X-ABLabel:_$!<Main>!$_
END:VCARD`,
			want: &cardContact{Name: "Ann", Numbers: []cardNumber{{"+15551112222", "mobile"}, {"+15551113333", "main"}}},
		},
		{
			name: "apple custom label is kept sanitized",
			vcf: `BEGIN:VCARD
VERSION:3.0
FN:Bob
item1.TEL:555 111 2222
item1.X-ABLabel:Office (HQ)
item2.TEL:555 111 3333
item2.X-ABLabel:` + "‮" + `Mom
END:VCARD`,
			want: &cardContact{Name: "Bob", Numbers: []cardNumber{{"+15551112222", "Office HQ"}, {"+15551113333", "Mom"}}},
		},
		{
			name: "custom label is length capped",
			vcf: `BEGIN:VCARD
VERSION:3.0
FN:Cy
item1.TEL:555 111 2222
item1.X-ABLabel:This is a very very long custom label
END:VCARD`,
			want: &cardContact{Name: "Cy", Numbers: []cardNumber{{"+15551112222", "This is a very very long"}}},
		},
		{
			name: "fax and pager skipped unless cell",
			vcf: `BEGIN:VCARD
VERSION:3.0
FN:Dee
TEL;TYPE=FAX,WORK:555 111 2222
TEL;TYPE=PAGER:555 111 3333
TEL;TYPE=CELL,FAX:555 111 4444
item1.TEL:555 111 5555
item1.X-ABLabel:_$!<WorkFAX>!$_
END:VCARD`,
			want:  &cardContact{Name: "Dee", Numbers: []cardNumber{{"+15551114444", "mobile"}}},
			skips: map[string]int{reasonFax: 3},
		},
		{
			name: "duplicate number in a card collapses, first label wins",
			vcf: `BEGIN:VCARD
VERSION:3.0
FN:Eve
TEL;TYPE=CELL:555 111 2222
TEL;TYPE=WORK:+1 (555) 111-2222
END:VCARD`,
			want: &cardContact{Name: "Eve", Numbers: []cardNumber{{"+15551112222", "mobile"}}},
		},
		{
			name: "name from N",
			vcf: `BEGIN:VCARD
VERSION:3.0
N:Family;Given;Middle;Dr.;Jr.
TEL:555 111 2222
END:VCARD`,
			want: &cardContact{Name: "Given Family", Numbers: []cardNumber{{"+15551112222", "other"}}},
		},
		{
			name: "name from NICKNAME",
			vcf:  "BEGIN:VCARD\nVERSION:3.0\nNICKNAME:Sparky\nTEL:555 111 2222\nEND:VCARD",
			want: &cardContact{Name: "Sparky", Numbers: []cardNumber{{"+15551112222", "other"}}},
		},
		{
			name: "name from ORG first component",
			vcf:  "BEGIN:VCARD\nVERSION:3.0\nORG:Acme Plumbing;Dept\nTEL:555 111 2222\nEND:VCARD",
			want: &cardContact{Name: "Acme Plumbing", Numbers: []cardNumber{{"+15551112222", "other"}}},
		},
		{
			name:  "no name at all",
			vcf:   "BEGIN:VCARD\nVERSION:3.0\nTEL:555 111 2222\nEND:VCARD",
			skips: map[string]int{reasonNoName: 1},
		},
		{
			name:  "no tel",
			vcf:   "BEGIN:VCARD\nVERSION:3.0\nFN:Nobody\nEND:VCARD",
			skips: map[string]int{reasonNoNumber: 1},
		},
		{
			name:  "kind group",
			vcf:   "BEGIN:VCARD\nVERSION:4.0\nKIND:group\nFN:Team\nTEL:555 111 2222\nEND:VCARD",
			skips: map[string]int{reasonNoNumber: 1},
		},
		{
			name: "per-number skips",
			vcf: `BEGIN:VCARD
VERSION:3.0
FN:Mix
TEL:611
TEL:555 111 2222 x9
TEL:abc
TEL:555 111 3333
END:VCARD`,
			want:  &cardContact{Name: "Mix", Numbers: []cardNumber{{"+15551113333", "other"}}},
			skips: map[string]int{reasonShortCode: 1, reasonExtension: 1, reasonInvalid: 1},
		},
		{
			name: "folded line",
			vcf:  "BEGIN:VCARD\nVERSION:3.0\nFN:Long\n  Name\nTEL:555 111\n  2222\nEND:VCARD",
			want: &cardContact{Name: "Long Name", Numbers: []cardNumber{{"+15551112222", "other"}}},
		},
		{
			// Pinned go-vcard behaviour: v2.1 bare parameters become an empty value with the number as
			// a parameter value. A single phone-looking parameter value is salvaged (the key is a hint).
			name: "vCard 2.1 bare CELL param is salvaged as mobile",
			vcf:  "BEGIN:VCARD\nVERSION:2.1\nFN:Old\nTEL;CELL:+15551234567\nEND:VCARD",
			want: &cardContact{Name: "Old", Numbers: []cardNumber{{"+15551234567", "mobile"}}},
		},
		{
			name: "vCard 2.1 CELL;PREF salvages the number without a label",
			vcf:  "BEGIN:VCARD\nVERSION:2.1\nFN:Old\nTEL;CELL;PREF:+15551234567\nEND:VCARD",
			want: &cardContact{Name: "Old", Numbers: []cardNumber{{"+15551234567", "other"}}},
		},
		{
			name:  "vCard 2.1 with no phone-like param is no_number",
			vcf:   "BEGIN:VCARD\nVERSION:2.1\nFN:Old\nTEL;CELL:\nEND:VCARD",
			skips: map[string]int{reasonNoNumber: 1},
		},
		{
			name: "default country code 44",
			vcf:  "BEGIN:VCARD\nVERSION:3.0\nFN:Uk\nTEL:020 7946 0958\nEND:VCARD",
			cc:   "44",
			want: &cardContact{Name: "Uk", Numbers: []cardNumber{{"+442079460958", "other"}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc := tc.cc
			if cc == "" {
				cc = "1"
			}
			got, skips := extractContact(decodeCard(t, tc.vcf), "", cc)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("contact = %+v, want %+v", got, tc.want)
			}
			if len(skips) == 0 {
				skips = nil
			}
			if !reflect.DeepEqual(skips, tc.skips) {
				t.Errorf("skips = %v, want %v", skips, tc.skips)
			}
		})
	}
}

func TestExtractContactNameCleaning(t *testing.T) {
	long := strings.Repeat("x", 100)
	c, _ := extractContact(decodeCard(t, "BEGIN:VCARD\nVERSION:3.0\nFN:‮Jo​  \tDoe\u0007\nTEL:555 111 2222\nEND:VCARD"), "", "1")
	if c == nil || c.Name != "Jo Doe" {
		t.Fatalf("name = %+v", c)
	}
	c, _ = extractContact(decodeCard(t, "BEGIN:VCARD\nVERSION:3.0\nFN:"+long+"\nTEL:555 111 2222\nEND:VCARD"), "", "1")
	if c == nil || c.Name != long { // extraction keeps the full name; the planner truncates with the suffix
		t.Fatalf("name = %+v", c)
	}
	if utf8.RuneCountInString(truncateRunes(long, maxContactNameRunes)) != maxContactNameRunes {
		t.Fatal("truncateRunes")
	}
}

func TestExtractContactUIDAsID(t *testing.T) {
	c, _ := extractContact(decodeCard(t, "BEGIN:VCARD\nVERSION:3.0\nUID:abc\nFN:U\nTEL:555 111 2222\nEND:VCARD"), "", "1")
	if c == nil || c.ID != "abc" {
		t.Fatalf("%+v", c)
	}
	c, _ = extractContact(decodeCard(t, "BEGIN:VCARD\nVERSION:3.0\nUID:abc\nFN:U\nTEL:555 111 2222\nEND:VCARD"), "/href/1.vcf", "1")
	if c == nil || c.ID != "/href/1.vcf" {
		t.Fatalf("%+v", c)
	}
}
