package connector

import (
	"encoding/xml"
	"strings"
)

// Message retraction (XEP-0424), moderation (XEP-0425) and the legacy `:0` forms of both.
const (
	retractNS1   = "urn:xmpp:message-retract:1"
	retractNS0   = "urn:xmpp:message-retract:0"
	moderateNS1  = "urn:xmpp:message-moderate:1"
	moderateNS0  = "urn:xmpp:message-moderate:0"
	fastenNS     = "urn:xmpp:fasten:0"
	fallbackNS   = "urn:xmpp:fallback:0"
	maxReasonLen = 500 // runes kept of a moderation/retraction reason
)

// retractInfo is a parsed retraction. Reason is user content: it may be put on the Matrix
// redaction but must never reach logs or spans (only HasReason may).
type retractInfo struct {
	Target             string // the id the retraction points at (an `id` or origin-id; a stanza-id for MUC)
	Moderated          bool   // a <moderated> child was present (XEP-0425)
	ModeratedByPresent bool
	HasReason          bool
	Reason             string // trimmed, capped at maxReasonLen runes
	Legacy             bool   // came from the `:0` apply-to form (no `:1` element was present)
	BothForms          bool   // the stanza carried both the `:1` and the `:0` form (`:1` was used)
	HasFallback        bool   // a XEP-0428 <fallback for=retract> marker was present
}

// form names the namespace variant for the debug line: "1", "0" or "1+0".
func (r retractInfo) form() string {
	switch {
	case r.BothForms:
		return "1+0"
	case r.Legacy:
		return "0"
	}
	return "1"
}

// retractScan accumulates retraction elements while a parser walks the children of a <message>. It
// only matches on resolved namespaces and only on DIRECT children of the message, so a retract
// element nested in an unrelated payload, and the XEP-0353 jingle-message <retract>, never match.
type retractScan struct {
	v1, v0      *retractInfo
	v0Confirmed bool
	cur         *retractInfo // retract/apply-to element we are inside (nil outside)
	curDepth    int
	reasonDepth int // depth of an open <reason> inside cur (0 = none)
	reasonBuf   strings.Builder
	fallback    bool
	tombstone   bool
}

// start is called for every start element. depth is the element's depth and childDepth the depth of
// direct children of the <message>.
func (s *retractScan) start(se xml.StartElement, depth, childDepth int) {
	if depth == childDepth {
		switch {
		case se.Name.Space == retractNS1 && se.Name.Local == "retract":
			if s.v1 == nil {
				s.v1 = &retractInfo{Target: attrValue(se.Attr, "id")}
				s.cur, s.curDepth = s.v1, depth
			}
		case se.Name.Space == fastenNS && se.Name.Local == "apply-to":
			if s.v0 == nil {
				s.v0 = &retractInfo{Target: attrValue(se.Attr, "id"), Legacy: true}
				s.cur, s.curDepth = s.v0, depth
			}
		case se.Name.Space == retractNS1 && se.Name.Local == "retracted":
			s.tombstone = true
		case se.Name.Space == fallbackNS && se.Name.Local == "fallback":
			if f := attrValue(se.Attr, "for"); f == retractNS1 || f == retractNS0 {
				s.fallback = true
			}
		}
		return
	}
	if s.cur == nil || depth != s.curDepth+1 {
		return
	}
	legacy := s.cur == s.v0
	switch {
	case se.Name.Local == "reason" && s.reasonDepth == 0 && !s.cur.HasReason:
		s.cur.HasReason = true
		s.reasonDepth = depth
		s.reasonBuf.Reset()
	case !legacy && se.Name.Space == moderateNS1 && se.Name.Local == "moderated":
		s.cur.Moderated = true
		s.cur.ModeratedByPresent = attrValue(se.Attr, "by") != ""
	case legacy && se.Name.Space == moderateNS0 && se.Name.Local == "moderated":
		s.cur.Moderated = true
		s.cur.ModeratedByPresent = attrValue(se.Attr, "by") != ""
		s.v0Confirmed = true
	case legacy && se.Name.Space == retractNS0 && se.Name.Local == "retract":
		s.v0Confirmed = true
	}
}

// chars is called for character data.
func (s *retractScan) chars(cd xml.CharData) {
	if s.reasonDepth > 0 && s.reasonBuf.Len() < 4*maxReasonLen*2 {
		s.reasonBuf.Write(cd)
	}
}

// end is called for every end element with the depth of the element that closes.
func (s *retractScan) end(depth int) {
	if s.reasonDepth > 0 && depth == s.reasonDepth {
		s.reasonDepth = 0
		if s.cur != nil {
			s.cur.Reason = capReason(s.reasonBuf.String())
		}
	}
	if s.cur != nil && depth == s.curDepth {
		s.cur = nil
	}
}

// capReason trims a reason and caps it at maxReasonLen runes.
func capReason(s string) string {
	s = strings.TrimSpace(s)
	if r := []rune(s); len(r) > maxReasonLen {
		s = strings.TrimSpace(string(r[:maxReasonLen]))
	}
	return s
}

// result returns the retraction of the stanza, or nil: the `:1` form wins; the legacy form counts
// only when it contained a `:0` <retract> or <moderated>.
func (s *retractScan) result() *retractInfo {
	var out *retractInfo
	switch {
	case s.v1 != nil:
		c := *s.v1
		c.BothForms = s.v0 != nil && s.v0Confirmed
		out = &c
	case s.v0 != nil && s.v0Confirmed:
		c := *s.v0
		out = &c
	default:
		return nil
	}
	out.HasFallback = s.fallback
	return out
}
