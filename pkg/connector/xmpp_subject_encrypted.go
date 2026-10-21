package connector

import (
	"context"
	"strings"
	"time"
	"unicode"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
)

// Live MUC subject (XEP-0045 section 8.1) and encrypted messages (OMEMO, OpenPGP, XEP-0454 files).

// sanitizeSubject removes control characters (except newline and tab) from a room subject.
func sanitizeSubject(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || !unicode.IsControl(r) {
			return r
		}
		return -1
	}, s)
}

// queueMUCSubject bridges a subject change of a room as a topic change of its portal. Memory and queueing
// only (Serve goroutine). The subject is content: only its length is ever logged. The event has no sender,
// so bridgev2 sends it as the bridge bot (occupant ghosts may lack the power level, and the room re-sends
// the subject on every join; bridgev2 ignores an unchanged topic). An empty subject clears the topic.
func (x *xmppClient) queueMUCSubject(ctx context.Context, room, subject string) {
	if x.login == nil || room == "" {
		return
	}
	topic := sanitizeSubject(subject)
	x.logFor(ctx).Debug().Str("room", room).Int("subject_len", len(topic)).Msg("MUC subject received, queuing topic change")
	x.queueRemoteEvent(ctx, &simplevent.ChatInfoChange{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatInfoChange,
			PortalKey:    networkid.PortalKey{ID: networkid.PortalID(room), Receiver: x.loginID},
			Timestamp:    time.Now(),
			CreatePortal: false,
		},
		ChatInfoChange: &bridgev2.ChatInfoChange{ChatInfo: &bridgev2.ChatInfo{Topic: &topic}},
	})
}

// Encrypted message kinds (the value of jabberRemoteMessage.encrypted).
const (
	encKindOMEMO   = "omemo"
	encKindOpenPGP = "openpgp"
	encKindFile    = "file"
	encKindOther   = "other"
)

// encryptedKind classifies a stanza as encrypted from its extras and its body/OOB URL; "" when it is not.
// Bodies are only inspected for the aesgcm:// scheme (XEP-0454), never stored or logged.
func encryptedKind(e messageExtras, body, oobURL string) string {
	if strings.HasPrefix(strings.TrimSpace(body), "aesgcm://") || strings.HasPrefix(strings.TrimSpace(oobURL), "aesgcm://") {
		return encKindFile
	}
	switch e.Encryption {
	case "":
		return ""
	case "omemo", "omemo2":
		return encKindOMEMO
	case "openpgp", "pgp":
		return encKindOpenPGP
	}
	return encKindOther
}

// applyEncryption returns the kind and the body/OOB URL to bridge: for an encrypted message with a
// body (or OOB URL) the content is dropped (never downloaded, never bridged). Body-less encrypted
// stanzas (e.g. OMEMO key transport) are dropped by the callers' existing "no body" rules.
func applyEncryption(e messageExtras, body, oobURL string) (kind, outBody, outOOB string) {
	if body == "" && oobURL == "" {
		return "", body, oobURL
	}
	if kind = encryptedKind(e, body, oobURL); kind != "" {
		return kind, "", ""
	}
	return "", body, oobURL
}

// encryptedNotice is the fixed notice text for an encrypted message (no message content).
func encryptedNotice(kind string) string {
	const tail = " and cannot be shown here. Open it in an XMPP client."
	switch kind {
	case encKindOMEMO:
		return "This message was end-to-end encrypted (OMEMO)" + tail
	case encKindOpenPGP:
		return "This message was end-to-end encrypted (OpenPGP)" + tail
	case encKindFile:
		return "This message was end-to-end encrypted (an encrypted file)" + tail
	}
	return "This message was end-to-end encrypted" + tail
}

// mamBackfillContent is the Matrix content of an archived message in FetchMessages: its text, or the
// encrypted notice.
func mamBackfillContent(item mamResultItem) *event.MessageEventContent {
	if kind := encryptedKind(item.Extras, item.Body, ""); kind != "" {
		return encryptedNoticeContent(kind)
	}
	return &event.MessageEventContent{MsgType: event.MsgText, Body: item.Body}
}

// encryptedNoticeContent is the Matrix content of the notice.
func encryptedNoticeContent(kind string) *event.MessageEventContent {
	return &event.MessageEventContent{MsgType: event.MsgNotice, Body: encryptedNotice(kind)}
}
