package connector

import (
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// makeEventSender creates an EventSender for a given JID.
// Aligned with Signal bridge pattern: includes SenderLogin for proper ghost user tracking.
func (x *xmppClient) makeEventSender(senderJID string) bridgev2.EventSender {
	senderID := networkid.UserID(senderJID)
	isFromMe := networkid.UserLoginID(senderJID) == x.loginID
	return bridgev2.EventSender{
		IsFromMe:    isFromMe,
		SenderLogin: networkid.UserLoginID(senderJID),
		Sender:      senderID,
	}
}
