package connector

import (
	"context"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
)

// Ensure stubJabberClient implements NetworkAPI at compile time.
var _ bridgev2.NetworkAPI = (*stubJabberClient)(nil)

// stubJabberClient is a minimal NetworkAPI used when a login has no stored
// credentials (meta nil or empty password). It allows the bridge to consider the
// user "logged in" for list-logins/logout; bridging requires a real XMPP session.
type stubJabberClient struct {
	loginID networkid.UserLoginID
}

func (s *stubJabberClient) Connect(ctx context.Context) {}

func (s *stubJabberClient) Disconnect() {}

func (s *stubJabberClient) IsLoggedIn() bool { return true }

func (s *stubJabberClient) LogoutRemote(ctx context.Context) {}

func (s *stubJabberClient) IsThisUser(ctx context.Context, userID networkid.UserID) bool {
	return networkid.UserLoginID(userID) == s.loginID
}

func (s *stubJabberClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *stubJabberClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *stubJabberClient) GetCapabilities(ctx context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	return jabberCaps
}

func (s *stubJabberClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	return nil, fmt.Errorf("not connected to Jabber (no XMPP session); reconnect or log in again")
}
