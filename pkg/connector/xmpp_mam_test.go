package connector

import (
	"context"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

// backfillChatAsync must do nothing (and start no goroutine that touches the nil session) when
// backfill is disabled or the client has no login.
func TestBackfillChatAsyncNoOps(t *testing.T) {
	ul := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "me@example.com"}}
	for name, xc := range map[string]*xmppClient{
		"no login":          {conn: &JabberConnector{backfillConfig: &BackfillConfig{Enabled: true}}},
		"no connector":      {login: ul},
		"no backfill cfg":   {login: ul, conn: &JabberConnector{}},
		"backfill disabled": {login: ul, conn: &JabberConnector{backfillConfig: &BackfillConfig{Enabled: false}}},
	} {
		t.Run(name, func(t *testing.T) {
			xc.backfillChatAsync(context.Background(), "+15550001111@cheogram.com", "widget_chat")
			time.Sleep(20 * time.Millisecond) // a started backfill would panic on the nil session or bridge
		})
	}
}
