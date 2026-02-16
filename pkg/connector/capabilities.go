package connector

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

const jabberCapID = "github.com/mautrix/jabber.capabilities.2025_02"

// jabberCaps declares which message types the bridge is allowed to send to the connector.
// An empty RoomFeatures causes the bridge core to reject messages before calling HandleMatrixMessage.
// Declare support for text and media so the bridge calls us; we handle unsupported types in code.
var jabberCaps = &event.RoomFeatures{
	ID: jabberCapID,
	File: map[event.CapabilityMsgType]*event.FileFeatures{
		event.MsgImage: {
			MimeTypes: map[string]event.CapabilitySupportLevel{
				"image/*": event.CapLevelFullySupported,
				"*/*":     event.CapLevelFullySupported,
			},
			MaxSize:          50 * 1024 * 1024,
			Caption:          event.CapLevelFullySupported,
		},
		event.MsgVideo: {
			MimeTypes: map[string]event.CapabilitySupportLevel{
				"video/*": event.CapLevelFullySupported,
				"*/*":     event.CapLevelFullySupported,
			},
			MaxSize:          50 * 1024 * 1024,
			Caption:          event.CapLevelFullySupported,
		},
		event.MsgAudio: {
			MimeTypes: map[string]event.CapabilitySupportLevel{
				"audio/*": event.CapLevelFullySupported,
				"*/*":     event.CapLevelFullySupported,
			},
			MaxSize: 20 * 1024 * 1024,
		},
		event.MsgFile: {
			MimeTypes: map[string]event.CapabilitySupportLevel{
				"*/*": event.CapLevelFullySupported,
			},
			MaxSize:          50 * 1024 * 1024,
			Caption:          event.CapLevelFullySupported,
		},
	},
	Reply: event.CapLevelFullySupported,
}

func (x *xmppClient) GetCapabilities(ctx context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	return jabberCaps
}
