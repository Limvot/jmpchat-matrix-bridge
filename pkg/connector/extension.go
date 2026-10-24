package connector

import (
	"context"

	"go.mau.fi/util/configupgrade"
	"go.opentelemetry.io/otel/attribute"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// connectorExtension is optional functionality compiled in with a build tag. Every method is called only
// when an extension is registered. Implementations must be safe for concurrent use.
type connectorExtension interface {
	exampleConfig() string                                     // appended to GetConfig's example
	upgradeConfig(h configupgrade.Helper)                      // called from GetConfig's upgrader
	init(br *bridgev2.Bridge)                                  // from JabberConnector.Init
	postInit(proc *commands.Processor, network *yaml.Node)     // from PostInitExtensions (main PostInit)
	start(ctx context.Context) error                           // from JabberConnector.Start; error fails startup
	stop()                                                     // from JabberConnector.Stop, before telemetry.Shutdown
	loginLoaded(ul *bridgev2.UserLogin, x *xmppClient)         // from LoadUserLogin, real clients only
	loginStopped(x *xmppClient)                                // from xmppClient.Disconnect, after the XMPP teardown
	decorateChatInfo(portalID string, info *bridgev2.ChatInfo) // DM chat info (getChatInfo DM paths, dmCreateChatResponse)
	suppressCallNotice(loginID networkid.UserLoginID, peerBareJID string) bool
	// feedCallJMI hands a parsed XEP-0353 JMI action to the extension (e.g. the Jingle call leg);
	// true = consumed, no notices. feedCallIQ does the same for a raw <jingle/> IQ payload.
	feedCallJMI(ctx context.Context, cl *xmppClient, fromBare string, fromSelf bool, jmi *jingleMessageInfo) bool
	feedCallIQ(ctx context.Context, cl *xmppClient, from, iqID string, payload []byte) bool
	startAttrs() []attribute.KeyValue // extra bridge.start attributes
}

// registerExtension is set by an init() in a build-tagged file; nil in the default build.
var registerExtension func(c *JabberConnector) connectorExtension

// extension returns the connector's extension (created once; nil when none is compiled in).
// GetConfig runs before Init (mxmain), so this must not depend on c.br.
func (c *JabberConnector) extension() connectorExtension {
	if c == nil || registerExtension == nil {
		return nil
	}
	c.extnOnce.Do(func() { c.extn = registerExtension(c) })
	return c.extn
}

// PostInitExtensions is called from main's PostInit after SetNetworkConfig. No-op without an extension.
func (c *JabberConnector) PostInitExtensions(proc *commands.Processor, network *yaml.Node) {
	if e := c.extension(); e != nil {
		e.postInit(proc, network)
	}
}

// ExtensionStartAttrs returns extra bridge.start attributes (nil without an extension).
func (c *JabberConnector) ExtensionStartAttrs() []attribute.KeyValue {
	if e := c.extension(); e != nil {
		return e.startAttrs()
	}
	return nil
}

// decorateChatInfo lets the extension adjust DM chat info. Nil-safe on c and info.
func (c *JabberConnector) decorateChatInfo(portalID string, info *bridgev2.ChatInfo) {
	if info == nil {
		return
	}
	if e := c.extension(); e != nil {
		e.decorateChatInfo(portalID, info)
	}
}
