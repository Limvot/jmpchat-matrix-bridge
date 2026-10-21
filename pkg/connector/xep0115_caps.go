package connector

import (
	"crypto/sha1"

	"mellium.im/xmpp/crypto"
	"mellium.im/xmpp/disco"
	"mellium.im/xmpp/disco/info"
)

// XEP-0115 entity capabilities of the bridge. The initial presence is broadcast to every roster
// contact, whose clients gate features on the answer, so the list is exactly what the bridge consumes
// inbound and nothing else: no chat states (dropped), receipts/markers (not implemented), Jingle
// message initiation (the bridge cannot take a call), no carbons (a server feature), no disco#items.
// urn:xmpp:bookmarks:1+notify asks our server for PEP bookmark events.

const (
	// bridgeCapsNode is the XEP-0115 node URL of this software.
	bridgeCapsNode = "https://github.com/williamkray/jmpchat-matrix-bridge"
	// bridgeIdentityName is the disco identity name (category client, type bot).
	bridgeIdentityName = "jmpchat-matrix-bridge"
)

// bridgeFeatureVars is the single source of truth for the hash and the disco#info reply. Keep sorted.
var bridgeFeatureVars = []string{
	"http://jabber.org/protocol/disco#info",
	oobNS,
	"urn:xmpp:bookmarks:1+notify",
	fallbackNS,
	retractNS0,
	retractNS1,
	reactionsNS,
	replyNS,
	sidNS,
}

// newBridgeDiscoInfo builds a fresh Info (Info.Hash sorts its slices in place, so every user gets its
// own copy). node is echoed as the query node attribute of a reply.
func newBridgeDiscoInfo(node string) disco.Info {
	feats := make([]info.Feature, 0, len(bridgeFeatureVars))
	for _, v := range bridgeFeatureVars {
		feats = append(feats, info.Feature{Var: v})
	}
	return disco.Info{
		InfoQuery: disco.InfoQuery{Node: node},
		Identity:  []info.Identity{{Category: "client", Type: "bot", Name: bridgeIdentityName}},
		Features:  feats,
	}
}

// bridgeCapsVer is the XEP-0115 section 5 verification string (SHA-1) of the bridge's disco#info.
func bridgeCapsVer() string {
	return newBridgeDiscoInfo("").Hash(sha1.New())
}

// bridgeCaps is the <c/> element of the bridge's presence.
func bridgeCaps() disco.Caps {
	return disco.Caps{Hash: crypto.SHA1, Node: bridgeCapsNode, Ver: bridgeCapsVer()}
}

// bridgeCapsDiscoNode is the disco node that answers with the hashed info.
func bridgeCapsDiscoNode() string { return bridgeCapsNode + "#" + bridgeCapsVer() }
