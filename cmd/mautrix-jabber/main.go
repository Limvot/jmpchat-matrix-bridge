package main

import (
	"fmt"
	"strings"

	"github.com/mautrix/jabber/pkg/connector"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"
)

// Filled at build time with -X linker flags (e.g. -X main.Tag=... -X main.Commit=... -X main.BuildTime=...)
var (
	Tag       = "unknown"
	Commit    = "unknown"
	BuildTime = "unknown"
)

func cmdPrefix(ce *commands.Event) string {
	if ce.Bridge.Config != nil && ce.Bridge.Config.CommandPrefix != "" {
		return ce.Bridge.Config.CommandPrefix
	}
	return "!jabber"
}

func main() {
	br := &mxmain.BridgeMain{
		Name:        "mautrix-jabber",
		Description: "A Matrix-Jabber (XMPP) puppeting bridge.",
		URL:         "https://github.com/williamkray/mautrix-jabber",
		Version:     "0.1.5",
		Connector:   &connector.JabberConnector{},
	}
	br.PostInit = func() {
		// Inject decoded network config into connector (bridge only receives BridgeConfig, not Network).
		var netCfg connector.JabberConfig
		if err := br.Config.Network.Decode(&netCfg); err == nil {
			if jc, ok := br.Connector.(*connector.JabberConnector); ok {
				jc.SetNetworkConfig(&netCfg)
				// Respect matrix.delivery_receipts for read receipts when bridging messages.
				jc.SetDeliveryReceipts(br.Config.Matrix.DeliveryReceipts)
				// Backfill config for MAM backfill-on-view and full backfill on login/sync-roster (config.backfill).
				jc.SetBackfillConfig(&connector.BackfillConfig{
					Enabled:            br.Config.Backfill.Enabled,
					MaxInitialMessages: br.Config.Backfill.MaxInitialMessages,
				})
				jc.SetAppServiceID(br.Config.AppService.ID)
			}
		}
		prefix := br.Config.Bridge.CommandPrefix
		if prefix == "" {
			prefix = "!jabber"
		}
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:   cmdSyncRoster,
			Name:   "sync-roster",
			RequiresLogin: true,
			Help:   commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Re-fetch roster from server and update Matrix display/room names; when backfill is enabled, also runs full MAM backfill for roster DMs. Usage: `%s sync-roster`", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:   cmdAddToRoster,
			Name:   "add-to-roster",
			RequiresLogin: true,
			Help:   commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Add or rename a contact in your Jabber roster. Usage: `%s add-to-roster <jid> <name>`", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:   cmdSearchRoster,
			Name:   "search-roster",
			RequiresLogin: true,
			Help:   commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Search your Jabber roster for contacts matching a JID or name. Usage: `%s search-roster <query>`", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:   cmdMUCJoin,
			Name:   "join",
			RequiresLogin: true,
			Help:   commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Join a multi-user chat (MUC) room. Usage: `%s join <room-jid>` (e.g. room@conference.example.com)", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:   cmdMUCLeave,
			Name:   "leave",
			RequiresLogin: true,
			Help:   commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Leave a multi-user chat (MUC) room. Usage: `%s leave <room-jid>`", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:   cmdMUCNick,
			Name:   "muc-nick",
			RequiresLogin: true,
			Help:   commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Set preferred MUC nickname for a room. Usage: `%s muc-nick <room-jid> <nick>`", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:   cmdMUCNickDefault,
			Name:   "muc-nick-default",
			RequiresLogin: true,
			Help:   commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Set default MUC nickname for all rooms. Usage: `%s muc-nick-default <nick>`", prefix)},
		})
	}
	br.InitVersion(Tag, Commit, BuildTime)
	br.Run()
}

func cmdSyncRoster(ce *commands.Event) {
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	rs, ok := login.Client.(connector.RosterSyncer)
	if !ok {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	rs.SyncRoster(ce.Ctx)
	// When backfill is enabled, run full MAM backfill for all roster DMs so portals get history
	// without needing to log out and back in (or wait for full_backfill_on_login on next connect).
	if bf, ok := login.Client.(connector.FullBackfillRunner); ok {
		go bf.RunFullBackfill(ce.Ctx)
		ce.Reply("Roster sync started. Display names and room names will update shortly. Full backfill (if enabled) is running in the background.")
		return
	}
	ce.Reply("Roster sync started. Display names and room names will update shortly.")
}

func cmdAddToRoster(ce *commands.Event) {
	if len(ce.Args) < 2 {
		ce.Reply("Usage: `%s add-to-roster <jid> <name>` — e.g. `%s add-to-roster friend@jabber.org My Friend`", cmdPrefix(ce), cmdPrefix(ce))
		return
	}
	jid := strings.TrimSpace(strings.ToLower(ce.Args[0]))
	name := strings.TrimSpace(strings.Join(ce.Args[1:], " "))
	if jid == "" || !strings.Contains(jid, "@") {
		ce.Reply("Invalid JID. Use the form user@domain (e.g. user@jabber.org).")
		return
	}
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	rm, ok := login.Client.(connector.RosterManager)
	if !ok {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	if err := rm.SetRosterItem(ce.Ctx, jid, name); err != nil {
		ce.Reply("Failed to set roster name: %v", err)
		return
	}
	ce.Reply("Roster updated: **%s** is now **%s**. Display and room names should update shortly.", jid, name)
}

func cmdSearchRoster(ce *commands.Event) {
	if len(ce.Args) == 0 {
		ce.Reply("Usage: `%s search-roster <query>` — e.g. `%s search-roster megan` or `%s search-roster @example.com`", cmdPrefix(ce), cmdPrefix(ce), cmdPrefix(ce))
		return
	}
	query := strings.TrimSpace(strings.Join(ce.Args, " "))
	if query == "" {
		ce.Reply("Query cannot be empty.")
		return
	}
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	searcher, ok := login.Client.(connector.RosterSearcher)
	if !ok {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	results, err := searcher.SearchRoster(ce.Ctx, query)
	if err != nil {
		ce.Reply("Failed to search roster: %v", err)
		return
	}
	if len(results) == 0 {
		ce.Reply("No contacts found matching **%s**.", query)
		return
	}
	var lines []string
	for _, contact := range results {
		if contact.Name != "" {
			lines = append(lines, fmt.Sprintf("- **%s** (%s)", contact.Name, contact.JID))
		} else {
			lines = append(lines, fmt.Sprintf("- %s", contact.JID))
		}
	}
	ce.Reply("Found %d contact(s) matching **%s**:\n%s", len(results), query, strings.Join(lines, "\n"))
}

func cmdMUCJoin(ce *commands.Event) {
	if len(ce.Args) == 0 {
		ce.Reply("Usage: `%s join <room-jid>` — e.g. `%s join myroom@conference.example.com`", cmdPrefix(ce), cmdPrefix(ce))
		return
	}
	roomJID := strings.TrimSpace(strings.ToLower(ce.Args[0]))
	if roomJID == "" || !strings.Contains(roomJID, "@") {
		ce.Reply("Invalid room JID. Use the form room@conference.domain (e.g. myroom@conference.example.com).")
		return
	}
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	muc, ok := login.Client.(connector.MUCJoiner)
	if !ok {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	if err := muc.JoinMUC(ce.Ctx, roomJID); err != nil {
		ce.Reply("Failed to join room: %v", err)
		return
	}
	// Get or create the Matrix portal so the user sees the room.
	portalKey := networkid.PortalKey{ID: networkid.PortalID(roomJID), Receiver: login.ID}
	portal, err := ce.Bridge.GetPortalByKey(ce.Ctx, portalKey)
	if err != nil {
		ce.Reply("Joined **%s** on Jabber, but could not open Matrix room: %v", roomJID, err)
		return
	}
	if portal.MXID == "" {
		info, err := login.Client.GetChatInfo(ce.Ctx, portal)
		if err != nil || info == nil {
			ce.Reply("Joined **%s** on Jabber. Opening Matrix room failed: %v", roomJID, err)
			return
		}
		if err := portal.CreateMatrixRoom(ce.Ctx, login, info); err != nil {
			ce.Reply("Joined **%s** on Jabber, but creating Matrix room failed: %v", roomJID, err)
			return
		}
		portal.UpdateBridgeInfo(ce.Ctx)
		portal.UpdateCapabilities(ce.Ctx, login, true)
		userIntent := ce.User.DoublePuppet(ce.Ctx)
		if userIntent != nil {
			_ = userIntent.EnsureJoined(ce.Ctx, portal.MXID)
		}
	}
	ce.Reply("Joined **%s**. Room: %s", roomJID, portal.MXID.String())
}

func cmdMUCLeave(ce *commands.Event) {
	if len(ce.Args) == 0 {
		ce.Reply("Usage: `%s leave <room-jid>` — e.g. `%s leave myroom@conference.example.com`", cmdPrefix(ce), cmdPrefix(ce))
		return
	}
	roomJID := strings.TrimSpace(strings.ToLower(ce.Args[0]))
	if roomJID == "" || !strings.Contains(roomJID, "@") {
		ce.Reply("Invalid room JID. Use the form room@conference.domain (e.g. myroom@conference.example.com).")
		return
	}
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	muc, ok := login.Client.(connector.MUCJoiner)
	if !ok {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	if err := muc.LeaveMUC(ce.Ctx, roomJID); err != nil {
		ce.Reply("Failed to leave room: %v", err)
		return
	}
	ce.Reply("Left **%s**. You can leave the Matrix room from the room menu if desired.", roomJID)
}

func cmdMUCNick(ce *commands.Event) {
	if len(ce.Args) < 2 {
		ce.Reply("Usage: `%s muc-nick <room-jid> <nick>` — e.g. `%s muc-nick myroom@conference.example.com MyNick`", cmdPrefix(ce), cmdPrefix(ce))
		return
	}
	roomJID := strings.TrimSpace(strings.ToLower(ce.Args[0]))
	nick := strings.TrimSpace(ce.Args[1])
	if roomJID == "" || !strings.Contains(roomJID, "@") {
		ce.Reply("Invalid room JID. Use the form room@conference.domain.")
		return
	}
	if nick == "" {
		ce.Reply("Nick cannot be empty.")
		return
	}
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	if err := connector.SetMUCPreferredNickForRoom(ce.Ctx, ce.Bridge, login.ID, roomJID, nick); err != nil {
		ce.Reply("Failed to set MUC nick: %v", err)
		return
	}
	ce.Reply("Preferred nick for **%s** set to **%s**. It will be used on next join (and on 409 retry).", roomJID, nick)
}

func cmdMUCNickDefault(ce *commands.Event) {
	if len(ce.Args) == 0 {
		ce.Reply("Usage: `%s muc-nick-default <nick>` — e.g. `%s muc-nick-default MyNick`", cmdPrefix(ce), cmdPrefix(ce))
		return
	}
	nick := strings.TrimSpace(strings.Join(ce.Args, " "))
	if nick == "" {
		ce.Reply("Nick cannot be empty.")
		return
	}
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	if err := connector.SetDefaultMUCNick(ce.Ctx, ce.Bridge, login, nick); err != nil {
		ce.Reply("Failed to set default MUC nick: %v", err)
		return
	}
	ce.Reply("Default MUC nick set to **%s**. It will be used for rooms that don't have a room-specific nick.", nick)
}
