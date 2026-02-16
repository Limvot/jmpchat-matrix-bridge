# Code optimization tracking

**Last updated:** 2025-02-18

Recommendations from codebase analysis (duplication, library overlap, configuration, structure, clarity, formatting, best practices). Resolved items are listed at the end.

## Duplication and reuse

- **[D-1] JID validation and normalization repeated** — Location: `cmd/mautrix-jabber/main.go` (cmdRosterAdd, cmdMUCJoin, cmdMUCLeave, cmdMUCNick), `pkg/connector/connector.go` (SubmitUserInput), `pkg/connector/xmpp_client.go` (SetRosterItem, ResolveIdentifier, JoinMUC, LeaveMUC). Recommendation: Extract a small helper e.g. `NormalizeAndValidateJID(s string) (string, error)` or `ParseBareJID(s string) (jid.JID, error)` used by commands and connector; centralize "trim, toLower, contains @" and parse in one place to avoid drift.

- **[D-2] Command handler boilerplate (login + client type check)** — Location: `cmd/mautrix-jabber/main.go`. Each command repeats: `login := ce.User.GetDefaultLogin(); if login == nil { ce.Reply("You must be logged in..."); return }; client, ok := login.Client.(connector.SomeInterface); if !ok { ce.Reply("Not connected to Jabber..."); return }`. Recommendation: Add a helper e.g. `getLoginAndClient[T connector.SomeInterface](ce *commands.Event) (login *..., client T, ok bool)` or a shared `requireLoginAndMUCJoiner(ce) (connector.MUCJoiner, bool)` so reply strings and logic live in one place.

- **[D-3] Session nil check and "not connected" return** — Location: `pkg/connector/xmpp_client.go` (many methods: SetRosterItem, isMUCJID path, JoinMUC, LeaveMUC, HandleMatrixMessage, upload path; also `xmpp_mam.go`). Recommendation: Add `func (x *xmppClient) sessionOrError() (*xmpp.Session, error)` that does lock/copy/unlock and returns a consistent error if nil; use it at the start of methods that need a session to reduce repeated `x.mu.Lock(); sess := x.session; x.mu.Unlock(); if sess == nil { return ..., fmt.Errorf("not connected to XMPP") }`.

- **[D-4] MAM backfill search-window logic duplicated** — Location: `pkg/connector/xmpp_mam.go` (runMAMCatchUpOnce/dedup and mamBackfillOnView/dedup both use `searchStart := item.Timestamp.Add(-2*time.Minute)`; if after end then `searchStart = end.Add(-5*time.Minute)`; then GetMessagesBetweenTimeQuery(..., searchStart, end.Add(time.Minute))). Recommendation: Extract e.g. `mamDedupSearchWindow(itemTimestamp, end time.Time) (start, end time.Time)` or a small struct with the two time bounds so the 2min/5min constants and query call live in one place.

- **[D-5] Last-backfill / last-sync map init pattern** — Location: `pkg/connector/xmpp_client.go` (lastBackfillOnView, lastMAMSync in xmpp_mam), `pkg/connector/xmpp_mam.go` (lastMAMSync). Repeated `mu.Lock(); if m == nil { m = make(map[string]time.Time) }; v := m[key]; mu.Unlock()`. Recommendation: Consider a tiny helper `getOrInitTimeMap(mu *sync.Mutex, m *map[string]time.Time, key string) (time.Time, bool)` or document the pattern in a single comment so future additions stay consistent.

## Library overlap and reinventions

- **[L-1] decodeBridgeUserID may duplicate bridge framework behavior** — Location: `pkg/connector/xmpp_client.go`. Custom logic reverses appservice localpart encoding (=40→@, =2f→/) so ghost.ID can be treated as a JID. Recommendation: Check whether `maunium.net/go/mautrix/bridgev2` or the appservice layer exposes a decode function for ghost/user IDs; if so, use it instead of custom `decodeBridgeUserID`.

- **[L-2] generateXMPPMessageID uses custom rand+hex while xid is already a dependency** — Location: `pkg/connector/xmpp_message.go`. Uses `rand.Read` + `hex.EncodeToString` for XMPP message IDs. `github.com/rs/xid` is already used elsewhere (e.g. `carbonsEnableID`). Recommendation: Consider `xid.New().String()` for message IDs to align with existing ID generation and reduce custom crypto code.

- **[L-3] FormattedBody (Matrix HTML) sent raw to XMPP when Body is empty** — Location: `pkg/connector/xmpp_client.go` HandleMatrixMessage. When `msg.Content.Body` is empty, `msg.Content.FormattedBody` (Matrix HTML) is used as-is for the XMPP body. XMPP typically expects plain text. Recommendation: Consider `maunium.net/go/mautrix/format` (ParseHTML or equivalent) to convert Matrix HTML to plain text when sending to XMPP, so rich formatting is stripped and clients receive readable text.

## Configuration

- **[CFG-1] network.default_port is not used** — Location: `pkg/connector/connector.go`. `JabberConfig` has `DefaultPort` and the example config shows `default_port: 5222`, but `dialXMPP` uses a hardcoded `xmppDefaultPort = "5222"`. Recommendation: Use the connector's network config when building the dial address: e.g. in `dialXMPP` (or a wrapper that has config), use `c.networkConfig.DefaultPort` with 5222 as fallback when 0. Ensures config.yaml changes to `default_port` take effect.

- **[CFG-2] network.default_server and network.resource_prefix are never read** — Location: `pkg/connector/connector.go` (JabberConfig struct and GetConfig example), `pkg/connector/connector.go` (dialXMPP uses origin's domain; BindResource() is called with no args). The example config documents `default_server`, `default_port`, and `resource_prefix`, but only `cheogram` is used. Recommendation: Either wire them (e.g. use DefaultServer when suggesting or validating server in login flow; pass ResourcePrefix to mellium's bind if the library supports a client-chosen resource prefix) or remove them from the example and struct and document "reserved for future use" so operators are not misled.

- **[CFG-3] MAM backfill time window is hardcoded** — Location: `pkg/connector/xmpp_mam.go`. `mamBackfillWindow = 365 * 24 * time.Hour` is fixed; `BackfillConfig` has `Enabled` and `MaxInitialMessages` but no history window. Recommendation: Add a config option (e.g. `backfill.history_days` or `backfill.history_duration`) to the bridge config and BackfillConfig; use it in `mamBackfillOnView` so operators can limit how far back MAM fetches (e.g. 7 or 30 days for performance or policy).

- **[CFG-4] Key timeouts and intervals not configurable** — Location: Scattered (see also S-2). Login dial timeout (50s), DNS/dial timeouts (15s), reconnect backoff (5s–5min), MAM catch-up interval (45s), MUC join delay (1500ms), media download/upload timeouts (60s), etc. are all hardcoded. Recommendation: Consider a small `network.tuning` or per-feature config section for operator-tunable values (e.g. `dial_timeout_seconds`, `mam_catch_up_interval_seconds`) so deployments can adapt without code changes; lower priority than CFG-1–CFG-3.

## Structure and references

- **[S-1] xmpp_client.go is very large** — Location: `pkg/connector/xmpp_client.go` (~1.8k lines). Recommendation: Split by concern: e.g. move MAM (runMAMCatchUpLoop, mamCatchUpForPortal, fetchMAMItems, mamBackfillOnView, dedup logic) into `xmpp_mam.go` (already has some MAM); move media upload (XEP-0363 discovery, HandleMatrixMessage media branch) into a `media.go` or keep in xmpp_client but extract functions to reduce HandleMatrixMessage length. Align with mautrix-signal file size if desired.

- **[S-2] Timeouts and intervals as magic numbers** — Location: Scattered (e.g. connector.go 50s dial, 15s resolve/dial; xmpp_client 5s carbons, 60s connect, 30s reconnect sleep, 1500ms MUC join delay; xmpp_mam 45s catch-up, 2min window, 25s timer; xmpp_inbound 5s/10s/60s timeouts, 400ms sleep, 1500ms metadata delay; xep0363 15s/60s). Recommendation: Group in a single `pkg/connector/timing.go` or at top of respective files as named constants (e.g. `dialTimeout`, `mamCatchUpInterval`) so tuning and documentation are in one place; already partially done in xmpp_mam and xep0363.

- **[S-3] xmppClient holds direct reference to JabberConnector** — Location: `xmppClient.conn *JabberConnector`. Recommendation: Fine for current design; if connector grows more optional features, consider injecting only the needed dependencies (e.g. backfillConfig, br) to simplify testing and reduce coupling. Low priority.

## Clarity and maintainability

- **[C-1] Remove or gate "#region agent log" and "DEBUG:" log messages** — Location: `pkg/connector/xmpp_client.go`, `pkg/connector/xmpp_inbound.go`, `pkg/connector/xmpp_message.go`. Many Debug logs are prefixed with "DEBUG:" and some are wrapped in `#region agent log`. Recommendation: Remove region comments (non-Go idiom); either drop "DEBUG:" prefix (zerolog level already implies debug) or guard verbose message-body/identifier logs behind a build tag or config so production logs stay minimal.

- **[C-2] HandleMatrixMessage is long and branchy** — Location: `pkg/connector/xmpp_client.go` HandleMatrixMessage. Recommendation: Extract steps into named functions: e.g. `resolveMessageBody(msg)`, `ensureMUCJoinedForSend(ctx, portal, toJIDStr)`, `buildXMPPMessageContent(msg, body, ...)`, `sendAndStoreMessageID(...)`. Improves readability and unit-testability of each step.

- **[C-3] MUC vs DM portal handling spread across methods** — Location: portalIsMUC, GetChatInfo, JoinMUC, message routing in HandleMatrixMessage and inbound. Recommendation: Add a short comment or doc at the top of xmpp_client.go (or a CONTRIBUTING snippet) summarizing how DM vs MUC is decided (portal key shape, disco cache) and where join/leave/message routing differ; helps future MUC (e.g. 3.5) and OMEMO work.

- **[C-4] Inbound message queueing: two near-identical blocks for reaction vs regular** — Location: `pkg/connector/xmpp_inbound.go` handleMessageWithOOB (reaction loop vs single QueueRemoteEvent). Recommendation: Consider building one jabberRemoteMessage and then looping only over reaction emojis when reactionTarget is set, to avoid duplicating struct field assignment.

## Formatting and best practices

- **[F-1] No unit tests** — Location: entire `mautrix-jabber` (no `*_test.go` files). Recommendation: Add tests for pure logic first: e.g. `parseJID`, `truncate`, Cheogram JID parsing (ParseCheogramGroupMMSJID, ResolveCheogramIdentifier), and MAM dedup window logic; then table-driven tests for error paths (invalid JID, empty password). Use Docker builder for `go test ./...` per go-docker-only rule.

- **[F-2] Inconsistent error wrapping for user-facing messages** — Location: Some places return `fmt.Errorf("...: %w", err)` (e.g. SetRosterItem "invalid JID format: %w", JoinMUC "invalid MUC JID %q: %w") which can expose JID or parser details to the user. Recommendation: Align with jabber-security: return generic errors to the user; log details at Debug. See also SECURITY.md [M-1].

- **[F-3] context.Background() in async paths** — Location: e.g. `go x.mamBackfillOnView(x.log.WithContext(context.Background()), toJIDStr)`, `context.WithTimeout(context.Background(), 5*time.Second)` in xmpp_inbound. Recommendation: Where the originating request context is not available, document why Background is used; consider passing a detached context with a timeout or cancel tied to login lifecycle so long-running backfills don't outlive the session.

## Resolved / Acknowledged

- *(None.)*
