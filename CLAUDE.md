# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

jmpchat-matrix-bridge (formerly mautrix-jabber) is a Matrix-Jabber (XMPP) puppeting bridge written in Go, built on the **mautrix bridgev2** framework, with a focus on JMP.chat/Cheogram. It is also a generic XMPP bridge: bidirectional DM and MUC bridging, media file transfers, reactions, replies, message carbons, MAM backfill, and Cheogram SMS/MMS integration.

## Build

Builds use Docker to ensure a repeatable environment with all C dependencies (CGO, libolm, libopus, sqlite-dev):

```bash
./build.sh
```

This runs `docker build` (tags `sipcalls nolibopusfile`, so SIP calls are included) and extracts the binary. For a local build without encryption support (requires Go 1.26+, CGO, sqlite-dev):

```bash
go build -tags nocrypto -ldflags "-s -w" -o jmpchat-matrix-bridge ./cmd/jmpchat-matrix-bridge
```

To include calls in a local build (also needs libopus dev headers and pkg-config): `go build -tags "nocrypto sipcalls nolibopusfile" ...`.

The production build includes libolm for Matrix E2EE. No test or lint configuration exists yet.

## Naming

The project was renamed from mautrix-jabber to jmpchat-matrix-bridge (issue #9; module `github.com/williamkray/jmpchat-matrix-bridge`, binary and `cmd/` dir `jmpchat-matrix-bridge`). Kept on purpose: `DBOwner: "megabridge/mautrix-jabber"` in `cmd/jmpchat-matrix-bridge/main.go` (existing databases store that owner and the bridge refuses to start on a mismatch; never change it), the legacy env var `MAUTRIX_JABBER_CHEOGRAM` (still read as a fallback), `jabberCapID` (stored capability ID), `ContentKeyStanzaID` (`me.mssj.mautrixjabber.stanza_id`, key in already-sent event content), `NetworkID` "jabber", prefix `!jabber`, and the `mautrix-jabber-verify.db` local file name. The image keeps a compat symlink `/app/mautrix-jabber`.

## Architecture

All bridge logic lives in `pkg/connector/`. The bridgev2 framework handles Matrix protocol, appservice lifecycle, config, and database.

### Entry Point

- **`cmd/jmpchat-matrix-bridge/main.go`** — Bootstrap. Creates `BridgeMain` with `JabberConnector`, registers custom bot commands (`sync-roster`, `add-to-roster`, `save-contact`, `remove-from-roster`, `search-roster`, `join`, `leave`, `muc-nick`, `muc-nick-default`), injects decoded network config via `PostInit`.

### Connector Core

- **`pkg/connector/connector.go`** — `JabberConnector` (implements `bridgev2.NetworkConnector`). Login flow (JID+password via `jabberLoginProcess`), XMPP session dial (`dialXMPP` with DNS→TCP→TLS/SASL), `LoadUserLogin` (spawns `xmppClient` or `stubJabberClient`). Also defines helper interfaces (`RosterSyncer`, `RosterManager`, `MUCJoiner`) and MUC nick persistence functions.

- **`pkg/connector/xmpp_client.go`** — `xmppClient` (implements `bridgev2.NetworkAPI` and related interfaces). The largest file (~1800 lines). Handles: reconnect loop with exponential backoff, roster sync, MUC join/leave/nick, `HandleMatrixMessage` (text, media, replies, reactions → XMPP), `ResolveIdentifier` (JID/phone → portal key), `GetChatInfo`/`GetUserInfo`, portal type detection (`portalIsMUC` via disco#info or heuristic).

- **`pkg/connector/muc_nick_conflict.go`** — MUC nick conflict handling: on 409 `onMUCJoinConflict` (Serve goroutine, state only) advances a per-room `mucJoinTrack` (`base_1`..`base_3`), `continueMUCJoinAfterConflict` sends the retry from a goroutine with its own span. Confirmation = the existing pending-nick match in `handlePresence` (our available presence from `room/nick`); then `announceMUCNickChange` stores `PortalMetadata.MUCAutoNick`/`MUCAutoNickBase` (auto, only valid while the preferred base is unchanged; `muc-nick` clears it, see `chooseMUCNick`) and posts an m.notice to the portal room and the management room. All-taken posts a failure notice.

### Message Handling

- **`pkg/connector/xmpp_inbound.go`** — Inbound message routing. Dispatches incoming XMPP stanzas to DM or MUC handlers, extracts sender, determines portal key, queues `RemoteEvent` for the framework. Handles self-echo filtering for MUC.

- **`pkg/connector/xmpp_message.go`** — Message conversion. Parses XMPP message content (body text, XEP-0066 OOB media URLs, XEP-0461 replies, XEP-0444 reactions, chat state notifications). Builds `bridgev2.ConvertedMessagePart` for Matrix. Handles deferred stanza ID metadata updates.

### Retractions and moderation (XMPP -> Matrix)

- **`pkg/connector/xmpp_retract_parse.go`** — `retractScan`: XEP-0424/0425 (`:1`) and legacy `:0` detection by resolved namespace on direct children of `<message>` (never the XEP-0353 jingle `<retract>`); the parsers (`parseMessageBodyOOBIDs`, carbons, MAM) fill `messageIDs.Retract`/`Tombstone` and drop the fallback body whenever a retract element is present.
- **`pkg/connector/xmpp_retract.go`** — room-scoped `findMessagesByXMPPRef` (per-kind lookup order). **`xmpp_retract_event.go`** — `retraction`, `validateRetraction` (all sender rules), `newRetractionEvent` (`simplevent.MessageRemove` + `PreHandleWithResultFunc`/`PostHandleFunc`, `retractionEnv` seam), reason path (`removeWithReason`: connector redacts with `Reason` and returns Success; no reason uses the bridgev2 path), the `jabber_retracted:<login>:<lower portal>:<scope>:<ref>` KV tombstones (scope `snd=<sender id>`, `occ=<occupant-id>` or the sender-independent `stanza` for room/archive stanza-ids; hidden MAM originals are tombstoned too) (checked in `jabberRemoteMessage.ConvertMessage` and the MAM paths) and the MAM batch filter.
### Redactions (Matrix -> XMPP)

- **`pkg/connector/xmpp_retract_out.go`** — `HandleMatrixMessageRemove` (`RedactionHandlingNetworkAPI`): own messages only (`isOwnMessage`), Cheogram JIDs (`IsCheogramDMJID`, incl. group MMS) are refused with an `m.notice` and a nil return (redaction stands), DM ref via `xmppIDForMessage` (origin-id, else id; `matrix:` rows use `XMPPMessageID`), MUC ref = room stanza-id only (`resolveRetractRef`), then tombstones via `markRetractedRefs`. Stanza builders `retractionPayload`/`sendXMPPRetraction` are in `xmpp_message.go`.
- **`pkg/connector/xmpp_muc_echo.go`** — `mucEchoTable`: rendezvous between the room's echo of our MUC message (Serve goroutine, `handleOwnMUCEcho`, exact origin-id/id match, memory only) and the row insert (`MatrixMessageResponse.PostSave`); stores `StanzaID`/`OccupantID` on the row; redactions wait up to `mucEchoWait` (3 s). `handleMatrixMessage` registers the id before sending. Issue #11 (own-nick messages from other clients) should reuse `matchEcho`.

- Retraction events never create portals and go through `queueRemoteEvent` (`x.queueHook` is a test seam).

### Protocol Extensions

- **`pkg/connector/xmpp_carbons.go`** — XEP-0280 Message Carbons. Enables on connect, parses `<sent>`/`<received>` forwarded stanzas, queues as remote events.

- **`pkg/connector/xmpp_mam.go`** — XEP-0313 Message Archive Management. Backfill-on-view (`FetchMessages`), periodic catch-up loop (~45s interval), deduplication against recent messages. Catch-up and full backfill skip Cheogram number JIDs (`cheogramE164`, in `cheogram.go`) that have no Matrix room and were not seen on this connection (`mamEligible`, `roomPortalIDs`: one `GetAllWithMXID` query per cycle), so bulk-imported numbers do not cause polling or portal creation.

- **`pkg/connector/xep0363.go`** — XEP-0363 HTTP File Upload. Discovers upload service via common subdomains, requests upload slots, uploads via HTTP PUT.

- **`pkg/connector/xep0054.go`** — XEP-0054 vCard avatars for 1:1 contacts (`fetchVCardPhoto`, 1 MiB cap + `image/*` sniff via `validateAvatarBytes`, shared `applyGhostAvatar`). **`xep0054_muc.go`** / **`muc_presence.go`** — MUC occupant avatars: `parsePresence` (one token pass: XEP-0153 hash tri-state, item jid, status codes), in-memory occupant table (`noteMUCPresence`/`noteMUCGhost`, Serve goroutine, memory + non-blocking enqueue only), one worker per session (`startMUCAvatars`, paced occupant-JID vcard-temp get with real-JID fallback, `decideMUCAvatar`, MXC cache). When the 1:1 path applies a new avatar or removal for a bare JID (`doAvatarRefresh`), it calls `propagateContactAvatar` (memory only, any goroutine): occupants of this session whose room-reported `item jid` matches (case-insensitive) and that have a ghost get a no-IQ job applying the cached MXC (bypasses the 60 s interval; marks the advertised state handled so stale room presence cannot revert it). Contact rule (`applyContact`): when the room reports a real JID and the 1:1 contact ghost for that bare JID has a known avatar (`classifyContactGhost`: proven MXC, or fetched and known-none; read as a fresh DB row via `loadContactAvatar`, never the shared cached ghost), the contact avatar is authoritative for the occupant on every worker job (presence, first message, or an unknown-photo presence once per session): its MXC is applied or the avatar removed, no IQ, room-hash fetch skipped; otherwise the room-hash path runs. A room advertising a different state than the contact triggers `queueAvatarRefresh` for the contact (`maybeRefreshContact`: contact fetched over 10 min ago, max one per contact per 10 min and one per advertised state per 6 h; result flows back only via propagation). Occupants are only fetched after a ghost was seen this session (they spoke); the worker is the sole writer of MUC occupant avatars, so `getUserInfo` must keep returning a nil Avatar for them. No config toggle.

- **`pkg/connector/cheogram.go`** — Cheogram/JMP.chat integration. Phone number normalization (+1 default), short code handling, group MMS JID construction (comma-separated sorted numbers), sender extraction from body prefix.

### CardDAV contact import (untagged, all builds)

Phone numbers from a CardDAV address book are added to the roster as Cheogram contacts (`network.cheogram` required). Add-only, manual, read-only towards the server. Files in `pkg/connector/`:

- **`carddav_config.go`** — `CardDAVConfig` (`network.carddav`: `allow_insecure_http`, `allow_private_hosts`, `default_country_code`), example config, upgrader copies; the limits are constants (`cardDAVMaxContacts` 5000, `cardDAVMaxAddsPerRun` 500, `cardDAVAddsPerSecond` 5, `cardDAVMaxResponseBytes` 32 MiB).
- **`carddav_creds.go`** — `Extra["carddav"]` = `{v,url,username,password,book,books}` (plaintext password), saved through `setLoginExtraKey`.
- **`carddav_http.go`** — HTTP client seam `newCardDAVHTTPClient` (no proxy, no redirects followed, SSRF dialer `blockedCardDAVAddr`, TLS 1.2+), `normalizeCardDAVURL`, `sameSite` (eTLD+1 rule for credentials/redirects/hrefs), classified error kinds (`cardDAVError`; spans/logs record only the kind).
- **`carddav_dav.go`** — hand-rolled PROPFIND/REPORT with streaming, per-`<response>` tolerant decoding (bad entries counted, 507 = truncated flag, size cap errors instead of truncating).
- **`carddav_client.go`** — discovery (explicit book, principal/home-set, manual `/.well-known/carddav` hop), book selection (skips `z-app-generated*` and `collected`), `addressbook-query` with an `addressbook-multiget` fallback, per-book error isolation (`fetchAll`), `fetchContacts` (span `carddav.fetch`), `validateCardDAVLogin`.
- **`carddav_phone.go`**, **`carddav_cards.go`**, **`carddav_plan.go`** — pure core: TEL normalization (pre-pass + `mrtc-sip-bridge/pkg/phone`), vCard extraction (name chain, labels incl. sanitized custom `X-ABLabel`, vCard 2.1 salvage), planner (`planCardDAVSync`: dedupe, `Name (label)`, already-in-roster skip).
- **`carddav_sync.go`** — executor `applyPlan` (rate limit, 5-consecutive-failure abort, disconnect stop, `inRoster` re-check), per-login busy map and in-memory last-run summary, `cardDAVSyncJob` (detached; replies through a copied `commands.Event`, never the handler's).
- **`carddav_commands.go`**, **`carddav_prompt.go`** — `carddav-login|logout|status|sync` handlers and a local copy of `hostkit.PromptFields` (hostkit is tagged-only); **`carddav_roster.go`** — `xmppClient.rosterSnapshot()`.
- Shared untagged helpers used by both CardDAV and the sipcalls files: `metadata.go` (`setExtraKey`, `loginMeta`, `saveUserLogin`, `extraWriteMu`, `setLoginExtraKey`), `commands_login.go` (`lookupCommandLogin`, `checkJabberLogin`). All writers of `JabberLoginMeta.Extra` must take `extraWriteMu` (via `setLoginExtraKey` or directly); lock order `extraWriteMu` before `callsExt.mu`, never held across network I/O.
- Tests use a fake TLS CardDAV server (`carddav_fake_test.go`, generic and SOGo layouts). `TestCardDAVSmoke` runs against a real server when `CARDDAV_SMOKE_URL`, `CARDDAV_SMOKE_USER` and `CARDDAV_SMOKE_PASS` are set (e.g. a local Radicale container); otherwise it skips.

### Telemetry

- **`pkg/telemetry/`** — Optional OpenTelemetry tracing (`telemetry.go`: `Init`/`Shutdown`/env resolution; `span.go`: span and log-correlation helpers; `http.go`: HTTP transport wrapping; `env.go`: injectable getenv). Off unless an OTLP endpoint env var is set; when off nothing is installed and helpers are no-ops that return the input ctx unchanged. `Init` never fails the bridge (problems log one warning and leave tracing disabled). Conventions: goroutines never receive a live span (start their own span, or a linked root); leaf operations use child-only spans so uninstrumented paths do not create orphan roots; never record message bodies, captions, emoji, filenames or URLs (full JIDs are allowed); never block or do IQ round trips on the mellium Serve goroutine. Log correlation: root spans and `bridge.handle_remote_event` annotate the ctx logger (`zerolog.Ctx`) with `trace_id`/`span_id`/`dd.trace_id`/`dd.span_id`; our own lines under a span use `x.logFor(ctx)` (or `telemetry.Logger(ctx, cleanLogger)`), never `x.log` directly; never pass a logger derived from `zerolog.Ctx(ctx)` to `telemetry.Logger`.

### Data & Metadata

- **`pkg/connector/dbmeta.go`** — Database metadata types: `PortalMetadata` (last sync, MUC nick), `GhostMetadata` (profile fetch time), `MessageMetadata` (XMPP stanza ID, backfill source).

- **`pkg/connector/metadata.go`** — `JabberLoginMeta` (password, default MUC nick; `Server`/`Port` fields are reserved for a future per-account override and are not yet used).

- **`pkg/connector/roster_contacts.go`** — Named-roster snapshot and pure helpers (`phoneContacts`, `pageContacts`) behind the Dialpad widget Contacts tab (named Cheogram phone numbers only; sort and search rules documented in the file).

- **`pkg/connector/id.go`** — `EventSender` construction from JIDs.

- **`pkg/connector/capabilities.go`** — Declared capabilities: text, image/video/audio/file (50 MiB), captions, replies, reactions.

- **`pkg/connector/stub_client.go`** — Minimal `NetworkAPI` for logins without credentials (allows list/logout only).

## Bot Commands

All commands use the configured prefix (default `!jabber`):

| Command | Description |
|---------|-------------|
| `login password` | Log in with JID + password |
| `pm <jid-or-phone>` | Start a DM (Cheogram: phone numbers resolved automatically) |
| `join [<login>] <room@conference.domain>` | Join a MUC room |
| `leave [<login>] <room@conference.domain>` | Leave a MUC room |
| `sync-roster [<login>]` | Re-fetch roster, update display/room names |
| `add-to-roster [<login>] <jid> <name>` | Add or rename a roster contact (inside a one-to-one chat, `add-to-roster <name>` works like `save-contact`) |
| `save-contact [<login>] <name>` | Inside a one-to-one chat room: save (or rename) that chat's contact in the roster; the room name updates. Works in every build |
| `remove-from-roster [[<login>] <jid-or-phone>]` | Remove a contact from the roster; with no argument, inside a one-to-one chat, removes that chat's contact. Names fall back to the JID/number. Works in every build |
| `carddav-login` | Set up the CardDAV account (management room, no arguments; prompts for URL, username, password, optional address book; validates before saving; needs `network.cheogram`) |
| `carddav-sync` / `carddav-sync apply` | Preview / apply the import of CardDAV phone numbers into the roster (detached run, result as a later message) |
| `carddav-status` / `carddav-logout` | Show the CardDAV account and last sync / remove the account |
| `roster-only-calls on\|off\|status` | (`sipcalls` build) Ring for incoming SIP calls only from numbers in the roster; others get 480 plus a notice in the management room. Gates SIP calls only, not XEP-0353 call notifications |
| `search-roster [<login>] <query>` | Search roster by JID or name |
| `muc-nick [<login>] <room-jid> <nick>` | Set room-specific MUC nickname |
| `muc-nick-default [<login>] <nick>` | Set default MUC nickname for all rooms |

Account selection: with several Jabber logins, the commands above taking `[<login>]` accept the login ID (lower-cased JID) as an optional first argument (`CommandLogin` in `pkg/connector/command_login.go`, same convention as bridgev2 `getClientForStartingChat`). Only the sender's own logins match, case-insensitively; the argument is not consumed if it would leave fewer arguments than the command needs (so `join me2@x.org` still means the room and `remove-from-roster me2@x.org` the contact; `add-to-roster` takes a login only before a JID, so `add-to-roster me2@x.org Full Name` is the contact; to add your own other account to the roster write `add-to-roster me1@x.org me2@x.org Name`). Without it: the chat room's login inside a portal, else the default login (first by ID); outside a portal with several logins the success reply notes which account was used. carddav/sipcalls commands use `lookupCommandLogin` only (no explicit argument).

## Setup for Development / Testing

See `README.md` for full setup instructions including Docker. Quick summary:

```bash
# Generate example config (writes config.yaml directly; use -c - to print to stdout)
./jmpchat-matrix-bridge -e

# Edit config.yaml (set homeserver.address, homeserver.domain, database, appservice, etc.)

# Generate appservice registration (give to Synapse)
./jmpchat-matrix-bridge -g

# Run the bridge
./jmpchat-matrix-bridge
```

Default appservice port is **29318**.

## Docker

A `Dockerfile` and `entrypoint.sh` are provided. The entrypoint prints an example config to stdout when no config is mounted, making first-run setup easy:

```bash
docker build $(./build-args.sh) -t williamkray/jmpchat-matrix-bridge .   # build-args.sh passes git commit/tag/repo/build time
docker run --rm williamkray/jmpchat-matrix-bridge > config.yaml   # get example config
```

See `README.md` for full Docker usage with registration generation and volume mounts.

## Key Dependencies

- Go 1.26+ (per `go.mod`)
- `maunium.net/go/mautrix v0.32.0` — bridgev2 framework (Matrix protocol, appservice, DB schema)
- `go.mau.fi/util v0.11.0` — shared utilities
- `mellium.im/xmpp v0.22.0` — XMPP client library
- `mellium.im/sasl v0.3.2` — SASL authentication
- `mellium.im/xmlstream v0.15.4` — XML stream processing
- Database: PostgreSQL 10+ (`github.com/lib/pq`) or SQLite (`github.com/mattn/go-sqlite3`)
- Logging: `github.com/rs/zerolog`

## Configuration

`config.yaml` is generated by the binary. The Jabber-specific section (`network:`) maps to `JabberConfig` in `connector.go`:

- `resource_prefix` — XMPP resource prefix
- `cheogram` — Enable Cheogram phone number resolution (also via `JMPCHAT_BRIDGE_CHEOGRAM=1` env var; the pre-rename `MAUTRIX_JABBER_CHEOGRAM` is still accepted)

The bridge is not bound to any single XMPP server: the connection host is derived from the JID's domain at login (`dialXMPP` does A/AAAA lookup on the domain, port 5222). There is no server/port config setting. Note: `dialXMPP` does not yet do SRV lookup, so servers whose client-connect host differs from the JID domain aren't supported yet — see the SRV/override to-do in README.md.

Tracing is configured only through environment variables (no config-file keys), see `pkg/telemetry` and the README "Tracing (OpenTelemetry)" section. It is enabled iff `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set. Protocol comes from `OTEL_EXPORTER_OTLP_(TRACES_)PROTOCOL` and defaults to `http/protobuf` per the OTel spec (the code does not infer it from the port; always set it explicitly: `grpc` for :4317, `http/protobuf` for :4318). `OTEL_SDK_DISABLED=true` or `OTEL_TRACES_EXPORTER=none` disables it. `DD_SERVICE`/`DD_VERSION`/`DD_ENV` are fallbacks for resource attributes; `OTEL_SERVICE_NAME`/`OTEL_RESOURCE_ATTRIBUTES` override them.

CardDAV import is configured under `network.carddav` (`allow_insecure_http`, `allow_private_hosts`, `default_country_code`); see the CardDAV section in `README.md`.

Other relevant framework config sections: `bridge.permissions`, `matrix.delivery_receipts`, `backfill.enabled` / `backfill.max_initial_messages`, `encryption.*`.

`registration.yaml` is auto-generated and must be registered with the Matrix homeserver.

### Test Infrastructure

For local Docker testing, `docker-run/` (gitignored) contains a self-contained `config.yaml` + `registration.yaml` pair with a local SQLite database. `docker-registration.yaml` (gitignored) holds the matching registration used when generating the docker-run setup.

A local verification config also exists: `config.verify.yaml` / `registration.verify.yaml` (gitignored) with a SQLite database (`mautrix-jabber-verify.db`) for quick startup testing without a homeserver.

## Related Documents

- **[README.md](README.md#to-do-unimplemented-work)** — Canonical to-do list of pending work
- **[ROADMAP.md](ROADMAP.md)** — Phased production plan, implementation notes, known issues
- **[CODE_OPTIMIZATION.md](CODE_OPTIMIZATION.md)** — Code optimization backlog (duplication, config, structure)

**Encryption is deprioritized** — do not implement OMEMO or add XMPP crypto dependencies until core features are stable.

## SIP calls

Calls for Cheogram/JMP numbers (SIP -> MatrixRTC) are behind build tag `sipcalls`, in files named `pkg/connector/*sipcalls*.go` (plus `pkg/telemetry/backend_sipcalls*.go` and `cmd/jmpchat-matrix-bridge/variant_sipcalls.go`); untagged files only hold call-free extension seams (`extension.go`). `sipcalls_widgetfeatures.go` implements the Dialpad widget's optional host features (`hostkit.RosterOnlyPrefs`/`ChatOpener`/`ContactSaver`/`ContactLookup`, wired as `Deps.Features`): roster-only toggle, Message (reuses `ResolveCallRoom`, detached from the request, MAM backfill afterwards via `xmppClient.backfillChatAsync`), Save contact (the `save-contact` roster set) and the saved-name lookup; the widget and this file never log or trace contact names (the roster-fetch log line in `syncRoster` still logs roster names; `SetRosterItem` and the roster-push handler no longer do). The tag is included in the default Docker build/image (with `nolibopusfile`); calls stay off unless `network.calls.enabled: true`. The call core is the public module `github.com/williamkray/mrtc-sip-bridge`; the host contract and user docs are in its `docs/EMBEDDING.md`. Local dev builds with calls use `-tags "nocrypto sipcalls nolibopusfile"` (needs libopus dev headers and pkg-config).
