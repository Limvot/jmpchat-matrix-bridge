# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

mautrix-jabber is a Matrix-Jabber (XMPP) puppeting bridge written in Go, built on the **mautrix bridgev2** framework. It supports bidirectional DM and MUC bridging, media file transfers, reactions, replies, message carbons, MAM backfill, and Cheogram SMS/MMS integration.

## Build

Builds use Docker to ensure a repeatable environment with all C dependencies (CGO, libolm, sqlite-dev):

```bash
./build.sh
```

This runs `docker build` and extracts the binary. For a local build without encryption support (requires Go 1.25+, CGO, sqlite-dev):

```bash
go build -tags nocrypto -ldflags "-s -w" -o mautrix-jabber ./cmd/mautrix-jabber
```

The production build includes libolm for Matrix E2EE. No test or lint configuration exists yet.

## Architecture

All bridge logic lives in `pkg/connector/`. The bridgev2 framework handles Matrix protocol, appservice lifecycle, config, and database.

### Entry Point

- **`cmd/mautrix-jabber/main.go`** — Bootstrap. Creates `BridgeMain` with `JabberConnector`, registers custom bot commands (`sync-roster`, `add-to-roster`, `search-roster`, `join`, `leave`, `muc-nick`, `muc-nick-default`), injects decoded network config via `PostInit`.

### Connector Core

- **`pkg/connector/connector.go`** — `JabberConnector` (implements `bridgev2.NetworkConnector`). Login flow (JID+password via `jabberLoginProcess`), XMPP session dial (`dialXMPP` with DNS→TCP→TLS/SASL), `LoadUserLogin` (spawns `xmppClient` or `stubJabberClient`). Also defines helper interfaces (`RosterSyncer`, `RosterManager`, `MUCJoiner`) and MUC nick persistence functions.

- **`pkg/connector/xmpp_client.go`** — `xmppClient` (implements `bridgev2.NetworkAPI` and related interfaces). The largest file (~1800 lines). Handles: reconnect loop with exponential backoff, roster sync, MUC join/leave/nick, `HandleMatrixMessage` (text, media, replies, reactions → XMPP), `ResolveIdentifier` (JID/phone → portal key), `GetChatInfo`/`GetUserInfo`, portal type detection (`portalIsMUC` via disco#info or heuristic).

### Message Handling

- **`pkg/connector/xmpp_inbound.go`** — Inbound message routing. Dispatches incoming XMPP stanzas to DM or MUC handlers, extracts sender, determines portal key, queues `RemoteEvent` for the framework. Handles self-echo filtering for MUC.

- **`pkg/connector/xmpp_message.go`** — Message conversion. Parses XMPP message content (body text, XEP-0066 OOB media URLs, XEP-0461 replies, XEP-0444 reactions, chat state notifications). Builds `bridgev2.ConvertedMessagePart` for Matrix. Handles deferred stanza ID metadata updates.

### Protocol Extensions

- **`pkg/connector/xmpp_carbons.go`** — XEP-0280 Message Carbons. Enables on connect, parses `<sent>`/`<received>` forwarded stanzas, queues as remote events.

- **`pkg/connector/xmpp_mam.go`** — XEP-0313 Message Archive Management. Backfill-on-view (`FetchMessages`), periodic catch-up loop (~45s interval), deduplication against recent messages.

- **`pkg/connector/xep0363.go`** — XEP-0363 HTTP File Upload. Discovers upload service via common subdomains, requests upload slots, uploads via HTTP PUT.

- **`pkg/connector/cheogram.go`** — Cheogram/JMP.chat integration. Phone number normalization (+1 default), short code handling, group MMS JID construction (comma-separated sorted numbers), sender extraction from body prefix.

### Data & Metadata

- **`pkg/connector/dbmeta.go`** — Database metadata types: `PortalMetadata` (last sync, MUC nick), `GhostMetadata` (profile fetch time), `MessageMetadata` (XMPP stanza ID, backfill source).

- **`pkg/connector/metadata.go`** — `JabberLoginMeta` (password, default MUC nick; `Server`/`Port` fields are reserved for a future per-account override and are not yet used).

- **`pkg/connector/id.go`** — `EventSender` construction from JIDs.

- **`pkg/connector/capabilities.go`** — Declared capabilities: text, image/video/audio/file (50 MiB), captions, replies, reactions.

- **`pkg/connector/stub_client.go`** — Minimal `NetworkAPI` for logins without credentials (allows list/logout only).

## Bot Commands

All commands use the configured prefix (default `!jabber`):

| Command | Description |
|---------|-------------|
| `login password` | Log in with JID + password |
| `pm <jid-or-phone>` | Start a DM (Cheogram: phone numbers resolved automatically) |
| `join <room@conference.domain>` | Join a MUC room |
| `leave <room@conference.domain>` | Leave a MUC room |
| `sync-roster` | Re-fetch roster, update display/room names |
| `add-to-roster <jid> <name>` | Add or rename a roster contact |
| `search-roster <query>` | Search roster by JID or name |
| `muc-nick <room-jid> <nick>` | Set room-specific MUC nickname |
| `muc-nick-default <nick>` | Set default MUC nickname for all rooms |

## Setup for Development / Testing

See `README.md` for full setup instructions including Docker. Quick summary:

```bash
# Generate example config (writes config.yaml directly; use -c - to print to stdout)
./mautrix-jabber -e

# Edit config.yaml (set homeserver.address, homeserver.domain, database, appservice, etc.)

# Generate appservice registration (give to Synapse)
./mautrix-jabber -g

# Run the bridge
./mautrix-jabber
```

Default appservice port is **29318**.

## Docker

A `Dockerfile` and `entrypoint.sh` are provided. The entrypoint prints an example config to stdout when no config is mounted, making first-run setup easy:

```bash
docker build -t mautrix-jabber .
docker run --rm mautrix-jabber > config.yaml   # get example config
```

See `README.md` for full Docker usage with registration generation and volume mounts.

## Key Dependencies

- Go 1.25+ (per `go.mod`)
- `maunium.net/go/mautrix v0.27.0` — bridgev2 framework (Matrix protocol, appservice, DB schema)
- `go.mau.fi/util v0.9.8` — shared utilities
- `mellium.im/xmpp v0.22.0` — XMPP client library
- `mellium.im/sasl v0.3.2` — SASL authentication
- `mellium.im/xmlstream v0.15.4` — XML stream processing
- Database: PostgreSQL 10+ (`github.com/lib/pq`) or SQLite (`github.com/mattn/go-sqlite3`)
- Logging: `github.com/rs/zerolog`

## Configuration

`config.yaml` is generated by the binary. The Jabber-specific section (`network:`) maps to `JabberConfig` in `connector.go`:

- `resource_prefix` — XMPP resource prefix
- `cheogram` — Enable Cheogram phone number resolution (also via `MAUTRIX_JABBER_CHEOGRAM=1` env var)

The bridge is not bound to any single XMPP server: the connection host is derived from the JID's domain at login (`dialXMPP` does A/AAAA lookup on the domain, port 5222). There is no server/port config setting. Note: `dialXMPP` does not yet do SRV lookup, so servers whose client-connect host differs from the JID domain aren't supported yet — see the SRV/override to-do in README.md.

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
