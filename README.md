# mautrix-jabber

A Matrix–Jabber (XMPP) puppeting bridge. Use your Matrix account to send and receive messages with Jabber users and join Jabber MUCs, with double-puppet support. Primary use case is for bridging JMP.chat (Cheogram) messages to matrix, for this reason PM handling is much more thorough than multi-user chat (MUC) capabilities. If using this for more broad Jabber/XMPP use cases, you may be better served by a different project.

## Prerequisites

- Go 1.22+ (for building)
- PostgreSQL 10+ or SQLite (for the bridge database)
- A Matrix homeserver (e.g. Synapse) where you can register an appservice

## Build

Default build includes **Matrix end-to-bridge encryption (E2BE)** and requires CGO and libolm:

```bash
./build.sh
```

Or with Go (libolm dev headers must be installed):

```bash
CGO_ENABLED=1 go build -ldflags "-s -w" -o mautrix-jabber ./cmd/mautrix-jabber
```

To build without encryption support (no libolm): `go build -tags nocrypto -ldflags "-s -w" -o mautrix-jabber ./cmd/mautrix-jabber`. The Docker image is built with E2BE support.

## Setup

The bridge follows the same setup flow as other mautrix Go bridges: generate an example config, edit it, generate the appservice registration, register on the homeserver, then run.

### 1. Example config

Generate `config.yaml` from the built-in example:

```bash
./mautrix-jabber -e
```

This writes `config.yaml` (or use `-c path` to choose a file). To print the example to stdout instead of writing a file, use `-c -`:

```bash
./mautrix-jabber -e -c -
```

### 2. Edit config

In `config.yaml` set at least:

- **`database`**  
  - PostgreSQL: `type: postgres` and a real `uri` (e.g. `postgres://user:password@host/db?sslmode=disable`).  
  - SQLite (e.g. for local testing): `type: sqlite3-fk-wal` and `uri: file:mautrix-jabber.db?_txlock=immediate`.

- **`homeserver.address`** – URL the bridge uses to reach the homeserver (e.g. `http://localhost:8008`).

- **`homeserver.domain`** – Server name (e.g. `example.com`). Must not be `example.com` if you want to generate a registration (the `-g` step refuses it as a safeguard).

- **`appservice`** – `address`, `hostname`, `port` so the homeserver can reach the bridge. The rest can stay as generated.

**Cheogram (SMS/MMS):** Set `network.cheogram: true` to resolve phone numbers and short codes to Cheogram JIDs (e.g. `!jabber pm 2121112222` → +12121112222@cheogram.com). Fallback: `MAUTRIX_JABBER_CHEOGRAM=true` if connector config is not available.

  When Cheogram is enabled, **group MMS** is also supported. Cheogram represents group MMS conversations as a single JID whose local part is a comma-separated list of phone numbers (e.g. `+12121112222,+12023334444@cheogram.com`). The bridge treats that as one portal but creates a separate Matrix ghost for each number, so display names and roster resolution apply per participant. Outbound: `!jabber pm 212-111-2222, 2023334444, 18188675309` normalizes and sorts the numbers, then opens a DM with the combined JID. Inbound: messages from that JID are attributed to the correct sender (from a `<xmpp:JID>` body prefix when present, or the first number as fallback), and that prefix is stripped from the bridged message body.

  **Starting a DM:** Use `!jabber pm <jid>` or `!jabber start-chat <jid>` (they are the same; `pm` is an alias). With multiple Jabber accounts linked, add an optional login JID first to choose which account to use: `!jabber pm <login-jid> <contact-jid>` (e.g. `!jabber pm alice@jabber.org friend@example.com`).

### 3. Generate registration

Generate the appservice registration from your config (required for the homeserver to accept the bridge):

```bash
./mautrix-jabber -g
```

Or with custom paths:

```bash
./mautrix-jabber -g -c config.yaml -r registration.yaml
```

This:

- Writes `registration.yaml` (or the path from `-r`).
- Updates `config.yaml` with the new `as_token` and `hs_token`.

**`registration.yaml`** will look like:

- `id` – appservice id (from `appservice.id`).
- `url` – appservice URL (from `appservice.address`).
- `as_token`, `hs_token` – generated secrets.
- `sender_localpart` – bridge bot user.
- `namespaces.users` – regexes for `@jabberbot` and `@jabber_.*` on your `homeserver.domain`.

Ensure `url` matches the address where the bridge will listen (e.g. `http://localhost:29318` if that is your `appservice.address`).

### 4. Register on the homeserver

Add the registration file to Synapse's `homeserver.yaml`:

```yaml
app_service_config_files:
  - /path/to/registration.yaml
```

**If using end-to-bridge encryption (E2BE):** You must also add experimental features to `homeserver.yaml` and add MSC flags to the registration file (see [End-to-bridge encryption (E2BE)](#optional-end-to-bridge-encryption-e2be) below) before restarting.

Restart the homeserver. It should start without error; if it does not, check `registration.yaml` for valid YAML and correct `url`/`namespaces`.

### 5. Run the bridge

```bash
./mautrix-jabber
```

Or with custom paths:

```bash
./mautrix-jabber -c config.yaml -r registration.yaml
```

The bridge will listen on `appservice.hostname` and `appservice.port`. When the homeserver sends appservice transactions to `appservice.address`, the bridge should handle them without crashing.

---

## Optional: End-to-bridge encryption (E2BE)

To use **encrypted Matrix rooms** and hide messages from the homeserver. If using this with JMP.chat/Cheogram, remember that ALL SMS OVER THE CHEOGRAM BRIDGE AND PSTN IS PLAINTEXT, THIS DOES NOT MAKE YOUR CONVERSATIONS PRIVATE! This is recommended to remain disabled if you run your own matrix server for performance benefits.

To enable, configure encryption in three places: the bridge config, Synapse's `homeserver.yaml`, and the appservice registration file.

#### 1. Bridge configuration

Enable encryption in `config.yaml`:

```yaml
encryption:
  allow: true
  default: true   # enable encryption in new portal rooms
  require: false  # set true to drop unencrypted messages
  # Required: use appservice mode (MSC3202) instead of /sync
  appservice: true
  # Required: use MSC4190 for device management (required with MSC3861/MAS)
  msc4190: true
  # Recommended: enable self-signing for cross-signing support
  self_sign: true
  # Recommended: enable key sharing for better UX
  allow_key_sharing: true
  # Required: 32-byte key for encrypting keys in database (use "generate" for auto-generation)
  pickle_key: "generate"  # or a 32-byte key; do not use default in production
  # Recommended: trust on first use for better security
  verification_levels:
    receive: cross-signed-tofu
    send: cross-signed-tofu
    share: cross-signed-tofu
```

**Important:** `encryption.appservice: true` is **required** when using `encryption.msc4190: true`. Without it, Synapse will reject `/sync` requests from appservice users.

#### 2. Synapse configuration

Add the following experimental features to your Synapse `homeserver.yaml`:

```yaml
experimental_features:
  # Required: Enable MSC3202 device masquerading (for older Synapse <1.141)
  msc3202_device_masquerading: true
  # Required: Enable MSC2409 to-device messages via appservice transactions
  msc2409_to_device_messages_enabled: true
  # Required: Enable MSC3202 transaction extensions for ephemeral events
  msc3202_transaction_extensions: true
```

**Note:** For Synapse 1.141+, `msc3202_device_masquerading` may not be required, but the other two flags are still needed for appservice mode encryption.

#### 3. Appservice registration file

Add the following fields to your `registration.yaml`:

```yaml
# Required: Enable MSC2409 ephemeral events (to-device messages) via appservice transactions
de.sorunome.msc2409.push_ephemeral: true
# Required: Enable MSC4190 device management
io.element.msc4190: true
# Required: Enable MSC3202 device masquerading and transaction extensions
org.matrix.msc3202: true
```

**Important:** Use `de.sorunome.msc2409.push_ephemeral` (not `ephemeral_events`). Synapse checks for this specific key.

After updating the registration file, restart Synapse to load the new configuration.

#### 4. Expected behavior

Once configured correctly:

- Messages from Jabber will be encrypted when sent to Matrix rooms with encryption enabled
- The bridge bot's device (`@jabberbot:your.domain`) will be used to encrypt messages sent by ghost users
- You may see a client warning: **"the sender of the event does not match the owner of the device that sent it"**

This warning is **expected and harmless** with MSC3202 device masquerading. The bridge bot's device encrypts messages on behalf of ghost users, which causes clients to show this verification notice. Messages are encrypted and decrypted correctly. To suppress the warning, verify the bridge bot's device (`@jabberbot:your.domain`, device ID shown in logs) in your Matrix client.

#### 5. Troubleshooting E2BE

**Issue:** Bridge logs show "no group session created" or "Didn't get session, giving up trying to decrypt event"

- **Check:** Verify `de.sorunome.msc2409.push_ephemeral: true` is in registration (not `ephemeral_events`)
- **Check:** Verify `msc2409_to_device_messages_enabled: true` and `msc3202_transaction_extensions: true` are in Synapse `experimental_features`
- **Check:** Verify `encryption.appservice: true` in bridge config
- **Check:** Restart Synapse after registration changes

**Issue:** Synapse returns `NotImplementedError` when bridge tries to use `/sync`

- **Fix:** Set `encryption.appservice: true` in bridge config (required with MSC4190)

**Issue:** Keys are not being shared/received

- **Check:** Ensure `allow_key_sharing: true` in bridge config
- **Check:** Verify ephemeral events are enabled in registration and Synapse config (see above)
- **Check:** Bridge logs should show "Received inbound group session" when keys arrive

For more details, see [End-to-bridge encryption](https://docs.mau.fi/bridges/general/end-to-bridge-encryption.html).

## Production deployment

- **Secrets**: Prefer `env_config_prefix` in config so tokens and the database URI come from environment variables instead of the config file. Keep config files out of version control.
- **Database**: For multiple users, use PostgreSQL (`database.type: postgres`) with a restricted DB user and `max_open_conns` tuned for your deployment.
- **Process**: Run the bridge with minimal OS permissions (e.g. dedicated user, read-only config mount). Login passwords are stored in the bridge DB (JSON); protect the database and its volume with filesystem or DB ACLs.

## Docker

Build the image:

```bash
docker build -t mautrix-jabber .
```

**First run (no config):** To create your initial config, run the container with `-e -c -` so it prints the example config to stdout (the default CMD passes config paths, so you must override with these flags):

```bash
docker run --rm mautrix-jabber -e -c - > config.yaml
```

Edit `config.yaml` (database, `homeserver.address`, `homeserver.domain`, `appservice`, `bridge.permissions`). Then generate the registration. The bridge updates the config file when generating tokens, so use a **directory mount** (not single-file mounts) to avoid "device or resource busy" on some systems:

```bash
mkdir -p ./bridge-conf && cp config.yaml ./bridge-conf/
docker run --rm -v "$(pwd)/bridge-conf:/app/conf:rw" \
  mautrix-jabber -c /app/conf/config.yaml -r /app/conf/registration.yaml -g
cp ./bridge-conf/registration.yaml ./
cp ./bridge-conf/config.yaml ./
```

Add the registration to your homeserver, then run the bridge with config and registration mounted (read-only is fine) and a volume for the database if using SQLite:

```bash
docker run -d --name mautrix-jabber \
  -v "$(pwd)/config.yaml:/app/config.yaml:ro" \
  -v "$(pwd)/registration.yaml:/app/registration.yaml:ro" \
  -v bridge-data:/app/data \
  -p 29318:29318 \
  mautrix-jabber
```

Use `appservice.hostname: 0.0.0.0` in config so the bridge listens on all interfaces. For SQLite, set `database.uri` to `file:/app/data/mautrix-jabber.db?_txlock=immediate` and mount a volume at `/app/data`. Postgres is also supported.

## Tracing (OpenTelemetry)

Tracing is optional and **off by default**. It is enabled purely by environment variables; there is no config-file setting. When no endpoint is set, nothing is installed (no exporter, goroutines or sockets), so there is no overhead. Export problems are logged as rate-limited warnings and never stop or block the bridge.

**Enabling.** Tracing turns on only when `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is non-empty. Always include the `http://` or `https://` scheme. These standard variables are read:

| Variable | Notes |
|----------|-------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Required to enable. For `http/protobuf`, the base variable gets `/v1/traces` appended; the `TRACES` variant is used as-is. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` / `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` | `grpc` or `http/protobuf`. **Set this explicitly.** Per the OTel spec the default is `http/protobuf`, and the bridge does not guess from the port. Any other value (e.g. `http/json`) logs a warning and disables tracing. |
| `OTEL_EXPORTER_OTLP_HEADERS`, `_TIMEOUT`, `_COMPRESSION`, `_CERTIFICATE`, `_INSECURE`, ... | Handled by the OTLP exporters themselves (also in `TRACES_` variants). |
| `OTEL_SDK_DISABLED=true` | Disable tracing without removing the rest of the config. An unparseable value logs a warning and is treated as false. |
| `OTEL_TRACES_EXPORTER` | Only `otlp` (or unset) is supported; `none` disables; other values warn and disable. Setting `otlp` alone does not enable tracing. |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | Standard sampling, e.g. `parentbased_traceidratio` / `0.25`. |
| `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | Standard resource settings; these override the `DD_*` fallbacks below. |
| `OTEL_BSP_SCHEDULE_DELAY`, other `OTEL_BSP_*` | Batch export tuning, e.g. `1000` (ms) for faster delivery while testing. |

Resource defaults: `service.name` is `mautrix-jabber` and `service.version` is the bridge version. `DD_SERVICE`, `DD_VERSION` and `DD_ENV` (sets both `deployment.environment.name` and `deployment.environment`) are used as fallbacks, and the `OTEL_*` variables above take precedence over them.

**Match the protocol to the port.** Port 4317 is gRPC and port 4318 is HTTP. If the configured protocol and port disagree (for example `http/protobuf` against `:4317`), the bridge logs a startup warning and you will see export errors in the log. On success it logs one `OpenTelemetry tracing enabled` line with the protocol, `scheme://host:port` and sampler.

### Generic OpenTelemetry collector

```bash
# gRPC (collector port 4317)
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
OTEL_EXPORTER_OTLP_PROTOCOL=grpc

# or HTTP (collector port 4318)
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf

OTEL_SERVICE_NAME=mautrix-jabber
OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=prod
```

### Datadog Agent (OTLP ingest)

Enable OTLP ingest on the Agent (APM must be enabled, which is the default; set `DD_APM_ENABLED=true` to be explicit):

```bash
DD_API_KEY=<your key>
DD_SITE=datadoghq.com
DD_APM_ENABLED=true
DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_GRPC_ENDPOINT=0.0.0.0:4317
DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT=0.0.0.0:4318
```

Then point the bridge at it (`DD_AGENT_HOST` alone does nothing):

```bash
# gRPC
OTEL_EXPORTER_OTLP_ENDPOINT=http://datadog-agent:4317
OTEL_EXPORTER_OTLP_PROTOCOL=grpc

# or HTTP
OTEL_EXPORTER_OTLP_ENDPOINT=http://datadog-agent:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf

DD_SERVICE=mautrix-jabber   # optional
DD_ENV=prod                 # optional
DD_VERSION=0.1.8            # optional
```

### docker-compose example (bridge + Datadog Agent)

```yaml
services:
  mautrix-jabber:
    image: williamkray/mautrix-jabber
    volumes:
      - ./config.yaml:/app/config.yaml:ro
      - ./registration.yaml:/app/registration.yaml:ro
      - bridge-data:/app/data
    environment:
      OTEL_EXPORTER_OTLP_ENDPOINT: http://datadog-agent:4317
      OTEL_EXPORTER_OTLP_PROTOCOL: grpc
      DD_SERVICE: mautrix-jabber
      DD_ENV: prod
    depends_on: [datadog-agent]

  datadog-agent:
    image: gcr.io/datadoghq/agent:7
    environment:
      DD_API_KEY: ${DD_API_KEY}
      DD_SITE: datadoghq.com
      DD_APM_ENABLED: "true"
      DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_GRPC_ENDPOINT: 0.0.0.0:4317
      DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT: 0.0.0.0:4318

volumes:
  bridge-data:
```

To use a plain collector instead, swap the second service for `otel/opentelemetry-collector-contrib` with an `otlp` receiver (grpc and http) and your exporter of choice, and keep the endpoint/protocol pair above consistent with the port.

### What is traced

Spans cover Matrix-to-XMPP handlers (messages, reactions, viewing a chat, identifier resolution, chat/user info, MUC join/leave), inbound XMPP stanzas (messages, carbons, MUC, Jingle notices), delivery to the bridge framework (`bridge.handle_remote_event` and message conversion share one trace with the inbound stanza), XEP-0363 uploads, OOB downloads, MAM backfill and catch-up, roster sync, avatar refresh, connect/dial, login, bot commands, and outbound Matrix HTTP calls made while handling traced work (shown as `METHOD /_matrix/...` with ids replaced by `*`).

A `bridge.start` span is emitted once at startup (handy as a smoke test). Background work (roster sync and full backfill after connect, MAM catch-up, backfill triggered by a new portal or a sent message, avatar refresh, roster pushes) starts its own trace, linked to the triggering trace where there is one. `xmpp.session` is recorded when a connection ends and covers its lifetime. When roster contacts or known portals exist, MAM catch-up produces one `mam.catchup.cycle` span about every 45 seconds; per-portal `mam.catchup.portal` spans appear only when messages were caught up or a query failed. Framework calls such as chat/user info lookups appear as small standalone traces when they happen outside a traced event.

### Privacy

Trace attributes include **full JIDs** (including Cheogram phone-number JIDs), XMPP stanza ids, Matrix room and event ids, timings, file sizes, MIME types and error messages. Message bodies, captions, reaction emoji, filenames and URLs are **never** recorded, and URLs inside error messages are reduced to scheme and host. Anyone with access to your trace backend can still see conversation metadata (who talks to whom, and when), so restrict access accordingly.

### Log correlation

Log lines emitted inside a sampled, traced operation carry four fields, each once:

- `trace_id` (32 lowercase hex characters) and `span_id` (16 lowercase hex characters): the OpenTelemetry convention.
- `dd.trace_id` and `dd.span_id`: decimal strings. The trace id is the low 64 bits. This is the Datadog convention.

The fields appear only on lines logged inside traced work. Other lines, and all lines when tracing is off, have no trace fields and are otherwise unchanged.

Correlation needs JSON logs on stdout:

```yaml
logging:
  writers:
    - type: stdout
      format: json
```

Datadog recognises both pairs by default (Logs > Pipelines > Preprocessing for JSON logs), so no pipeline changes should be needed. For the Logs tab service filter to match the traces, set the container label `com.datadoghq.tags.service` to the same value as `DD_SERVICE` or `OTEL_SERVICE_NAME`.

Expected in a correlated trace: XMPP message handling (text only, OOB URL, MUC, carbons, reactions), the framework's per-event lines, Matrix message handler debug lines, upload lines, and Matrix HTTP client lines made inside a handler.

Not expected: XMPP stanza receipt before mux dispatch, Serve start and stop, presence, reconnect loop, the "Roster push applied" line (the roster refresh it triggers is correlated), lines logged before a span starts, and queue-level framework errors.

**Fallback.** If the Logs side panel shows the fields but no link to the trace, go to Logs > Pipelines > Preprocessing for JSON logs and set the Trace Id attributes to only `trace_id` and the Span Id attributes to only `span_id`. Wait a few minutes, send a new message, and check again. If it still fails, try only `dd.trace_id` and `dd.span_id`.

## Verifying

1. **Registration** – `registration.yaml` is valid YAML and has `id`, `url`, `as_token`, `hs_token`, `sender_localpart`, and `namespaces.users` with `exclusive: true` and regexes that match your `username_template` (e.g. `@jabber_.*`). If using E2BE, verify `de.sorunome.msc2409.push_ephemeral: true`, `io.element.msc4190: true`, and `org.matrix.msc3202: true` are present.

2. **Synapse** – After adding the registration and restarting, Synapse starts and does not reject the file. If using E2BE, verify experimental features (`msc2409_to_device_messages_enabled`, `msc3202_transaction_extensions`) are enabled in `homeserver.yaml`.

3. **Bridge** – `./mautrix-jabber` starts, binds to the appservice port, and does not panic when it receives `/transactions` from the homeserver. If using E2BE, check logs for "End-to-bridge encryption is in appservice mode" and verify no `NotImplementedError` from Synapse.

**Local run (no Synapse):** use a config with `database.type: sqlite3-fk-wal` and `database.uri: file:mautrix-jabber-verify.db?_txlock=immediate`, `homeserver.address` and `homeserver.domain` set, and `bridge.permissions` including your domain. Generate registration with `-g -c config.verify.yaml -r registration.verify.yaml`, then run `./mautrix-jabber -c config.verify.yaml -r registration.verify.yaml`. The bridge will start, listen on the appservice port, and retry the homeserver (connection refused if Synapse is not running). Full `/transactions` handling can only be checked with a real homeserver.

## Troubleshooting

### Inbound messages not appearing in Matrix

- The bridge handles both **normal** and **chat** message types; many servers (e.g. Snikket) use `chat` for 1:1. If you still see no messages, check logs for `"XMPP message received (text only), queued for bridge"` and `"XMPP message receiver (Serve) started"`.

### Same JID logged in from bridge and another client

When the same Jabber account is used by the bridge and another app (e.g. Gajim, Conversations, Movim), messages from that other client should appear in Matrix via **Message Carbons (XEP-0280)** (real-time) and **MAM catch-up** (~45s polling). Backfill on room open is also supported. If messages are missing: check logs for "Carbons enable failed" (some servers reject carbons); ensure your server supports MAM and archives messages.

## Status

- **DM bridging** – JID+password login, 1:1 chats, portal creation on first message, identifier resolution (`!jabber pm` / `!jabber start-chat` with optional login JID for multi-account).
- **E2BE** – Build includes libolm; enable via `encryption.allow` and related config.
- **Implemented** – Reconnection with backoff, IPv6/dual-stack, media (Matrix↔XMPP), reactions (single per user), quotes/replies, contact roster and display names (`!jabber roster-add`, `!jabber roster-sync`), MUC (minimal: join/leave, message routing), MAM (XEP-0313) backfill and periodic catch-up (~45s), Message Carbons (XEP-0280). When backfill is enabled, **full backfill** runs for roster DMs after each connect and on `!jabber sync-roster` (so portals get history before new messages; sync-roster runs it without re-login). See [ROADMAP.md](ROADMAP.md) for phase summary.
- **Planned** – Unimplemented work is listed in [To-do](#to-do-unimplemented-work) below.

### Double puppeting

Automatic double puppeting lets the bridge use the user's real Matrix account (so messages from other clients are bridged as the user, not the ghost). It requires a **separate** appservice registration on the homeserver (e.g. `doublepuppet-registration.yaml`) with `url` null and a generated `as_token`. The bridge does not load that file; you must set the **same** token in the bridge config under `double_puppet.secrets` for your homeserver domain, in the form `"as_token:<token>"`. See [Double puppeting - mautrix-bridges](https://docs.mau.fi/bridges/general/double-puppeting.html).

## To-do (unimplemented work)

Canonical list of pending work; completed items have been removed. Order is approximate (next focus first, then phase order). For phased plan and implementation notes, see [ROADMAP.md](ROADMAP.md).

1. **Reaction target for XMPP-originated messages** — Reactions in Jabber to another user’s XMPP-originated message (e.g. in MUC) sometimes do not bridge to Matrix. Connector implements `GetMessageMetadata()` and deferred stanza-ID metadata update; if the framework does not persist `XMPPMessageID`, reaction lookup can still fail. Follow-up: verify framework use of metadata or extend heuristic.
2. **Multiple reactions per user per message** — Today only one reaction per user per message is shown on the other side (Matrix↔Jabber). To preserve multiple: Matrix→Jabber must send full reaction set in one XEP-0444 update (query existing from DB); Jabber→Matrix may need deduplication/event-id handling so each emoji is accepted.
3. **MUC room topic/description (3.5)** — Validate that room topic and description are correctly read from disco#info (XEP-0128 muc#roominfo) in all cases; fix parsing or source if wrong. Low priority.
4. **At-mentions (2.4)** — Map Matrix mentions (`@user:server`) to XMPP (e.g. JID or MUC nick) and XMPP mentions to Matrix. Deprioritized; deferred.
5. **OMEMO (4.1)** — XEP-0384; decrypt/encrypt on XMPP side in message and inbound paths; key storage. DM first; MUC later if desired.
6. **Roster isolation (2.1b, multi-user hardening)** — Ensure roster commands (`roster-add`, `roster-sync`) and roster-derived data (ghost display names, DM room names) are strictly isolated per bridged Jabber ID and per Matrix user. When multiple accounts are bridged, roster data from one login must not affect another. Fix mixup behavior where roster or display names from one account leak or override another.
7. **SRV lookup / per-account server override** — `dialXMPP` only does an A/AAAA lookup on the JID's domain and connects on port 5222; it does not resolve `_xmpp-client._tcp.<domain>` SRV records. Accounts on servers where the client-connect host differs from the JID domain (a common setup) can't connect. Add SRV lookup and wire up the reserved `JabberLoginMeta.Server`/`Port` override fields. Revisit if more connection-failure issues are reported (context: closed GitHub issue #1).

## Contributing

Code optimization recommendations (duplication, library overlap, configuration, structure, clarity) are tracked in [CODE_OPTIMIZATION.md](CODE_OPTIMIZATION.md).

## Links

- [Bridge setup (Go)](https://docs.mau.fi/bridges/go/setup.html)
- [Registering appservices](https://docs.mau.fi/bridges/general/registering-appservices.html)

### Alternative projects (inspiration and reference)

- **[Koishi](https://github.com/jjj333-p/koishi)** — XMPP ↔ Matrix bridge in Python; useful for feature and protocol ideas (e.g. puppeting, media, reply bridging, read receipts) even though the stack differs from this bridge (Go, mautrix bridgev2, mellium).
