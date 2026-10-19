# mautrix-jabber production roadmap

Prioritized, step-by-step plan to make the bridge production-ready: security, scalability, native UX (media, reactions, quotes, mentions), MUC, and optional OMEMO. This is the **canonical planning document**; implementation notes and known issues are included below.

---

## Phase 1: Security foundation and Matrix E2BE (done)

- **1.1 Enable Matrix E2BE** — Build with libolm; config `encryption.allow`; document delete_keys/verification_levels.
- **1.2 Harden credential handling** — No JID/password in Info logs; redact errors; Debug-only identifiers with one-time warning; README production notes.

---

## Phase 2: Scalability, robustness, and native UX

- **2.1 Multi-user and connection hardening** *(done)*  
  Reconnection loop in LoadUserLogin/xmppClient with exponential backoff; optional connection limits; document Postgres and pool tuning for multi-user.

- **2.1b Roster isolation per login and per Matrix user** *(planned)*  
  Ensure roster commands (`roster-add`, `roster-sync`) and roster-derived data (ghost display names, DM room names) are strictly isolated per bridged Jabber ID (login) and per Matrix user. When multiple accounts are bridged—one Matrix user with multiple Jabber logins, or multiple Matrix users with different Jabber accounts—roster data from one login must not affect another. Fix any mixup behavior where roster or display names from one account leak or override another. Part of multi-user hardening.

- **2.2 IPv6 / dual-stack** *(done)*  
  Dial path supports IPv6: we try all resolved addresses with `tcp` (dual-stack) and prefer IPv6 when both A and AAAA exist (`sortIPsPreferIPv6`). Optional config for `tcp4`/`tcp6` can be added later if needed.

- **2.3 Media handling** *(done)*  
  Support non-text messages so behavior feels native on both sides.
  - **Outbound (Matrix → XMPP):** `HandleMatrixMessage` handles `m.image`, `m.video`, `m.audio`, `m.file`: downloads media from Matrix, uploads via XEP-0363 to sender's upload service (discovered via common subdomains: `upload`, `share`, `files`, `http_upload`), and sends via XEP-0066 OOB with URL fallback in body per XEP-0066 spec. Uses mellium's `mellium.im/xmpp/upload` package. Properly handles captions (distinguishes user captions from auto-generated filenames).
  - **Inbound (XMPP → Matrix):** Parses XEP-0066 OOB URLs from incoming messages, downloads media, uploads to Matrix, and sends as proper media messages (`m.image`/`m.file`/etc.) with `Content.URL`, `Content.File` (for encrypted rooms), `Content.FileName`, and `Content.Info` (MimeType, Size) set correctly. Falls back to single HTTPS URL in body detection. Filters out XEP-0085 chat state notifications (typing indicators).
  - **Status:** ✅ Complete — tested with images, videos (mp4), animated webp, PDFs, and other file types. Works in both directions with proper inline rendering.
  - Config and limits (size, types) for operators can be added when needed.

- **2.4 Reactions, quotes, and at-mentions**  
  Make bridged conversations feel native on both sides.
  - **Reactions:** *(basic done)* Map Matrix reactions (e.g. `m.reaction`) to XMPP (XEP-0444 or fallback to a short reply “reacted with …”); map XMPP reactions/replies back to Matrix reactions where supported. Single reaction per user per message works both ways; *future:* preserve multiple reactions (see [README](README.md)#to-do-unimplemented-work).
  - **Reaction target lookup:** Reactions in Jabber to XMPP-originated messages can fail to bridge. Connector has `GetMessageMetadata()` and deferred metadata update; follow-up in [README](README.md)#to-do-unimplemented-work.
  - **Quotes / replies:** *(done)* Reply chains preserved: Matrix `m.in_reply_to` ↔ XMPP reply/thread (e.g. `<reply>` XEP or stanza reference). Ensure reply metadata is stored and sent so both sides show “replying to X”.
  - **At-mentions:** *(deprioritized)* Map Matrix mentions (`@user:server`) to XMPP (e.g. mention by JID or nick in MUC); map XMPP mentions to Matrix; deferred to a later phase.

- **2.5 Contact roster and display names**  
  Use Jabber roster for ghost names and allow managing roster from Matrix.
  - **Display names:** When a JID is in the user's Jabber contact roster, set the Matrix ghost (bridged user) display name to the roster contact name so Matrix shows the same name as Jabber.
  - **Roster from Matrix:** *(done)* `!jabber roster-add <jid> <name>` adds or renames a contact; `!jabber roster-sync` re-fetches roster and refreshes display/room names in Matrix.
  - **Multi-login / roster isolation (2.1b):** When one Matrix account is logged into multiple Jabber accounts (or multiple Matrix users bridge different Jabber accounts), roster commands and roster-derived data must be strictly isolated per login and per Matrix user. See Phase 2.1b.

---

## Phase 3: Multi-user chat (MUC) and backfill

**Status:** MUC support is minimally functional and "enough" for now. Known issues: reactions to XMPP-originated messages often don't bridge to Matrix (see 2.4 reaction target lookup); room topic/description may be incomplete or wrong in some cases (see 3.5).

- **3.1 Portal model and MUC room type** — MUC portal key (full MUC JID); GetChatInfo for MUC (RoomTypeGroupDM, members); inbound groupchat detection and RemoteEventThatMayCreatePortal.
- **3.2 MUC join/leave and presence** — Join/leave from Matrix (!jabber join, ResolveIdentifier); MUC presence handling; optional leave from Matrix.
- **3.3 MUC message routing and identity** — Send/receive as MUC occupant; ghost identity for occupants; deduplication and ordering.

- **3.4 Backfill messages** *(working)* — Backfill and catch-up via MAM (XEP-0313); connector implements `FetchMessages`; backfill on view, on portal creation (inbound), on PM open, and periodic catch-up. Cheogram group MMS sender parsing applied. Future optimization: cache triggering message and append after backfill for correct ordering (see [Implementation notes](#implementation-notes-and-known-issues)).
- **Message Carbons (XEP-0280)** *(done)* — Carbons enabled on connect; `<sent>` / `<received>` copies are parsed (XEP-0297 forwarded stanza), bridged to Matrix, and deduplicated. Chat state notifications (no body) are skipped. Messages from other clients appear in Matrix when the server supports carbons; MAM catch-up remains fallback when carbons are not available or bridge was down.

- **3.5 Validate MUC room topic and description** *(low priority, follow-up)* — Confirm room topic and description are correctly pulled from disco#info (XEP-0128 muc#roominfo) in various situations (e.g. description-only, subject-only, both, different servers). Fix source or parsing if wrong; document behavior.

---

## Phase 4: Stronger security (OMEMO, optional)

- **4.1 OMEMO support on XMPP side** — XEP-0384; decrypt/encrypt in message and inbound paths; key storage; DM first, MUC later if desired.

---

## Order of implementation

1. **Phase 1** — E2BE then credential hardening (done).
2. **Phase 2** — Reconnection (2.1) *(done)*, roster isolation (2.1b) *(planned)*, IPv6 (2.2) *(done)*, media (2.3) *(done)*, reactions/quotes/mentions (2.4), contact roster and display names (2.5) *(done)*.
3. **Phase 3** — MUC portal (3.1), join/leave (3.2), message routing (3.3) *(minimal done)*; backfill (3.4) *(done)*. Follow-up 3.5 (room topic/description) low priority.
4. **Double puppeting** — Supported; see README. User's real Matrix account can be used via `double_puppet.secrets` and separate appservice registration.
5. **Phase 4** — OMEMO (4.1) after MUC or in parallel if E2EE on XMPP is prioritized.

**Next priorities:** MUC is in place and "enough" for now (known issues: reactions to XMPP-originated messages, room topic/description). Backfill (3.4) done via MAM. Roster isolation (2.1b) for multi-user hardening. Optional MUC polish (3.5) or Phase 2.1 hardening if desired. **Canonical task list:** see [README.md](README.md)#to-do-unimplemented-work.

---

## Implementation notes and known issues

Technical context for future work. See [README](README.md#to-do-unimplemented-work) for the canonical unimplemented task list.

### Backfill message ordering

**Current behavior:** When an inbound message creates a new portal room, backfill runs asynchronously. The triggering message is bridged first, then backfill history appears—so the newest message can appear above older history.

**Mitigation (implemented):** When backfill is enabled, **full backfill** runs (1) after each connect and (2) on `!jabber sync-roster` for all roster DMs. That pre-creates portals and fills history from MAM so new messages no longer create a room and then show history after; users can run sync-roster without logging out to trigger it on demand.

**Future optimization:** Cache the triggering message temporarily; when backfill completes, queue backfill items then the cached triggering message. Requires refactoring `maybeTriggerBackfillOnNewPortal` to accept and queue the triggering message after backfill, and callers to not queue it when the goroutine will.

### Multiple reactions per user per message

**Current behavior:** When a user adds more than one reaction to the same message, only the most recently sent reaction is shown on the other side. Consistent in both directions.

**Path forward:**

1. **Matrix → Jabber (XEP-0444):** Per XEP-0444, when updating reactions the client must send *all* active reactions in one `<reactions>` element. The bridge currently sends only the new emoji. Query existing reactions from the bridge database for this user + target message; in `HandleMatrixReaction`, collect all existing emojis, add the new one, and send the full set.
2. **Jabber → Matrix:** When Jabber sends multiple `<reaction>` children, we queue one event per emoji. The framework may deduplicate. Investigate why multiple queued events result in only one Matrix reaction and adjust deduplication or event-id handling.

### Reaction target not found (XMPP-originated messages)

**Observed:** Reaction in Jabber to another user's MUC message does not appear on Matrix. Logs: "Reaction target not found: no message with this XMPP ID in portal".

**Cause:** For XMPP-originated messages the framework may not set `Message.Metadata.XMPPMessageID` when persisting.

**Done in connector:** (1) `GetMessageMetadata()` returns `XMPPMessageID` so the framework can store it. (2) `scheduleStanzaIDMetadataUpdate()` runs after `ConvertMessage` returns: after a short delay we find the just-persisted message and call `br.DB.Message.Update` to set `Metadata.XMPPMessageID`. Reaction lookup uses 30-day window and metadata/ID matching. Follow-up: verify framework persists metadata or extend heuristic.

### Multi-login edge case / roster isolation

When one Matrix user is logged into multiple Jabber accounts, DMs with the same remote JID exist as separate portals. Roster names and ghost/room display can differ per login. Observed: mixup behavior when multiple accounts are bridged—roster commands or roster-derived data may not be strictly isolated per login and per Matrix user. See **2.1b Roster isolation per login and per Matrix user** in Phase 2.

### Server connection: A/AAAA only, no SRV lookup

`dialXMPP` derives the connection host from the JID's domain via a plain A/AAAA lookup on port 5222; it does not resolve `_xmpp-client._tcp.<domain>` SRV records, and the `JabberLoginMeta.Server`/`Port` override fields are reserved but not wired in. This means accounts on servers where the client-connect host differs from the JID domain (e.g. `user@example.com` connecting to `xmpp.example.com`) cannot connect. The bridge is otherwise fully server-independent (any account whose domain resolves directly works). See README to-do item 7 (context: closed GitHub issue #1).

### syncRoster replaces the roster cache after the fetch (known race)

`syncRoster` fetches the roster and then replaces the whole in-memory cache (`applyRosterItems(replace=true)`) with that snapshot. A roster push or `SetRosterItem` applied between the server answering the fetch and the replacement is overwritten until the next sync, so a just-added contact can briefly be missing (and, with roster-only calls on, its calls briefly rejected) or a just-removed one reappear. Not fixed: the window is small and the next push, `sync-roster` or reconnect repairs it. A fix would merge by a per-item version or buffer pushes received while a fetch is in flight.

### Dialpad widget: Message, Save contact and roster-only toggle (done, v0.3.0)

The Dialpad widget (room widget and PWA) can open a number's chat (`OpenChat`, via `ResolveCallRoom`, with the MAM backfill started afterwards), save a number to the roster under a name (same semantics as `save-contact`, overwrites an existing name), show "Saved as ..." in number rooms and toggle roster-only calls (#7). Implemented in `sipcalls_widgetfeatures.go` against mrtc-sip-bridge v0.2.0's optional host interfaces. The features need a ready login (registered SIP account), the same gate as calling. Known limitation: if the user has left an existing portal room, Message returns its room ID without re-inviting (as `pm` does).

---

## Future phases (documented for later)

- **MUC polish** — Room topic/description validation (3.5), multiple reactions per user. Backfill (3.4) done. See README to-do.

---

## Out of scope (unless explicitly requested)

- Relay mode.
- Adding or reordering phases without user direction.
