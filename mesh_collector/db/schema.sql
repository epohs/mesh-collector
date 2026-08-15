-- schema_version: 0.11.0

-- -------------------
-- What the version number promises
-- -------------------
--
-- This project owns the schema and the readers only consult it, so the version
-- above is a contract with them. It is MAJOR.MINOR.PATCH, and each part means
-- something a reader is entitled to rely on:
--
--   MAJOR  A reader that worked before may now be wrong. Something it selected
--          was removed, renamed, or kept its name and changed meaning. Readers
--          must refuse an archive whose major differs from the one they were
--          written against — there is no safe way to guess.
--   MINOR  Additive only. New tables, new columns, new meta keys. Everything a
--          previous reader selected is still there and still means the same
--          thing, so an older reader stays correct against a newer archive.
--   PATCH  Nothing structural. Comments, indexes, a corrected default.
--
-- The obligation runs the other way too: anything that would break an existing
-- reader is a MAJOR bump, however small it looks. Dropping a column to tidy up
-- is a major change.
--
-- What this buys is that readers declare the *minimum* they need rather than
-- the exact version they saw. RxOnly sat at 0.6.0 while mesh-console needed
-- 0.7.0, and both read a 0.8.0 archive the whole time — neither had to move
-- because a column was added for somebody else's benefit.
--
-- Both now need 0.8.0, because both surface the telemetry columns 0.8.0 added.
-- That they agree today is what they each happen to select and not the two being
-- kept in step; the constants are still declared separately in each project, and
-- the next column one of them selects alone will part them again.
--
-- 0.9.0 is that promise being collected rather than restated: it adds six
-- columns to `nodes` and neither reader was touched, so both still declare
-- 0.8.0 and both still read this archive correctly. A reader that wants the new
-- columns raises its own constant when it starts selecting them, and not before.
--
-- 0.10.0 adds `emoji` to `messages` and `direct_messages`, and this time both
-- readers do move, because both select it to tell a tapback from an ordinary
-- message. It is the first version whose MINOR reaches two digits, which is a
-- trap only for code that compares these strings lexically — "0.10.0" sorts
-- *below* "0.9.0" as text. Nothing does: both readers split on '.' and compare
-- integer tuples (_parse_version), and the collector compares for equality
-- only. Anything new that reads this number owes the same arithmetic.
--
-- 0.11.0 adds no tables and no columns: it is two meta keys, firmware_version
-- and firmware_channel, documented below. A version bump for two optional keys
-- looks like ceremony, but the MINOR line above says "new meta keys" in so many
-- words, and a convention kept only when it is convenient is not one. The rung
-- in _UPGRADES is correspondingly empty — there is no ALTER to run, the stamp
-- itself is the upgrade — and neither reader moves its floor, because both
-- treat an absent key as "the collector never said" rather than as an error.
--
-- The collector itself is stricter, and has to be, but it is no longer strict to
-- the point of destroying the archive over an added column. It carries an
-- upgrade ladder for MINOR bumps: a database whose version has a rung in
-- _UPGRADES is altered in place, after a backup, and its rows are kept. Anything
-- with no rung — a MAJOR bump, a version this code has never heard of, a foreign
-- database — still means a rebuild, and still refuses to happen without
-- ALLOW_DESTRUCTIVE_REBUILD. See _initialize_or_upgrade_database().
--
-- The ladder is what makes a MINOR bump honest. Additive-only was already the
-- promise this file made to readers; until 0.10.0 the writer kept it by throwing
-- the archive away, which satisfied the letter of it and nobody's expectations.
--


-- -------------------
-- Meta table
-- -------------------
--
-- Keys written by the collector. Every value is TEXT; readers parse.
--
--   schema_version         Version string from the top of this file.
--   local_node_id          Hex node id of the attached device ('!433a1b2c'),
--                          in the same format as nodes.node_id.
--   max_messages           Integer. Channel messages the collector keeps.
--   max_direct_messages    Integer. Direct messages the collector keeps.
--   stores_direct_messages 'true' or 'false'. Whether direct messages are
--                          being archived at all.
--   primary_channel        Integer channel index treated as primary.
--   tracked_channels       Comma-separated channel indexes being archived
--                          ('0,2,3'), or '' when none are.
--   accepts_transmit       'true' or 'false'. Whether the collector is hosting
--                          a mesh-link control socket and will transmit for a
--                          client that can reach it. Optional: absent means
--                          'false', which is what a reader must assume of any
--                          archive written before this key existed.
--   firmware_version       The attached device's firmware as it reports it —
--                          version then build hash ('2.7.26.54e0d8d'). Optional,
--                          and '' when the device did not say. Republished on
--                          every startup, so a reflash shows up on the next
--                          restart rather than never.
--   firmware_channel       'beta' or 'alpha': which release channel that build
--                          shipped on, per Meshtastic's own release listing.
--                          The device does not know this about itself — the
--                          collector looks it up online at startup — so '' or
--                          absent means "could not say" (offline, an unlisted
--                          build, an archive from before this key), and a
--                          reader shows the version untagged rather than
--                          guessing.
--
-- The collector republishes the policy keys on every startup, so they describe
-- the archive as it is being written now. They are facts for readers to consult
-- instead of keeping their own copy of the collector's configuration; whether a
-- reader exposes what it finds is a separate, local decision.
--
-- accepts_transmit is a fact about the writer, not an invitation: a reader that
-- sees 'true' still has to reach a socket only the collector's own user can
-- open, and it should fail closed on anything it cannot confirm.
--
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- -------------------
-- Nodes table
-- -------------------
--
-- The telemetry columns after altitude arrived in 0.8.0. Like battery_level
-- and voltage before them they are latest-value only — each new reading
-- replaces the last, nothing in this archive is a time series. temperature,
-- humidity and pressure come from the environment-metrics telemetry arm;
-- channel_util, air_util_tx and uptime_seconds come from device metrics and
-- local stats, which report the same radio-health numbers.
--
-- 0.9.0 adds six more on the same latest-value-only terms. Three of them are
-- facts about how a node was heard rather than anything it measured:
--
--   hops_away        Hops between this radio and the node. 0 is a direct
--                    neighbour and is a real reading, not a missing one —
--                    read this column with IS NULL, never with falsiness.
--                    The firmware tracks it (NodeDB.cpp:1946) and the library
--                    hangs it on the node dict; for a live packet it is
--                    hopStart - hopLimit. It earns its width because it is
--                    one of the very few things knowable about a node that
--                    has never sent a NODEINFO, and such rows are ~10% of
--                    this table.
--   via_mqtt         1 when the node was last heard through an MQTT gateway,
--                    0 when it was last heard over RF, NULL when only the
--                    device's own node cache has ever described it. Same
--                    meaning as messages.via_mqtt, which has had the column
--                    since before this one existed; `nodes` lacking it was an
--                    inconsistency. A decoded packet settles it either way,
--                    because the library omits the field when it is false and
--                    false is exactly what a LoRa packet means. A cache
--                    replay can only ever set it — absence there is silence
--                    rather than a denial, and silence stays NULL.
--   has_public_key   1 when the node published a PKI public key, 0 when a
--                    NODEINFO this collector decoded carried none, and NULL
--                    while no NODEINFO has ever arrived at all — which for a
--                    nameless row is the ordinary state and not a gap to be
--                    filled in. **The key itself is deliberately not stored.**
--                    Presence is the entire signal an archive owes anybody,
--                    and the log's own byte-field table already reduces
--                    public_key to its shape for the same reason (see
--                    _BYTE_FIELDS in collector/__init__.py) — a key is not
--                    legible to a reader in any alphabet.
--
-- The other three are environment-metrics fields 0.8.0 decoded and then threw
-- away: lux, iaq (air-quality index, 0-500) and gas_resistance (the raw MOhm
-- reading iaq is derived from). They arrive together, from BME680-class
-- sensors.
--
-- The rest of that arm — windSpeed, windDirection, radiation — and the whole
-- of the airQualityMetrics, powerMetrics, healthMetrics and hostMetrics arms
-- have columns nowhere, on purpose. Nothing on this mesh emits them: a
-- 45-minute window carrying 247 device-metrics frames contained not one, and
-- powerMetrics stayed absent even with five solar nodes in the table, because
-- that arm is opt-in firmware telemetry nobody enabled. A column that is
-- always NULL is width plus a promise to readers that something is being
-- collected. They are one MINOR bump away the day hardware shows up.
--
CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    short_name TEXT,
    long_name TEXT,
    hardware TEXT,
    role TEXT,
    first_seen INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    last_seen INTEGER,
    battery_level INTEGER,
    voltage REAL,
    snr REAL,
    rssi REAL,
    latitude REAL,
    longitude REAL,
    altitude REAL,
    temperature REAL,
    humidity REAL,
    pressure REAL,
    channel_util REAL,
    air_util_tx REAL,
    uptime_seconds INTEGER,
    -- Appended rather than slotted in beside the columns they belong with.
    -- A reader selecting by name cannot tell the difference, but one that
    -- indexes a `SELECT *` row positionally can, and additive has to mean
    -- additive for both kinds.
    hops_away INTEGER,
    via_mqtt INTEGER DEFAULT 0,
    has_public_key INTEGER DEFAULT 0,
    lux REAL,
    iaq INTEGER,
    gas_resistance REAL
);

-- Index to quickly find active nodes by last_seen
CREATE INDEX IF NOT EXISTS idx_nodes_last_seen
ON nodes (last_seen);


-- -------------------
-- Channels table
-- -------------------
CREATE TABLE IF NOT EXISTS channels (
    channel_index INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);


-- -------------------
-- Channel messages table
-- -------------------
--
-- `emoji` arrived in 0.10.0 and is deliberately tri-state. The firmware sets a
-- flag beside the reply id to say "this text is a reaction to that message, not
-- a message of its own", and until 0.10.0 this archive dropped it — leaving
-- readers to guess from the text, which is only ever a guess: a reply that
-- happens to be nothing but 👍 looks identical to a tapback.
--
--   NULL  Written before 0.10.0. The flag was not recorded, so it is unknown,
--         and a reader must fall back to whatever heuristic it used before.
--         **Never backfilled.** Guessing a value here would launder a guess
--         into a fact, and the point of the column is to stop doing that.
--   0     The flag was recorded and was not set: an ordinary message.
--   1     The flag was recorded and was set: a reaction.
--
-- Read it with IS NULL, never with falsiness — 0 and NULL are different answers
-- here, and only one of them is an answer.
--
-- Clients disagree on what they put in the field: some send 1, some send a
-- codepoint. The collector stores any nonzero as 1, so this column means "is a
-- reaction" and never "which emoji" — the emoji itself is in `text`, where it
-- has always been.
--
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER UNIQUE,
    channel_index INTEGER,
    from_node TEXT,
    to_node TEXT,
    text TEXT,
    rx_time INTEGER,
    hop_count INTEGER,
    snr REAL,
    rssi REAL,
    reply_to INTEGER,
    via_mqtt INTEGER DEFAULT 0,
    -- Appended, and with no DEFAULT, for the same reason `nodes` appends: an
    -- existing row upgraded in place gets NULL, which is exactly the "unknown"
    -- this column needs to mean. A DEFAULT 0 would quietly assert that every
    -- message ever archived was not a reaction.
    emoji INTEGER
);

-- Covering index for channel message lists (filter, order, JOIN keys)
CREATE INDEX IF NOT EXISTS idx_messages_channel_covering
ON messages (channel_index, rx_time DESC, id, message_id, from_node, reply_to, via_mqtt);

-- Reply-to JOIN optimization
CREATE INDEX IF NOT EXISTS idx_messages_reply_to
ON messages (reply_to);


-- -------------------
-- Direct messages table
-- -------------------
--
-- to_node arrived in 0.7.0, with transmitting. Before it, every row in this
-- table was a direct message *received*, so the recipient was always the local
-- node and there was nothing to record. A direct message the collector sends
-- has the local node in from_node instead, and without this column there would
-- be nowhere to say who it went to.
--
-- Both directions populate it, so the column means the same thing in every row:
-- an inbound row carries the local node here, an outbound row carries the peer.
-- Rows written before 0.7.0 do not exist — the version bump rebuilds.
--
-- `emoji` is the 0.10.0 column, tri-state, documented in full above `messages`.
-- It is here because mesh-console selects both tables through one shared column
-- list, so a column on one is a column on both whether or not tapbacks are as
-- interesting in a DM thread.
--
CREATE TABLE IF NOT EXISTS direct_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER UNIQUE,
    from_node TEXT,
    to_node TEXT,
    text TEXT,
    rx_time INTEGER,
    snr REAL,
    rssi REAL,
    reply_to INTEGER,
    via_mqtt INTEGER DEFAULT 0,
    emoji INTEGER
);

-- Covering index for DM lists (order, JOIN keys)
CREATE INDEX IF NOT EXISTS idx_dms_covering
ON direct_messages (rx_time DESC, id, message_id, from_node, reply_to, via_mqtt);

-- Reply-to JOIN optimization
CREATE INDEX IF NOT EXISTS idx_dms_reply_to
ON direct_messages (reply_to);
