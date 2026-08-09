-- schema_version: 0.8.0

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
-- The collector itself is stricter, and has to be: it has no migrations, so any
-- difference between this file and an existing database means a rebuild. See
-- _initialize_or_upgrade_database().
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
    uptime_seconds INTEGER
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
    via_mqtt INTEGER DEFAULT 0
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
    via_mqtt INTEGER DEFAULT 0
);

-- Covering index for DM lists (order, JOIN keys)
CREATE INDEX IF NOT EXISTS idx_dms_covering
ON direct_messages (rx_time DESC, id, message_id, from_node, reply_to, via_mqtt);

-- Reply-to JOIN optimization
CREATE INDEX IF NOT EXISTS idx_dms_reply_to
ON direct_messages (reply_to);
