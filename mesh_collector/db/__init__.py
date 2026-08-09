from __future__ import annotations

import logging
import sqlite3
import threading
import time

from datetime import datetime
from pathlib import Path
from typing import Optional

from mesh_collector.config import Config


SCHEMA_FILE = Path(__file__).parent / "schema.sql"




class SchemaVersionMismatch(RuntimeError):
  """Raised when the database predates schema.sql and a rebuild wasn't authorized."""




class Storage:
  """SQLite storage layer for nodes, messages, and direct messages."""

  def __init__(self) -> None:
    self.db_path = Path(Config.get("DB_PATH"))
    self.db_path.parent.mkdir(parents=True, exist_ok=True)

    self.conn = sqlite3.connect(
      self.db_path,
      check_same_thread=False,
    )
    self.conn.row_factory = sqlite3.Row

    # One connection shared by two threads when transmitting is on: meshtastic's
    # reader thread (pubsub callbacks) and the main thread (the control drain).
    # sqlite3 serializes individual statements, but `with self.conn:` transaction
    # scope is per-connection, so unlocked writers can commit or roll back each
    # other's half-done work. Reentrant because upsert/insert methods call the
    # prune methods while already holding it. Callers never see this lock.
    self._lock = threading.RLock()

    with self.conn:
      self.conn.execute("PRAGMA foreign_keys = ON;")
      self.conn.execute("PRAGMA journal_mode = WAL;")
      self.conn.execute("PRAGMA synchronous = NORMAL;")
      self.conn.execute("PRAGMA busy_timeout = 3000;")

    self._initialize_or_upgrade_database()

    self._node_insert_count = 0
    self._message_insert_count = 0
    self._dm_insert_count = 0
    self._prune_interval = Config.get("PRUNE_INTERVAL")




  def _read_schema_version(self) -> str:
    with open(SCHEMA_FILE, "r") as f:
      first_line = f.readline().strip()
    if first_line.startswith("-- schema_version:"):
      return first_line.split(":", 1)[1].strip()
    return "0.0.0"




  def _get_db_schema_version(self) -> str:
    try:
      row = self.conn.execute(
        "SELECT value FROM meta WHERE key='schema_version';"
      ).fetchone()
      return row["value"] if row else "0.0.0"
    except sqlite3.OperationalError:
      return "0.0.0"




  def _backup_database(self) -> Path:
    """Write a timestamped copy of the database beside it, and return its path.

    Uses VACUUM INTO rather than a file copy so the snapshot is consistent even
    though the database is in WAL mode.
    """
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = self.db_path.with_name(f"{self.db_path.name}.{stamp}.bak")

    if backup_path.exists():
      raise FileExistsError(f"Refusing to overwrite existing backup {backup_path}")

    self.conn.execute("VACUUM INTO ?", (str(backup_path),))
    logging.warning("Wrote pre-rebuild backup to %s", backup_path)

    return backup_path




  def _initialize_or_upgrade_database(self) -> None:
    """Create the schema, or rebuild the database if the schema version changed.

    A rebuild drops every table, so on an existing database it happens only when
    ALLOW_DESTRUCTIVE_REBUILD is set, and only after a backup has been written.
    """
    schema_version = self._read_schema_version()
    db_version = self._get_db_schema_version()

    if schema_version == db_version:
      return

    # An existing database is one that holds tables, not one that names a
    # version. A pre-meta or foreign database also reads as "0.0.0", and
    # classifying it as fresh skipped the gate and the backup while
    # CREATE TABLE IF NOT EXISTS kept its old table shapes — which then got
    # stamped with the new version anyway.
    is_existing_database = self.conn.execute(
      "SELECT name FROM sqlite_master WHERE type='table' "
      "AND name NOT LIKE 'sqlite_%' LIMIT 1"
    ).fetchone() is not None

    if is_existing_database and not Config.get("ALLOW_DESTRUCTIVE_REBUILD", False):
      raise SchemaVersionMismatch(
        f"Database at {self.db_path} is schema version {db_version}, but "
        f"schema.sql is {schema_version}. Upgrading rebuilds the database from "
        "scratch and discards every node and message it holds.\n"
        "\n"
        "To keep the archive, point DB_PATH at a database built by this version, "
        "or downgrade to the code that wrote this one.\n"
        "To discard it, re-run with ALLOW_DESTRUCTIVE_REBUILD enabled — a "
        "timestamped backup is written beside the database first."
      )

    logging.info("Initializing/upgrading database: %s -> %s", db_version, schema_version)

    # Fail before dropping anything if the backup can't be written.
    if is_existing_database:
      self._backup_database()

    with open(SCHEMA_FILE, "r") as f:
      sql_script = f.read()

    with self.conn:
      if is_existing_database:
        existing_tables = self.conn.execute(
          "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
        for row in existing_tables:
          table_name = row["name"]
          if table_name.startswith("sqlite_"):
            continue
          logging.info("Dropping table %s", table_name)
          self.conn.execute(f"DROP TABLE IF EXISTS {table_name};")

      logging.info("Rebuilding database schema")
      self.conn.executescript(sql_script)

      self.conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?)",
        (schema_version,)
      )

    logging.info("Database initialized/upgraded successfully")




  def get_meta(self, key: str) -> Optional[str]:
    with self._lock:
      row = self.conn.execute(
        "SELECT value FROM meta WHERE key = ?", (key,)
      ).fetchone()
      return row["value"] if row else None




  def set_meta(self, key: str, value: str) -> None:
    with self._lock, self.conn:
      self.conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        (key, value),
      )




  def set_meta_values(self, values: dict[str, str]) -> None:
    """Write several meta keys in one transaction, so readers never see a
    half-published policy."""
    with self._lock, self.conn:
      self.conn.executemany(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        sorted(values.items()),
      )




  def upsert_channel(self, channel_index: int, name: str) -> None:
    """Insert or update a channel."""
    try:
      with self._lock, self.conn:
        self.conn.execute(
          """INSERT INTO channels (channel_index, name)
             VALUES (?, ?)
             ON CONFLICT(channel_index) DO UPDATE SET
             name = excluded.name""",
          (channel_index, name)
        )
    except Exception:
      logging.exception("Failed to upsert channel")




  def insert_message(
    self,
    message_id: int,
    channel_index: int,
    from_node: str,
    to_node: Optional[str],
    text: str,
    rx_time: int,
    hop_count: Optional[int],
    snr: Optional[float],
    rssi: Optional[int],
    reply_to: Optional[int] = None,
    via_mqtt: bool = False,
  ) -> bool:
    """Insert channel message and periodically prune old messages.

    Returns True if inserted, False if duplicate.
    """
    with self._lock:
      with self.conn:
        cursor = self.conn.execute(
          """INSERT OR IGNORE INTO messages
             (message_id, channel_index, from_node, to_node, text, rx_time, hop_count, snr, rssi, reply_to, via_mqtt)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
          (message_id, channel_index, from_node, to_node, text, rx_time, hop_count, snr, rssi, reply_to, int(via_mqtt)),
        )
        inserted = cursor.rowcount > 0

      if inserted:
        self._message_insert_count += 1
        if self._message_insert_count >= self._prune_interval:
          self._prune_messages()
          self._message_insert_count = 0

      return inserted




  def insert_direct_message(
    self,
    message_id: int,
    from_node: str,
    to_node: Optional[str],
    text: str,
    rx_time: int,
    snr: Optional[float],
    rssi: Optional[int],
    reply_to: Optional[int] = None,
    via_mqtt: bool = False,
  ) -> bool:
    """Insert direct message and periodically prune old messages.

    Returns True if inserted, False if duplicate.
    """
    with self._lock:
      with self.conn:
        cursor = self.conn.execute(
          """INSERT OR IGNORE INTO direct_messages
             (message_id, from_node, to_node, text, rx_time, snr, rssi, reply_to, via_mqtt)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
          (message_id, from_node, to_node, text, rx_time, snr, rssi, reply_to, int(via_mqtt)),
        )
        inserted = cursor.rowcount > 0

      if inserted:
        self._dm_insert_count += 1
        if self._dm_insert_count >= self._prune_interval:
          self._prune_direct_messages()
          self._dm_insert_count = 0

      return inserted




  def get_node(self, node_id: str) -> Optional[dict]:
    """Retrieve a node by its node_id."""
    with self._lock:
      row = self.conn.execute(
        "SELECT * FROM nodes WHERE node_id = ?", (node_id,)
      ).fetchone()
      return dict(row) if row else None




  def upsert_node(
    self,
    node_id: str,
    short_name: Optional[str],
    long_name: Optional[str],
    hardware: Optional[str],
    role: Optional[str],
    last_seen: int,
    battery_level: Optional[int],
    voltage: Optional[float],
    snr: Optional[float],
    rssi: Optional[int],
    latitude: Optional[float],
    longitude: Optional[float],
    altitude: Optional[int],
    temperature: Optional[float] = None,
    humidity: Optional[float] = None,
    pressure: Optional[float] = None,
    channel_util: Optional[float] = None,
    air_util_tx: Optional[float] = None,
    uptime_seconds: Optional[int] = None,
    *,
    is_new: bool = False,
  ) -> None:
    """Insert or update a node, preserving non-null existing values.

    `is_new` marks a true insert — the caller has already read the row, and
    cursor.rowcount cannot tell (ON CONFLICT DO UPDATE reports 1 on the
    UPDATE branch too). Only true inserts advance the prune counter.
    """
    with self._lock:
      with self.conn:
        self.conn.execute(
          """INSERT INTO nodes
             (node_id, short_name, long_name, hardware, role, last_seen,
              battery_level, voltage, snr, rssi, latitude, longitude, altitude,
              temperature, humidity, pressure, channel_util, air_util_tx,
              uptime_seconds)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(node_id) DO UPDATE SET
             short_name = COALESCE(excluded.short_name, short_name),
             long_name = COALESCE(excluded.long_name, long_name),
             hardware = COALESCE(excluded.hardware, hardware),
             role = COALESCE(excluded.role, role),
             last_seen = excluded.last_seen,
             battery_level = COALESCE(excluded.battery_level, battery_level),
             voltage = COALESCE(excluded.voltage, voltage),
             snr = COALESCE(excluded.snr, snr),
             rssi = COALESCE(excluded.rssi, rssi),
             latitude = COALESCE(excluded.latitude, latitude),
             longitude = COALESCE(excluded.longitude, longitude),
             altitude = COALESCE(excluded.altitude, altitude),
             temperature = COALESCE(excluded.temperature, temperature),
             humidity = COALESCE(excluded.humidity, humidity),
             pressure = COALESCE(excluded.pressure, pressure),
             channel_util = COALESCE(excluded.channel_util, channel_util),
             air_util_tx = COALESCE(excluded.air_util_tx, air_util_tx),
             uptime_seconds = COALESCE(excluded.uptime_seconds, uptime_seconds)
          """,
          (
            node_id, short_name, long_name, hardware, role, last_seen,
            battery_level, voltage, snr, rssi, latitude, longitude, altitude,
            temperature, humidity, pressure, channel_util, air_util_tx,
            uptime_seconds,
          ),
        )

      if is_new:
        self._node_insert_count += 1
        if self._node_insert_count >= self._prune_interval:
          self.prune_stale_nodes()
          self._node_insert_count = 0




  def _prune_messages(self) -> None:
    """Delete channel messages beyond MAX_MESSAGES limit."""
    max_messages = Config.get("MAX_MESSAGES")
    # The DELETE below re-sorts the whole table to usually remove nothing;
    # an index-served COUNT decides first whether there is anything to do.
    count = self.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    if count <= max_messages:
      return
    with self.conn:
      deleted = self.conn.execute(
        """DELETE FROM messages
           WHERE id NOT IN (
               SELECT id FROM messages
               ORDER BY rx_time DESC
               LIMIT ?
           )""",
        (max_messages,),
      ).rowcount
      
    if deleted:
      noun = "Message" if deleted == 1 else "Messages"
      logging.info("%d %s pruned", deleted, noun)  


  def _prune_direct_messages(self) -> None:
    """Delete direct messages beyond MAX_DIRECT_MESSAGES limit."""
    max_dm = Config.get("MAX_DIRECT_MESSAGES")
    count = self.conn.execute("SELECT COUNT(*) FROM direct_messages").fetchone()[0]
    if count <= max_dm:
      return
    with self.conn:
      deleted = self.conn.execute(
        """DELETE FROM direct_messages
           WHERE id NOT IN (
               SELECT id FROM direct_messages
               ORDER BY rx_time DESC
               LIMIT ?
           )""",
        (max_dm,),
      ).rowcount
      
    if deleted:
      noun = "Direct message" if deleted == 1 else "Direct messages"
      logging.info("%d %s pruned", deleted, noun)      


  def prune_stale_nodes(self) -> None:
    """Delete nodes not seen within NODE_PRUNE_DAYS."""
    cutoff = int(time.time()) - (Config.get("NODE_PRUNE_DAYS") * 86400)
    with self._lock, self.conn:
      deleted = self.conn.execute("DELETE FROM nodes WHERE last_seen < ?", (cutoff,)).rowcount

      if deleted:
        noun = "Node" if deleted == 1 else "Nodes"
        logging.info("%d %s pruned", deleted, noun)




  def close(self) -> None:
    with self._lock:
      if self.conn is None:
        return
      self.conn.close()
      # A closed connection object is still truthy, so the ingest guard that
      # tests `getattr(storage, "conn", None)` would never trip on it. Leave
      # None behind so shutdown reads as shutdown.
      self.conn = None
