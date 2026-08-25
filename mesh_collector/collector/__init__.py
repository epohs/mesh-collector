from __future__ import annotations

import atexit
import json
import logging
import os
import re
import signal
import socket
import sys
import threading
import time
import urllib.request

from typing import TYPE_CHECKING, Optional

from meshtastic.mesh_interface import MeshInterface
from meshtastic.protobuf import config_pb2, mesh_pb2, portnums_pb2
from meshtastic.serial_interface import SerialInterface
from pubsub import pub

from mesh_collector import logfmt, selflog, transport
from mesh_collector.config import Config
from mesh_collector.db import Storage
from mesh_collector.transport import BLE, TCP

# The transmit path's types, for reading rather than for running. mesh-link is an
# optional dependency — an archive-only install has none on its import path, which
# is the whole point of `uv sync` without `--extra tx` — so it cannot be imported
# at module scope. `from __future__ import annotations` means every annotation
# below is a string that is never evaluated, so naming these costs nothing at
# runtime and the send methods stop being the only unannotated ones in the file.
if TYPE_CHECKING:
  from mesh_link import PendingRequest, SendTextRequest


LOG_FORMAT = "[%(levelname)s] %(message)s"

# How long the drain waits on the control queue before looping. Short enough
# that stop() is noticed promptly, long enough that an idle collector isn't
# spinning.
CONTROL_POLL_INTERVAL = 1.0

# How often the receive path accounts for what it threw away, and how many drops
# it will sit on before saying so early.
#
# Both exist because of a question this collector could not answer: a channel
# with no messages on it looks exactly like a channel whose PSK is wrong. Packets
# this radio cannot decrypt are dropped without a word at any level, so "quiet
# mesh" and "misconfigured key" produced identical logs — and the only way to
# tell them apart was to walk up to the radio.
#
# Fifteen minutes because the line is meant to be read after the fact, by
# somebody grepping a day of journal for it, and one line per quarter hour is
# cheap enough to leave on forever. The drop threshold is what makes it useful
# in the other direction: a radio being shouted at by a channel it cannot read
# reaches fifty drops long before the window closes, and waiting the rest of the
# window to mention it wastes the operator's time while they are standing there
# watching.
RX_SUMMARY_INTERVAL = 900
RX_SUMMARY_MAX_DROPS = 50

# **What the firmware calls a channel that has no name of its own.** An unnamed
# primary channel is not hashed under an empty string: `Channels::getName`
# substitutes the modem preset's name, so the default channel everybody shares
# hashes as "LongFast" and comes out 0x08. Without this table a radio running an
# unnamed primary could never be matched against its own packets, which is the
# one case `channel_hash` exists to catch.
#
# Keyed by the protobuf enum name rather than its number: the numbers are wire
# format and stable, but reading `LONG_FAST` beside `LongFast` is what makes a
# missing entry obvious when the firmware adds a preset. An unknown preset is a
# miss, not a guess — see `_channel_hash_name`.
_PRESET_NAMES = {
  "SHORT_TURBO": "ShortTurbo",
  "SHORT_FAST": "ShortFast",
  "SHORT_SLOW": "ShortSlow",
  "MEDIUM_FAST": "MediumFast",
  "MEDIUM_SLOW": "MediumSlow",
  "LONG_FAST": "LongFast",
  "LONG_MODERATE": "LongMod",
  "LONG_SLOW": "LongSlow",
  "VERY_LONG_SLOW": "VLongSlow",
}

# How long the liveness probe waits for the radio to answer before calling the
# link gone.
#
# **The deadline is the point of the probe, not a detail of it.** `sendHeartbeat`
# reaches `BLEClient.write_gatt_char`, which is `async_await(coro)` with
# `timeout=None` (`ble_interface.py:307`), which is `future.result(None)`
# (`:339`) — a wait that cannot expire. There is no argument that changes this;
# `sendHeartbeat` takes none. So the bound has to be ours, and the probe runs on
# a thread we can walk away from rather than on the main loop, where a write that
# never returns parks the whole collector inside the watchdog that was supposed to
# be protecting it.
#
# Ten seconds, against a measured ~0.2s for a healthy BLE write — fifty times the
# headroom, which matters because a TCP heartbeat can legitimately sit inside
# TCPInterface's own reconnect for a few seconds and must not be shot for it.
LIVENESS_PROBE_DEADLINE = 10.0

# The ceiling on the BLE retry backoff. A drop that outlasts a minute is not
# getting better on the timescale doubling can chase, and by then the attempt
# count is the thing about to end this process anyway.
RECONNECT_BACKOFF_CAP = 60

# "The library does not have this field", as distinct from "the field is None",
# which several of them legitimately are. Only `_ble_link_down_reason` needs it.
_UNREADABLE = object()

# Where the firmware release channel comes from. The device reports its version
# with a build hash ('2.7.26.54e0d8d') and nothing else — whether that build is
# a Beta or an Alpha is a fact about Meshtastic's release listing, not about the
# radio, so it has to be looked up. GitHub's own API rather than
# api.meshtastic.org/github/firmware/list, which was the obvious choice and was
# rejected: that index carries only the newest couple of releases per channel,
# so a device running last month's beta — the normal state of a device nobody
# is reflashing weekly — silently loses its tag there. One page of a hundred
# releases reaches back years; anything older than that is not going to be
# classified usefully anyway, and a startup should not walk pages of a
# rate-limited API on the off chance.
#
# Five seconds because this stands between the serial port opening and the main
# loop: a startup should not hang on GitHub having a bad day, and the failure
# mode is only an untagged version in a menu.
FIRMWARE_RELEASES_URL = (
  "https://api.github.com/repos/meshtastic/firmware/releases?per_page=100"
)
FIRMWARE_LOOKUP_TIMEOUT = 5

# How long to wait for a meshtasticd host to accept a connection, in tcp mode.
#
# This exists because TCPInterface does not bound its own connect:
# `myConnect` calls `socket.create_connection(addr)` with no timeout
# (tcp_interface.py:82-86), so it inherits the OS default — which for an
# unroutable address is around seventy-five seconds of nothing on macOS, and
# longer on Linux. A refused connection fails instantly, so this only bites the
# host that is *silently* absent: a Pi that has not finished booting, a wrong
# address, a firewall that drops rather than rejects. Under systemd that
# difference is between a unit that exits and gets restarted and one that sits in
# "activating" for a minute at a time.
#
# Five seconds because the realistic targets are localhost and a LAN address,
# both of which answer in milliseconds; anything slower than this is not a
# meshtasticd that will serve a reliable stream of packets anyway.
TCP_CONNECT_TIMEOUT = 5




def node_num_to_hex_id(node_num: int | str) -> str:
  """Convert a decimal nodeNum to the hex node_id format used by nodes.node_id
  (e.g. 1234567890 -> '!499602d2')."""
  return f"!{int(node_num) & 0xFFFFFFFF:08x}"




# The shape nodes.node_id is contracted to hold. Row creation is gated on this
# rather than on having a name: a node heard only by number is a real node, but
# a malformed id is a bug, and the readers join on this column.
NODE_ID_PATTERN = re.compile(r"^![0-9a-f]{8}$")




def _first_value(source: dict, *keys):
  """The first of `keys` holding a non-null value.

  Not `a or b`: an SNR or RSSI of exactly 0 is a measurement, and truthiness
  throws it away — which for RSSI, whose useful range straddles nothing in
  particular, silently dropped a real reading.
  """
  for key in keys:
    value = source.get(key)
    if value is not None:
      return value

  return None




def _sender_id(packet: dict) -> Optional[str]:
  """Who sent this packet, as a node_id, or None if not even the header says.

  `fromId` is the library's own resolution of the sender's number against the
  device NodeDB, so it is absent for any node the device cannot yet name — and
  it is absent for *every* node on a channel this radio has no key for, since
  the NODEINFO that would have named them was unreadable too.

  The number survives that: it rides in the packet header, outside everything
  the channel PSK covers, so an undecryptable packet still says who sent it.
  Hence the fallback rather than giving up when `fromId` is missing. A `from` of
  zero is not a sender — the proto omits a field at its default, so absent and
  zero arrive identically and neither one names a node.
  """
  from_id = packet.get("fromId")
  if from_id:
    return from_id

  raw_from = packet.get("from")
  if not raw_from:
    return None

  return node_num_to_hex_id(raw_from)




def _hops_taken(packet: dict) -> Optional[int]:
  """How many hops this packet spent getting here, or None if it can't be known.

  `is not None`, not truthiness: hopLimit is 0 on a packet that spent every hop
  getting here — the most-travelled packets are exactly the ones whose hop count
  used to come out NULL.

  This is also the arithmetic the firmware uses to maintain a node's hops_away
  (NodeDB.cpp:1946), so one packet's answer is the freshest reading available for
  nodes.hops_away as well as the hop count on a message row.
  """
  hop_start = packet.get("hopStart")
  hop_limit = packet.get("hopLimit")

  if hop_start is None or hop_limit is None:
    return None

  return hop_start - hop_limit




def _identity_label(row: dict) -> str:
  """Render a node row's identity for the log — "Foo Bar (FOO)" when it has
  both names, whichever one it has when it has one, "unnamed" when it has
  neither. Nameless is now an ordinary, expected state for a row."""
  long_name = row.get("long_name")
  short_name = row.get("short_name")

  if long_name and short_name:
    return f"{long_name} ({short_name})"

  return long_name or short_name or "unnamed"




def _node_label(node_id: str, row: Optional[dict]) -> str:
  """A node for a log line: its name and its id, or just its id.

  `!eb179ad7` is not a name, and a log full of them is a log you read with the
  node list open in another window. The name is nearly always already in hand
  where these lines are written — the receive path reads the row before it
  decides anything else — so this costs a dict lookup rather than a query.

  **The id stays.** It is what the archive is keyed by, what a grep for one node
  matches on, and what the console paints its own colour; dropping it to save
  eleven columns would break all three. And an unnamed node keeps the bare id
  rather than gaining the word "unnamed", which is `_identity_label`'s answer and
  the right one where the subject is the identity itself — here it would be a
  label reading `unnamed !eb179ad7`, which says nothing the id did not.

  Untidy logs get the id alone, unchanged, like every other formatting this
  module does under TIDY_LOGS.
  """
  if not logfmt.tidy_logs() or not row:
    return node_id

  name = row.get("long_name") or row.get("short_name")
  return f"{name} {node_id}" if name else node_id




def _is_fabricated_identity(node_id: str, user: dict) -> bool:
  """True when a cached `user` is meshtastic's invention rather than something
  a node broadcast.

  The library's _getOrCreateByNum (mesh_interface.py:1523) fills in every node
  it learns by number alone with longName "Meshtastic <suffix>", shortName
  "<suffix>", hwModel "UNSET". Nothing announced those names, and archiving
  them buries the fact that the node is still unidentified — so match the
  formula exactly and treat a hit as no identity at all.
  """
  suffix = node_id[-4:]
  return (
    user.get("longName") == f"Meshtastic {suffix}"
    and user.get("shortName") == suffix
    and user.get("hwModel") == "UNSET"
  )




class MeshtasticCollector:
  """
  Collects Meshtastic packets from an attached node and persists
  node metadata, channel messages, and direct messages to SQLite.

  The node is reached over USB serial, TCP to a meshtasticd daemon, or BLE,
  whichever CONNECTION_MODE names. All three of meshtastic's interface classes
  subclass MeshInterface and everything below is typed against that API, so the
  mode is decided once — in `transport`, before `start()` — and shows up here as
  construction, one error tuple, and the label log lines name.
  """

  def __init__(self, db: Storage) -> None:
    self.storage = db
    # Resolved before anything is opened, so a bad CONNECTION_MODE or a
    # BLE_ADDRESS nobody filled in fails while the failure is still cheap.
    self.transport = transport.from_config()
    self.interface: Optional[MeshInterface] = None
    self._running = False
    self._connection_lost = False
    self._stopping = False
    # Set while _reconnect_ble is rebuilding the interface. It guards the same
    # door _stopping guards and for a sharper reason: a *failed* reopen calls
    # `close()` on the half-built object (`ble_interface.py:74`), and BLE's
    # close() ends in `self._disconnected()` (`:271`), which publishes
    # meshtastic.connection.lost. We are subscribed by then, so without this flag
    # every unsuccessful retry would arrive at _on_connection_lost as a fresh
    # loss and stop the loop the retry is running inside.
    self._reconnecting = False
    # Set by _on_ble_disconnected, on bleak's event loop thread, and cleared by
    # the main loop. A plain bool store and load, no lock, for the reason the drop
    # counters give below.
    self._ble_disconnected = False
    # Reopened links, for the one line at shutdown that answers "did it hold?".
    self._reconnect_count = 0
    self.local_node_id: Optional[str] = None
    self.tracked_channels: list[int] = self._tracked_channel_indexes()

    self.transmit_enabled: bool = bool(Config.get("ENABLE_TX", False))
    self.control_server = None

    # What the receive path dropped, since the last summary was logged.
    #
    # Plain dicts with no lock. Every write below happens on meshtastic's single
    # reader thread, which is also the only place they are read — the main thread
    # runs the control drain and never touches these. A lock here would be
    # protecting a structure from itself.
    #
    # _undecryptable_counts is keyed by the packet's channel **hash**, not a
    # channel index: an encrypted packet's header carries a one-byte hash of the
    # channel name and PSK, and the index only exists after a successful decrypt.
    # The two are not interchangeable and the hash must never be written to
    # channel_index — it is a label for the log and nothing else. Two channels
    # can even collide onto one hash, which is why the log calls it a hash and
    # quotes it in hex rather than pretending to name a channel.
    self._undecryptable_counts: dict[int, int] = {}
    self._undecryptable_mqtt = 0
    # Channel index to the name the device gave it, for every channel the device
    # knows rather than only the tracked ones: a non-text packet can arrive on a
    # channel this collector does not archive, and the summary still has to name
    # it. Filled by _sync_channels, which is the only place that has read the
    # device's channel list. Empty until then, and _channel_label falls back to
    # the index, so a log line written before the sync is unlabelled rather than
    # wrong.
    self.channel_names: dict[int, str] = {}
    # The reverse of the problem above: channel **hash** to the name of the
    # configured channel that hashes to it. An undecryptable packet carries only
    # the hash, and the single question worth asking of it is whether it belongs
    # to a channel this radio is supposed to be able to read. A hit here is a key
    # mismatch and something to fix; a miss is a neighbour and nothing to do.
    self.channel_hashes: dict[int, str] = {}
    # Channel hashes already reported once, so the explanatory line fires on
    # first sighting and not forty times a minute afterwards. Never cleared by
    # the summary reset — first sighting means first this process, not first
    # this window.
    self._undecryptable_seen: set[int] = set()
    # Keyed (channel_index, portnum). Not dropped exactly — the node row still
    # updates — but not archived either, and a channel carrying nothing but
    # position packets is the other honest explanation for an empty message list.
    self._nontext_counts: dict[tuple[int, str], int] = {}
    self._rx_summary_at = time.time()

    # When the link last proved it was alive, for the LIVENESS_TIMEOUT watchdog.
    # Written by the receive handlers on meshtastic's reader thread and read by
    # the main loop: one float store, no lock, for the reason spelled out above
    # the drop counters. Stamped now rather than at 0 so an enabled watchdog
    # measures from startup instead of firing on its first pass.
    self._last_activity = time.time()




  def start(self) -> None:
    logging.info("Starting Meshtastic collector on %s", self.transport.label)

    # **A missing device is a condition, not a crash.** Unplugging the radio —
    # which a firmware update requires — used to print a thirty-line traceback
    # here on every restart systemd scheduled, seconds apart, until the device
    # came back: one real event, buried under forty copies of its own stack.
    # The retry loop is systemd's and stays systemd's (Restart= is the
    # reconnect policy, as the exit path below says); this is the same nonzero
    # exit, said in one line a person can read.
    #
    # WARNING rather than ERROR, deliberately. The error is the unplug itself,
    # and _on_connection_lost reported it once, at that level, when it
    # happened; every failed reopen after it is the same ongoing condition,
    # and forty ERRORs for one event teach a reader to skim past ERROR.
    #
    # OSError covers both the vanished /dev path (FileNotFoundError) and
    # pyserial's SerialException, which subclasses it; in tcp mode it is the
    # refused or unroutable socket. MeshInterfaceError is the library's own "the
    # port opened but no radio answered" — what a device sitting in its
    # bootloader mid-flash looks like from here. _connect_errors adds BLE's,
    # which are neither.
    try:
      self.interface = self._open_interface()
    except self._connect_errors() as error:
      logging.warning(
        "No Meshtastic device answered at %s (%s). It may be unplugged, "
        "rebooting, or mid-flash; exiting so the service manager keeps "
        "retrying until it returns.",
        self.transport.label,
        error,
      )
      self.storage.close()
      sys.exit(1)

    self._watch_for_ble_disconnect()

    # **A collector that cannot describe its channels must not archive into
    # them.** Nothing is in flight yet — the control socket is not listening and
    # the receive handlers are not subscribed until further down — so exiting
    # here loses no message and leaves no client waiting. The retry is systemd's,
    # exactly as it is for the missing device above; _sync_channels has already
    # said in the journal whether the radio or the database was at fault, because
    # a unit restarting every five seconds explains nothing on its own.
    if not self._sync_channels():
      self.stop()
      sys.exit(1)

    self._initial_node_sync()
    self._warn_if_mqtt_proxy_expected()

    self.local_node_id = node_num_to_hex_id(self.interface.localNode.nodeNum)
    stored_node_id = self.storage.get_meta("local_node_id")

    # A swap means the archive already named a *different* device. An archive
    # that has never named one is a first run, not a swap: restarting there
    # accomplishes nothing, and it used to happen on every fresh database,
    # announcing a device swap that had not occurred.
    if stored_node_id is not None and stored_node_id != self.local_node_id:
      logging.warning(
        "Device swap detected (stored=%s, current=%s)",
        stored_node_id,
        self.local_node_id,
      )
      # The tidy log's held Self state describes the device that was unplugged.
      # `selflog.record` would discard it anyway on the node_id mismatch, but a
      # swap is the one moment the archive is known to be changing identity, so
      # the state is removed here, explicitly, rather than left for a guard to
      # catch — a Self line must never carry the old node's readings under the
      # new node's id.
      selflog.reset(self.storage)
      self.storage.set_meta("local_node_id", self.local_node_id)
      self._restart_process()

    self.storage.set_meta("local_node_id", self.local_node_id)

    # Deliberately after the swap check and before the policy is published.
    # _restart_process() re-execs, which runs no finally block and no atexit
    # handler, so a socket opened above it would be left behind on every swap —
    # and publishing afterwards is what lets accepts_transmit report the socket
    # that exists rather than the one that was configured.
    self._start_control_server()

    self._publish_policy()
    self._publish_firmware()
    self.storage.prune_stale_nodes()

    pub.subscribe(self._on_receive, "meshtastic.receive")
    pub.subscribe(self._on_node_updated, "meshtastic.node.updated")
    pub.subscribe(self._on_connection_lost, "meshtastic.connection.lost")
    # Cheap observability on the transports that come and go. TCPInterface heals
    # a blip inside the library by re-running myConnect() (tcp_interface.py:137-180)
    # without telling anyone, so on TCP this line is the only trace that the link
    # dropped and came back at all.
    pub.subscribe(self._on_connection_established, "meshtastic.connection.established")

    self._running = True
    self._main_loop()

    # The loop only falls out here when _on_connection_lost or _supervise_ble_link
    # stopped it — a signal exits from inside its own handler and never returns
    # this far. Exit nonzero so Restart= in the service unit fires: the reconnect
    # is the restart. In-process reconnect-with-backoff was the alternative and
    # was not taken; systemd already owns the retry policy.
    #
    # **That is still the policy for serial and TCP, and it is no longer the whole
    # policy.** BLE reaches this line only after `_reconnect_ble` has used up
    # BLE_RECONNECT_ATTEMPTS, because on BLE the premises above do not hold: the
    # library reports no drop at all, so nothing was ever stopping this loop, and
    # a restart is not cheap — it re-runs channel sync, the node sync and the
    # firmware read for a radio that is usually back within seconds. The reasoning
    # for exit-1 was written for a vanished USB device and it is still right about
    # one; it was never right about a node someone walked out of range of.
    #
    # The exit stays as the backstop rather than being replaced by the retry
    # loop, and that ordering is the load-bearing part: a recovery that never
    # gives up is how a dead radio becomes a process that looks healthy forever,
    # which is the exact outcome exit-1 was chosen to prevent.
    if self._connection_lost:
      self.stop()
      sys.exit(1)




  def _open_interface(self) -> MeshInterface:
    """Construct the interface CONNECTION_MODE asked for.

    **The tcp and ble imports are inside their branches on purpose.** A serial
    install pays nothing for either, and more to the point `ble_interface` imports
    bleak, which needs a working Bluetooth stack to import cleanly on some hosts —
    a headless Pi archiving over USB should not fail to start over a radio it does
    not use. `transport` already validated the arguments and named them the way
    each constructor wants, so there is nothing to decide here.
    """
    if self.transport.mode == TCP:
      self._check_tcp_reachable()
      from meshtastic.tcp_interface import TCPInterface
      return TCPInterface(**self.transport.kwargs)

    if self.transport.mode == BLE:
      from meshtastic.ble_interface import BLEInterface
      return BLEInterface(**self.transport.kwargs)

    return SerialInterface(**self.transport.kwargs)




  def _check_tcp_reachable(self) -> None:
    """Prove the meshtasticd port accepts a connection, within TCP_CONNECT_TIMEOUT.

    A throwaway socket, opened and closed, purely so the *timeout* is ours — see
    TCP_CONNECT_TIMEOUT for why the library's own connect cannot be bounded from
    out here. Raises OSError (socket.timeout is one), which is already in
    `_connect_errors`, so a failure lands in start()'s existing warning-and-exit
    path with nothing special to say about it.

    The port can of course close again between this probe and the real connect a
    moment later. That race is not worth closing: what follows it is the
    library's own OSError on the same path, which is exactly where an unreachable
    host was always going to end up.
    """
    host = self.transport.kwargs["hostname"]
    port = self.transport.kwargs["portNumber"]

    with socket.create_connection((host, port), timeout=TCP_CONNECT_TIMEOUT):
      pass




  def _connect_errors(self) -> tuple[type[BaseException], ...]:
    """What a failure to open this transport is allowed to look like.

    **BLE's failures are `BLEInterface.BLEError`, which is neither an OSError nor
    a bleak type** — `ble_interface.py:33` derives it straight from Exception. It
    is what `find_device` raises for DEVICE_NOT_FOUND and MULTIPLE_DEVICES
    (`:169-176`), which is what a wrong or stale BLE_ADDRESS actually produces, so
    catching only BleakError here would miss the common case entirely and hand the
    operator a traceback instead of the one-line warning above. BleakError stays in
    the tuple for what escapes the library's own translation of it.

    Both imports are lazy, and in ble mode only, for the same reason
    `_open_interface` defers them.
    """
    errors: tuple[type[BaseException], ...] = (OSError, MeshInterface.MeshInterfaceError)

    if self.transport.mode == BLE:
      from bleak.exc import BleakError
      from meshtastic.ble_interface import BLEInterface
      errors += (BLEInterface.BLEError, BleakError)

    return errors




  def stop(self) -> None:
    logging.info("Stopping collector")

    # Raised before the interface comes down, because closing it fires the same
    # meshtastic.connection.lost event an unplug does — the library's reader
    # thread exits either way and cannot say which it was. Without this flag
    # every deliberate stop wrote _on_connection_lost's ERROR into the journal,
    # so the one line that is supposed to mean "the radio vanished" also meant
    # "somebody ran systemctl restart", which is exactly the boy-who-cried-wolf
    # problem log levels exist to prevent. The event arrives on the reader
    # thread mid-close(), strictly after this assignment, so the handler always
    # sees it in time.
    self._stopping = True
    self._running = False

    # Before the interface and the database, so a client cannot be granted a
    # send that then has nothing to send with.
    if self.control_server is not None:
      try:
        self.control_server.stop()
      except Exception:
        logging.exception("Failed to close the control socket")
      self.control_server = None

    # **A BLE link that is already down must be abandoned, never closed.** Closing
    # it waits on an event loop that a dropped BLE link has usually already
    # deadlocked (the chain is written out at `_watch_for_ble_disconnect`), and
    # that wait does not time out — so a shutdown arriving in the window between
    # the drop and the main loop noticing it would hang here, `systemctl stop`
    # would sit out its timeout, and the process would leave by SIGKILL. The
    # `except` below cannot help with that; a block is not an exception.
    if self.interface is not None and self.transport.mode == BLE:
      down = self._ble_link_down_reason()
      if down is not None:
        logging.info("Abandoning the %s interface: %s", self.transport.label, down)
        self._abandon_interface()

    if self.interface:
      try:
        self.interface.close()
      except Exception:
        # A vanished device can make close() itself raise; the database close
        # below and the nonzero exit still have to happen.
        logging.exception("Failed to close the %s interface", self.transport.mode)

    if self._reconnect_count:
      logging.info(
        "Reopened %s %d time%s this run",
        self.transport.label,
        self._reconnect_count,
        "" if self._reconnect_count == 1 else "s",
      )

    self.storage.close()




  def _publish_policy(self) -> None:
    """Record the collector's effective policy in meta, so readers can describe
    the archive without keeping their own copy of this configuration.

    Republished on every startup. See the meta table comments in schema.sql.
    """
    stores_dms = bool(Config.get("STORE_DIRECT_MESSAGES", False))

    policy = {
      "max_messages": str(Config.get("MAX_MESSAGES")),
      "max_direct_messages": str(Config.get("MAX_DIRECT_MESSAGES")),
      "stores_direct_messages": "true" if stores_dms else "false",
      "primary_channel": str(Config.get("PRIMARY_CHANNEL", 0)),
      "tracked_channels": ",".join(str(idx) for idx in self.tracked_channels),
      # Republished from the live server rather than from the setting, so this
      # says a socket is actually being served and not merely that somebody
      # asked for one. If the socket failed to bind, this reads false.
      "accepts_transmit": "true" if self.control_server is not None else "false",
    }

    self.storage.set_meta_values(policy)
    logging.info("Published collector policy: %s", policy)




  def _publish_firmware(self) -> None:
    """Record the device's firmware version and release channel in meta.

    The version is the device's own answer — DeviceMetadata arrives during the
    config download the SerialInterface constructor waits out — but the channel
    is not: firmware announces '2.7.26.54e0d8d' and has no idea whether that
    build shipped as a Beta or an Alpha. That is a fact about Meshtastic's
    release listing, not about the radio, so it is looked up there (see
    FIRMWARE_RELEASES_URL) and archived beside the version for readers to print.

    Republished on every startup for the same reason the policy keys are: these
    describe the device as it is being read *now*, and a reflash — the whole
    reason a reader wants this line — only becomes visible if the restart that
    follows it rewrites the keys. Both are written even when empty, so a value
    from a previous device or an earlier build cannot outlive whatever reported
    it.

    Nothing here may stop the collector. A startup that archives packets but
    cannot name its firmware is degraded; one that dies over a GitHub timeout
    is broken. The lookup wraps its own failures, and metadata that never
    arrived reads as '' — old firmware genuinely never sends it.
    """
    metadata = getattr(self.interface, "metadata", None)
    version = getattr(metadata, "firmware_version", "") or ""

    channel = self._lookup_firmware_channel(version) if version else ""

    self.storage.set_meta_values({
      "firmware_version": version,
      "firmware_channel": channel,
    })

    if version:
      logging.info("Device firmware %s (%s)", version, channel or "channel unknown")
    else:
      logging.warning(
        "Device did not report a firmware version; readers will say unknown"
      )




  def _lookup_firmware_channel(self, version: str) -> str:
    """Which channel `version` shipped on: 'beta', 'alpha', or '' for cannot-say.

    Read off the release title — Meshtastic names every release 'Meshtastic
    Firmware <version> Beta' or '... Alpha' — rather than inferred from GitHub's
    prerelease flag. The title is the label the project actually stamps on the
    release; the flag agrees with it today, but if the two ever part, the title
    is what a person comparing against the releases page will call correct.

    '' on any failure — network down, rate-limited, a build the listing has
    never heard of (self-compiled, or older than the page reaches back) —
    because a wrong tag is worse than none: the reader prints the version
    untagged and is not lying to anyone. And failure is one WARNING line, not a
    traceback: an unreachable GitHub during startup is weather, not a defect in
    this process.
    """
    try:
      with urllib.request.urlopen(
        FIRMWARE_RELEASES_URL, timeout=FIRMWARE_LOOKUP_TIMEOUT
      ) as response:
        releases = json.load(response)

      for release in releases:
        if release.get("tag_name") != f"v{version}":
          continue
        words = (release.get("name") or "").split()
        label = words[-1].lower() if words else ""
        return label if label in ("alpha", "beta") else ""

      logging.info(
        "Firmware %s is not in Meshtastic's release listing; "
        "its channel stays unsaid", version
      )
    except Exception as error:
      logging.warning(
        "Firmware channel lookup failed (%s); the version will read untagged",
        error,
      )

    return ""




  def _warn_if_mqtt_proxy_expected(self) -> None:
    """Say so if the device is relying on its attached client to reach MQTT.

    **The device asks whoever is attached as its client to do its publishing, and
    this collector does not.** The proxy is a property of the client connection
    rather than of the cable, so this holds identically over TCP and BLE.
    With `mqtt.proxy_to_client_enabled` set, the firmware sends
    a `mqttClientProxyMessage` for its own traffic and for anything it gateways;
    `meshtastic-python` re-publishes that on a pubsub topic and speaks to no broker,
    and nothing here subscribes. So the packets go out over LoRa as normal and their
    MQTT copies are dropped — silently, and for as long as this process is attached.

    Found on the live mesh, where the node had been gatewaying an NCMesh channel and
    stopped without anything saying so. Archiving is what this project does and
    running an MQTT client is not; what it owes the operator is to not take a
    behaviour away without mentioning it.

    Read from the device rather than inferred from traffic, so the warning appears at
    startup rather than the first time a packet happens to need proxying. Wrapped
    because a device that has not sent its module config yet, or a firmware without
    the field, is not a reason to fail to start.
    """
    try:
      mqtt = self.interface.localNode.moduleConfig.mqtt
      proxying = bool(mqtt.enabled and mqtt.proxy_to_client_enabled)
    except Exception:
      return

    if not proxying:
      return

    logging.warning(
      "This device has MQTT client proxying on, and mesh-collector does not "
      "provide it: while this collector is its attached client the device's MQTT "
      "uplink is off, for its own traffic and for anything it gateways. LoRa is "
      "unaffected. To stop relying on it, turn off Proxy to Client in the device's "
      "MQTT module settings (`meshtastic --set mqtt.proxy_to_client_enabled false`); "
      "to keep it, run a client that proxies instead of this collector."
    )




  def _start_control_server(self) -> None:
    """Bind the control socket, if transmitting was asked for.

    With ENABLE_TX off this does nothing at all — no socket, no import, no
    thread — which is the point: an archive-only collector is the same process
    it was before this existed.
    """
    if not self.transmit_enabled:
      logging.info("Transmitting is disabled (ENABLE_TX); no control socket")
      return

    # Imported here rather than at module scope so mesh-link is only needed by
    # an install that actually transmits. It is an optional dependency for the
    # same reason RxOnly does not install meshtastic: what a process cannot
    # import, it cannot be talked into doing.
    try:
      from mesh_link import ControlServer
    except ImportError as e:
      raise RuntimeError(
        "ENABLE_TX is on but mesh-link is not installed. Install this project "
        "with its transmit extra — `uv sync --extra tx` — or turn ENABLE_TX off."
      ) from e

    server = ControlServer(
      Config.get("CONTROL_SOCKET_PATH") or None,
      logger=logging.getLogger("mesh_collector.control"),
    )
    server.start()
    self.control_server = server

    logging.warning(
      "Transmitting is ENABLED. Anyone who can write to %s can send on this "
      "radio; the socket is mode 0600 and that is the whole authorization model.",
      server.socket_path,
    )




  def _main_loop(self) -> None:
    """Drain control requests, or idle if there is no socket.

    This is the only thread that transmits and the only one that writes an
    outbound row, which is what serializes sends without a lock around the
    interface. The socket's own threads parse and validate; they never get here.
    """
    while self._running:
      if self.control_server is None:
        time.sleep(1)
        self._check_link()
        continue

      pending = self.control_server.poll(timeout=CONTROL_POLL_INTERVAL)
      self._check_link()
      if pending is None:
        continue

      self._answer_control_request(pending)




  def _check_link(self) -> None:
    """One pass of link supervision, cheapest check first.

    Two things happen here and the order matters. `_supervise_ble_link` reads
    state that is already sitting in memory and costs nothing, so it runs every
    pass; `_check_liveness` sends a packet and is off unless an operator asked for
    it. When supervision acts — because the link went and was rebuilt, or because
    it went and this process is now on its way out — there is nothing for the
    silence watchdog to measure, so it is skipped for that pass.
    """
    if self._supervise_ble_link():
      return

    self._check_liveness()




  def _watch_for_ble_disconnect(self) -> None:
    """Take over bleak's disconnect callback, because the library's deadlocks.

    **This is the fix for the whole silent-death mode, and it is worth reading
    the chain before touching it.** meshtastic connects with
    `disconnected_callback=lambda _: self.close()` (`ble_interface.py:194`).
    bleak's CoreBluetooth backend delivers that callback by posting
    `did_disconnect_peripheral` to the BLEClient's own asyncio loop
    (`CentralManagerDelegate.py:177`) and calling it there (`:402`), so it runs
    **on the event loop thread**. `BLEInterface.close()` immediately calls
    `MeshInterface.close()`, which calls `_sendDisconnect()`, which reaches
    `write_gatt_char` → `async_await` → `run_coroutine_threadsafe(...).result(None)`:
    a wait, on the loop thread, for a coroutine only that loop can run.

    It never returns. Three things follow, and all three were measured on hardware
    before they were understood:

    - the event loop is dead from the first disconnect onward, so every later GATT
      call from any thread blocks forever — the read loop, the liveness probe, and
      our own `close()` alike;
    - `close()` never reaches its `self._disconnected()` (`:271`), which is the
      **only** publisher of meshtastic.connection.lost on BLE, so a dropped BLE
      link reports nothing and `_on_connection_lost` never runs;
    - `close()` never reaches its `atexit.unregister` (`:267`) either, so the
      exit handler registered at `:97` — `client.disconnect`, on the dead loop —
      is still armed, and a later `sys.exit(1)` hangs in atexit instead of
      exiting. The nonzero-exit backstop is booby-trapped by the same deadlock it
      is supposed to catch.

    Replacing the callback with one that only stores a flag removes all three at
    the source: the loop stays alive, so the interface can still be closed, and
    the drop is known within a pass of the main loop.

    Reaching `_backend` is the one private attribute this file depends on.
    `set_disconnected_callback` is a documented method of bleak's base client
    (`backends/client.py:63`) and the CoreBluetooth backend reads the callback at
    call time rather than capturing it at connect time, so installing it after
    construction works. If a future bleak moves it, this degrades to the polling
    below — which is why detection does not rest on this alone.
    """
    if self.transport.mode != BLE or self.interface is None:
      return

    self._ble_disconnected = False

    backend = getattr(
      getattr(getattr(self.interface, "client", None), "bleak_client", None),
      "_backend",
      None,
    )
    if backend is None or not hasattr(backend, "set_disconnected_callback"):
      logging.warning(
        "Could not install the BLE disconnect hook on %s; falling back to "
        "polling the link, which still detects a drop but leaves the library's "
        "own callback free to deadlock its event loop",
        self.transport.label,
      )
      return

    backend.set_disconnected_callback(self._on_ble_disconnected)




  def _on_ble_disconnected(self) -> None:
    """The peripheral went away. Runs on bleak's event loop thread.

    **It stores one bool and returns, and that is the entire contract.** This is
    the callback whose library-supplied version deadlocks the loop it runs on
    (see `_watch_for_ble_disconnect`); anything that blocks here reintroduces
    exactly that bug, and anything that raises escapes into bleak's callback
    dispatch. The journal line and every decision about what to do next belong to
    the main loop, which reads this flag in `_ble_link_down_reason` — that is also
    why there is no logging call here, since a log write is I/O and I/O is what
    must not happen on this thread.
    """
    self._ble_disconnected = True




  def _ble_link_down_reason(self) -> Optional[str]:
    """Why the BLE link looks dead, or None if it looks fine.

    Three signals, none of which touches the event loop, because on a dropped BLE
    link the event loop may already be wedged and anything that waits on it never
    comes back:

    1. **Our disconnect hook fired.** The strongest signal — the host's Bluetooth
       stack said the peripheral is gone.
    2. **`is_connected` is false.** A public bleak property that is a plain state
       read on both backends that matter: CoreBluetooth compares
       `_peripheral.state()` (`corebluetooth/client.py:158-164`) and BlueZ returns
       a bool kept current by DBus signals (`bluezdbus/client.py:543-550`).
       Neither awaits anything, so this answers even when the loop is dead — and
       it is the signal that survives the hook failing to install.
    3. **The read thread has exited.** bleak fails every pending delegate future
       with `BleakError("disconnected")` before it calls the disconnect callback
       (`corebluetooth/client.py:124-132`), and meshtastic re-raises a non-"Not
       connected" BleakError straight out of `_receiveFromRadioImpl`
       (`ble_interface.py:218-222`), so an in-flight read kills the thread. This
       also catches the unprovoked `TimeoutError` death that was observed with no
       disconnect behind it at all.

    Cheap first, and each one is sufficient on its own. None is sufficient
    *alone*: (1) needs the hook, (2) is the one that always works, and (3) fires
    only when a read was in flight — on a quiet mesh the thread is asleep in its
    `should_read` poll and stays alive over a drop.

    **Anything unreadable is reported as healthy.** A missing attribute means the
    library changed shape, and a supervisor that tears down a working link because
    it could not find a field is worse than one that misses a drop the other two
    signals will catch a moment later.
    """
    if self._ble_disconnected:
      return "the host's Bluetooth stack reported the peripheral disconnected"

    client = getattr(self.interface, "client", None)
    bleak_client = getattr(client, "bleak_client", None)
    if bleak_client is not None:
      try:
        connected = bleak_client.is_connected
      except Exception:
        connected = True
      if not connected:
        return "the peripheral is no longer connected"

    # **The sentinel is doing real work here.** `_receiveThread` is legitimately
    # None on a closed interface (`ble_interface.py:264`), so None has to mean
    # "gone" — but an *absent* attribute means the library renamed the field, and
    # reading that as "gone" would tear a healthy link down on the first pass of
    # every main loop and spend the whole reconnect budget in five seconds.
    thread = getattr(self.interface, "_receiveThread", _UNREADABLE)
    if thread is _UNREADABLE:
      return None
    if thread is None or not thread.is_alive():
      return "the library's BLE read thread has exited"

    return None




  def _supervise_ble_link(self) -> bool:
    """Notice a dropped BLE link and act on it. True if it acted.

    **BLE is the exception to this collector's exit-1 reconnect policy, and this
    is where the exception lives.** Serial and TCP still exit and let the service
    manager reconnect — see the note at `start()`'s exit path — because on those
    a drop is rare, reported immediately, and cheap to restart out of. BLE is
    none of the three: `BLEInterface` has no reconnect, the library reports
    nothing at all, and a restart re-runs channel sync, the node sync and the
    firmware read for a radio that is usually back in seconds.

    The exit is still the backstop, not a thing that was removed. Recovery is
    bounded by BLE_RECONNECT_ATTEMPTS, and running out falls through to exactly
    the nonzero exit that was there before.
    """
    if self.transport.mode != BLE or self.interface is None:
      return False

    # A deliberate stop tears the interface down on another thread, and a reopen
    # in progress is allowed to look disconnected — that is what it is.
    if self._stopping or self._reconnecting:
      return False

    reason = self._ble_link_down_reason()
    if reason is None:
      return False

    logging.error("Link lost on %s: %s", self.transport.label, reason)

    self._abandon_interface()

    if not self._reconnect_ble():
      self._connection_lost = True
      self._running = False

    return True




  def _abandon_interface(self) -> None:
    """Drop a BLE interface without closing it, and disarm what it left behind.

    **Not calling `close()` is the point.** On a dropped BLE link the library's
    own disconnect callback is usually already parked inside `close()` on the
    event loop thread, and `close()` is not re-entrant across that: calling it
    from here would queue a second coroutine on a loop that will never run
    another one, and block the main thread forever alongside it. Even where the
    disconnect hook prevented that deadlock, the peripheral is gone, so the
    disconnect packet `MeshInterface.close()` tries to send has nowhere to go.

    What does have to happen is the atexit handler. `BLEInterface` registers
    `client.disconnect` at `ble_interface.py:97` and only ever unregisters it
    inside `close()` (`:267`) — which is precisely the call being skipped. Left
    armed, it runs at interpreter shutdown, waits on the dead loop, and hangs the
    process in `sys.exit(1)`: the nonzero exit that the whole recovery design
    keeps as its backstop would never actually exit. One handler per abandoned
    interface, so a soak that reconnects forty times arms forty of them.

    `_want_receive = False` asks the read thread to stop if it is still alive. It
    is best-effort — a thread blocked in a GATT read on the dead loop will not
    see it, and there is no way to make it — but on the quiet-mesh path, where
    the thread is sleeping in its `should_read` poll, this is what ends it.

    The heartbeat timer has to be cancelled here for the same reason as the
    atexit handler: `MeshInterface._startHeartbeat` arms a 300s
    `threading.Timer` (`mesh_interface.py:1170-1180`) that re-arms itself
    forever, and the only thing that ever cancels it is `close()` (`:145-146`) —
    again the call this method exists to skip. Left armed it fires about five
    minutes later against a client that has been torn down and dies with an
    unhandled traceback; measured, that is a 45-line `BleakError: Service
    Discovery has not been performed yet` in the journal roughly five minutes
    after *every* BLE recovery. Nothing breaks, but it is exactly the kind of
    thing that gets read as the failure. This cancel is best-effort for its own
    second reason: the timer's callback sets `self.heartbeatTimer = None` before
    building its replacement, so a cancel landing inside that window reads None
    and misses. That is a millisecond hole every 300s, and the cost of losing
    the race is one log traceback, not a fault.
    """
    interface = self.interface
    self.interface = None
    if interface is None:
      return

    handler = getattr(interface, "_exit_handler", None)
    if handler is not None:
      atexit.unregister(handler)

    try:
      interface._want_receive = False  # pylint: disable=protected-access
    except Exception:
      logging.debug("Could not stop the read loop on the abandoned interface")

    try:
      timer = getattr(interface, "heartbeatTimer", None)
      if timer is not None:
        timer.cancel()
    except Exception:
      logging.debug("Could not cancel the heartbeat timer on the abandoned interface")




  def _reconnect_ble(self) -> bool:
    """Rebuild the BLE interface, with backoff, up to the configured cap.

    True if the link is back and the caller should carry on; False if it is out
    of attempts and the caller should take the exit.

    **A new object every time.** `BLEInterface` cannot be reconnected — its read
    loop sets `_want_receive = False` and returns, and nothing in the class puts
    it back — so recovery means construction, with the same 10s scan and the same
    `_connect_errors` tuple the first connect used.

    **This blocks the main loop, deliberately.** The control socket is not drained
    while a reopen is in progress, so a queued send waits out the backoff. That is
    the honest behaviour: there is no radio to send with, and answering a transmit
    request during a reconnect would mean accepting work that cannot be done.

    **What this does not redo, on purpose.** Channel sync, the initial node sync
    and the firmware read do not run again. BLE_ADDRESS pins the radio, so the
    device that answers a reopen is the device that dropped, and its channels are
    the ones already in the archive. The honest cost is that a node reconfigured
    or reflashed *while the link was down* keeps its old channel and firmware rows
    until the next restart — a rare trade for not putting three more ways to fail
    inside a recovery path, and the reason it is written down rather than assumed.

    Expect **two** journal lines per recovery. `_on_connection_established` fires
    on a rebuild — unlike at startup, where the interface is constructed before
    that subscription exists, which is why a first connect never logs one — and
    this method logs its own besides. They are not redundant: the library's line
    says a link came up, and this one says it was a recovery, which attempt won,
    and how many times that has happened since the process started.
    """
    attempts = self.transport.reconnect_attempts
    if not attempts:
      return False

    self._reconnecting = True
    try:
      delay = self.transport.reconnect_backoff

      for attempt in range(1, attempts + 1):
        # **The first attempt is immediate, and the backoff sits between the
        # rest.** The drop this is most often answering is a node that walked out
        # of range and back, where the link is available again the moment anyone
        # asks; and `BLEInterface` opens with a 10s scan regardless, so even the
        # immediate attempt gives the stack time to settle. Doubling after the
        # wait rather than after the failure is what makes the first gap the
        # configured one instead of twice it.
        if attempt > 1:
          if not self._sleep_before_retry(delay):
            return False
          delay = min(delay * 2, RECONNECT_BACKOFF_CAP)

        logging.info(
          "Reopening %s (attempt %d of %d)", self.transport.label, attempt, attempts
        )

        # Cleared before the attempt, not after: the hook belonging to the old
        # client can still fire while this one is being built, and a flag set by
        # a dead link must not be read as the new link having dropped.
        self._ble_disconnected = False

        try:
          self.interface = self._open_interface()
        except self._connect_errors() as error:
          # WARNING, not ERROR, for the reason start()'s open gives: the error
          # was the drop, reported once when it happened, and a retry that did
          # not work is the same condition continuing.
          logging.warning(
            "Could not reopen %s (%s)", self.transport.label, error
          )
          self.interface = None
          continue

        self._watch_for_ble_disconnect()
        self._last_activity = time.time()
        self._reconnect_count += 1

        logging.info(
          "Link re-established on %s after %d attempt%s (%d since startup)",
          self.transport.label,
          attempt,
          "" if attempt == 1 else "s",
          self._reconnect_count,
        )
        return True
    finally:
      self._reconnecting = False

    logging.error(
      "Gave up reopening %s after %d attempts; exiting so the service manager "
      "keeps retrying until the radio returns",
      self.transport.label,
      attempts,
    )
    return False




  def _sleep_before_retry(self, delay: int) -> bool:
    """Wait out the backoff in one-second slices. False if we should stop trying.

    Sliced rather than slept in one go so that a SIGTERM arriving mid-backoff is
    acted on now instead of up to a minute later — the signal handler runs
    `stop()`, which clears `_running`, and this is what notices.
    """
    for _ in range(delay):
      if not self._running:
        return False
      time.sleep(1)

    return self._running




  def _check_liveness(self) -> None:
    """Probe a silent link, if LIVENESS_TIMEOUT asked us to. Off by default.

    **This is not filling a hole the library ignores; it makes an existing
    detection prompt.** A dropped TCP connection heals inside TCPInterface
    (`_readBytes`/`_writeBytes` → `_reconnect()`, `tcp_interface.py:137-180`), a
    refused reconnect raises out of the reader thread into `_disconnected()` and
    so into our exit-1, and even a genuinely half-open socket is eventually
    caught by the library's own 300s heartbeat (`mesh_interface.py:1170-1190`)
    when that write exhausts the kernel's retransmits — on the order of fifteen
    minutes. What this does is notice in LIVENESS_TIMEOUT seconds instead, and
    say which transport it was.

    Off by default for every mode, serial included: pyserial's reader death is
    already immediate and reliable, so there is nothing here for it to improve.
    It exists for TCP over a network that can go away quietly.

    The send is `sendHeartbeat()` — the same no-op keepalive the library uses. An
    exception means the link is gone, and it is routed through the existing
    _connection_lost path rather than a second shutdown of its own, so a watchdog
    loss and an unplug exit by exactly the same road.

    **This is not BLE's drop detector and must not be described as one.** It was
    written as though it were, and on hardware it did the opposite: the probe's
    write blocked instead of raising and parked the whole collector inside this
    method, 155 seconds of dead link with nothing noticed and nothing exited.
    `_probe_link` is the answer to the blocking half — it bounds the wait this
    file cannot bound any other way — but the detector is
    `_ble_link_down_reason`, which costs nothing and cannot false-positive.

    That second property is why this stays off by default on BLE too. The trigger
    here is *silence*, and a minute of silence is ordinary on a quiet mesh: a 60s
    timeout was measured firing at +61.2s against a link that was perfectly
    healthy. A probe that mostly reports on links that were fine is a poor trade
    for a watchdog that only has to say something when something is wrong.

    **On BLE a failed probe reports rather than exits.** It sets the same flag the
    disconnect hook sets, so the next pass of `_supervise_ble_link` handles it
    like any other drop — reopen, and exit only if that runs out of attempts.
    Serial and TCP keep the original behaviour, which is to stop the loop and let
    the exit path fire.
    """
    timeout = self.transport.liveness_timeout
    if not timeout or self.interface is None:
      return

    # Read once. The reader thread can store into _last_activity between the
    # comparison and the log, and a heartbeat sent one interval early costs
    # nothing while an inconsistent pair of reads would be a puzzle later.
    silent_for = time.time() - self._last_activity
    if silent_for < timeout:
      return

    logging.warning(
      "Nothing heard on %s for %.0fs (LIVENESS_TIMEOUT=%ds); probing the link",
      self.transport.label,
      silent_for,
      timeout,
    )

    # Stamped before the probe, not after: a link that is genuinely down but
    # whose send blocks would otherwise re-probe on every pass of this loop.
    self._last_activity = time.time()

    failure = self._probe_link()
    if failure is None:
      return

    if self.transport.mode == BLE:
      logging.warning("Liveness probe failed on %s (%s)", self.transport.label, failure)
      self._ble_disconnected = True
      return

    logging.error(
      "Liveness probe failed on %s (%s); shutting down so the service "
      "manager restarts this collector",
      self.transport.label,
      failure,
    )
    self._connection_lost = True
    self._running = False




  def _probe_link(self) -> Optional[str]:
    """Send a heartbeat with a deadline of our own. None if the radio answered.

    **The thread is not decoration.** `sendHeartbeat` offers no timeout and, on
    BLE, ends in a `future.result(None)` that cannot expire; putting it on a
    thread is the only way to stop waiting for it, because there is nothing to
    interrupt and nothing to cancel. A probe that overruns is abandoned, not
    killed — Python cannot kill a thread — so it stays parked on whatever it was
    parked on, holding a little memory, until the process ends.

    That leak is real and it is bounded on purpose. A probe only overruns on a
    link that is already gone, and a gone link is about to be either reopened by
    `_reconnect_ble` — which is capped — or exited out of. What must not happen
    is an unbounded retry policy above an unbounded probe, which is how a
    collector accumulates wedged threads for days and still reports itself
    healthy.
    """
    outcome: list[str] = []

    def probe() -> None:
      try:
        self.interface.sendHeartbeat()
      except Exception as error:  # pylint: disable=broad-except
        outcome.append(str(error) or error.__class__.__name__)
      else:
        outcome.append("")

    thread = threading.Thread(target=probe, name="LivenessProbe", daemon=True)
    thread.start()
    thread.join(LIVENESS_PROBE_DEADLINE)

    if thread.is_alive():
      return f"no answer within {LIVENESS_PROBE_DEADLINE:.0f}s"

    return outcome[0] if outcome and outcome[0] else None




  def _answer_control_request(self, pending: PendingRequest) -> None:
    """Act on one request and answer it, whatever happens.

    BaseException rather than Exception on purpose. meshtastic's _sendPacket
    calls our_exit() — which is sys.exit() — for a destination it cannot
    resolve, and SystemExit does not derive from Exception. mesh-link validates
    destinations to the two forms that never reach that code, so this is the
    second line of defence rather than the first, but a library that can end the
    process is not one to leave a gap for.
    """
    from mesh_link import (
      ERR_INTERNAL,
      ERR_SEND_FAILED,
      SendTextRequest,
      StatusRequest,
    )

    try:
      request = pending.request

      if isinstance(request, StatusRequest):
        pending.respond(self._transmit_status())
        return

      if isinstance(request, SendTextRequest):
        self._handle_send_request(pending, request)
        return

      pending.fail(
        ERR_INTERNAL,
        f"The collector does not know how to handle {type(request).__name__}.",
      )

    except SystemExit as e:
      logging.error("The radio library tried to exit the process during a send: %s", e)
      pending.fail(
        ERR_SEND_FAILED,
        "The radio library rejected that request outright. Nothing was sent.",
      )
    except BaseException:
      logging.exception("Unhandled error answering a control request")
      pending.fail(ERR_INTERNAL, "The collector failed to handle that request.")




  def _transmit_status(self) -> dict:
    """What a client needs before offering somebody a compose box."""
    return {
      "local_node_id": self.local_node_id,
      "accepts_transmit": True,
      "tracked_channels": list(self.tracked_channels),
      "primary_channel": int(Config.get("PRIMARY_CHANNEL", 0)),
      "stores_direct_messages": bool(Config.get("STORE_DIRECT_MESSAGES", False)),
      "schema_version": self.storage.get_meta("schema_version"),
    }




  def _handle_send_request(
    self, pending: PendingRequest, request: SendTextRequest
  ) -> None:
    """Transmit one message and record what was sent.

    The archive has to be written here rather than left to the receive path.
    LoRa is half-duplex so the radio cannot hear itself, the meshtastic library
    does not loop sent packets back to meshtastic.receive, and firmware dedup
    drops the mesh rebroadcast of a packet this node originated. Nothing
    observes our own transmissions, so the send path records them or nothing
    does.
    """
    from mesh_link import ERR_CHANNEL_NOT_TRACKED, ERR_SEND_FAILED

    if self.interface is None:
      pending.fail(ERR_SEND_FAILED, "The collector has no interface open.")
      return

    is_direct = request.is_direct

    # A channel message the collector does not archive would be written into a
    # channel with no row in `channels`, which every reader joins against. Refuse
    # rather than create a message nothing can display.
    if not is_direct and not self._should_log_channel(request.channel_index):
      pending.fail(
        ERR_CHANNEL_NOT_TRACKED,
        f"Channel {request.channel_index} is not tracked by this collector; "
        f"it archives {self.tracked_channels or 'no channels'}.",
      )
      return

    want_ack = request.resolve_want_ack()

    # A reaction is a reply carrying a flag, so asked for without a reply_to it is
    # not something the protocol can express. The flag is dropped rather than
    # honoured, and the archive records the 0 that matches what actually went out
    # instead of a 1 describing a packet nobody sent.
    is_reaction = bool(request.emoji) and request.reply_to is not None

    try:
      if is_reaction:
        packet = self._transmit_reaction(request, want_ack)
      else:
        packet = self.interface.sendText(
          text=request.text,
          destinationId=request.destination,
          channelIndex=request.channel_index,
          wantAck=want_ack,
          replyId=request.reply_to,
        )
    except Exception as e:
      logging.exception("Send failed for destination %s", request.destination)
      pending.fail(ERR_SEND_FAILED, f"The radio refused the message: {e}")
      return

    # sendText returns the protobuf MeshPacket it built, with id already
    # assigned. That id is the one the mesh will carry, and messages.message_id
    # is UNIQUE with reply_to chains referencing it, so it has to be captured
    # here rather than invented.
    message_id = int(getattr(packet, "id", 0) or 0)

    if not message_id:
      logging.error("Send returned no packet id; not archiving %r", request.text[:60])
      pending.respond({
        "message_id": None,
        "archived": False,
        "reason": "The radio returned no packet id, so there is nothing to record.",
      })
      return

    archived, rx_time = self._archive_outbound(
      request, message_id, is_direct, is_reaction
    )

    logging.info(
      "TX %s %s: %s",
      request.destination if is_direct else f"CH{request.channel_index}",
      "(archived)" if archived else "(not archived)",
      request.text[:100],
    )

    pending.respond({
      "message_id": message_id,
      "archived": archived,
      "destination": request.destination,
      "channel_index": None if is_direct else request.channel_index,
      "want_ack": want_ack,
      "rx_time": rx_time,
    })




  def _transmit_reaction(
    self, request: SendTextRequest, want_ack: bool
  ) -> mesh_pb2.MeshPacket:
    """Send a reaction, which `sendText` cannot express.

    The flag that makes a reply a reaction is `emoji` on meshtastic's `Data`, and
    neither `sendText` nor the `sendData` under it takes an argument for it — 2.7.11
    passes `replyId` through and stops there. The field is in the protobuf, so the
    only thing missing is a way to set it, and that is what this is.

    Deliberately not a rewrite of the ordinary path. `sendText` still sends every
    message that is not a reaction, because this reaches past the library's public
    surface into `_generatePacketId` and `_sendPacket`, and confining that to
    reactions keeps a version of meshtastic that moves those from breaking sends
    outright — it would break tapbacks, and the rest would carry on.

    Mirrors what `sendData` does with the same inputs, priority included, so a
    reaction is an ordinary text packet in every respect except the one flag. The
    payload length check is the library's own, kept because this no longer runs
    through the code that raises it.
    """
    payload = request.text.encode("utf-8")
    if len(payload) > mesh_pb2.Constants.DATA_PAYLOAD_LEN:
      raise ValueError(
        f"A reaction of {len(payload)} bytes does not fit in one packet; "
        f"the limit is {mesh_pb2.Constants.DATA_PAYLOAD_LEN}."
      )

    packet = mesh_pb2.MeshPacket()
    packet.channel = request.channel_index
    packet.decoded.portnum = portnums_pb2.PortNum.TEXT_MESSAGE_APP
    packet.decoded.payload = payload
    packet.decoded.want_response = False
    packet.decoded.reply_id = request.reply_to
    packet.decoded.emoji = 1
    packet.id = self.interface._generatePacketId()

    # sendData's default, applied here because that default lives in a signature
    # this call does not go through.
    packet.priority = 70

    return self.interface._sendPacket(
      packet, request.destination, wantAck=want_ack
    )




  def _archive_outbound(
    self,
    request: SendTextRequest,
    message_id: int,
    is_direct: bool,
    is_reaction: bool = False,
  ) -> tuple[bool, int]:
    """Write the row for a message this collector just sent.

    Returns whether a row was written, and the rx_time stamped on it, so the
    caller can report both without reaching back in for them.

    _handle_text_message cannot be reused for this. Its DM test is
    `to_id == self.local_node_id`, which is true for a direct message arriving
    and never for one being sent, where the local node is the sender.

    The receive-side metrics are NULL rather than zero, because there is no
    measurement to record: nothing was received. snr and rssi describe a signal
    this node heard, and hop_count is how far a packet travelled to reach us.
    """
    rx_time = int(time.time())

    try:
      if is_direct:
        if not Config.get("STORE_DIRECT_MESSAGES"):
          logging.debug("Sent DM not archived: direct message storage is off")
          return False, rx_time

        inserted = self.storage.insert_direct_message(
          message_id=message_id,
          from_node=self.local_node_id,
          to_node=request.destination,
          text=request.text,
          rx_time=rx_time,
          snr=None,
          rssi=None,
          reply_to=request.reply_to,
          via_mqtt=False,
          # 0 or 1, never NULL: this row's flag is known either way, because this
          # collector is the one that just chose it. NULL means "written before the
          # flag existed", which is a different claim and an untrue one for a row
          # being written now.
          #
          # `is_reaction` rather than `request.emoji`, so the row agrees with the
          # packet: a flag asked for without a reply_to was dropped on the way out,
          # and recording the request instead of the transmission would leave an
          # archive saying a reaction was sent when a plain message was.
          emoji=1 if is_reaction else 0,
        )
        return inserted, rx_time

      inserted = self.storage.insert_message(
        message_id=message_id,
        channel_index=request.channel_index,
        from_node=self.local_node_id,
        to_node="^all",
        text=request.text,
        rx_time=rx_time,
        hop_count=None,
        snr=None,
        rssi=None,
        reply_to=request.reply_to,
        via_mqtt=False,
        emoji=1 if is_reaction else 0,
      )
      return inserted, rx_time

    except Exception:
      logging.exception("Sent message %s but failed to archive it", message_id)
      return False, rx_time




  def _on_connection_established(self, interface=None) -> None:
    """The link is up. Logged for the transports that can come back by themselves."""
    logging.info("Connection established on %s", self.transport.label)
    self._last_activity = time.time()




  def _on_connection_lost(self, interface=None) -> None:
    """The node is gone and meshtastic's reader thread has exited.

    Without this the process idles forever archiving nothing — alive as far as
    the service manager can tell, so Restart= never fires. Stop the main loop
    instead; start() sees _connection_lost and turns that into a nonzero exit.
    meshtastic publishes this with interface=, tolerated the same way
    _on_receive tolerates it.

    **Serial and TCP reach here. BLE does not, and cannot.** This docstring used
    to claim the opposite — that bleak's disconnected_callback calls
    `BLEInterface.close()`, whose `self._disconnected()` (`ble_interface.py:271`)
    publishes this event, so a walked-away node could not leave the process
    alive-but-deaf. Every link in that chain is real and the conclusion is still
    false: the callback deadlocks partway through `close()`, on the event loop
    thread, long before it reaches the line that publishes. It was measured — a
    yanked link, a node reboot, and an unprovoked read-thread death, and this
    handler ran in none of the three.

    The chain is written out in full at `_watch_for_ble_disconnect`, which
    replaces that callback so it can no longer deadlock, and
    `_supervise_ble_link` is what a BLE drop actually reaches. This method still
    matters on BLE for one case: our own `close()`, which publishes on the way
    out and is why `_stopping` is checked below.
    """
    # Our own close fires this event too — the reader thread exits identically
    # whether the device vanished or stop() dismissed it, and only this process
    # knows which just happened. During a deliberate stop there is nothing to
    # report ("Stopping collector" already said it) and nothing to do: stop()
    # has the shutdown in hand, and setting _connection_lost here would turn a
    # clean exit into the nonzero one that exists for unplugs.
    if self._stopping:
      return

    # A retry that failed to connect publishes this from inside its own attempt:
    # `BLEInterface.__init__` closes the half-built object on error
    # (`ble_interface.py:74`), and BLE's close() publishes unconditionally. The
    # loss it names has already been reported by `_supervise_ble_link` and is
    # being acted on right now, so treating it as a new one would stop the
    # recovery loop with the very event that recovery is responding to.
    if self._reconnecting:
      return

    logging.error(
      "Connection lost on %s; shutting down so the service manager "
      "restarts this collector",
      self.transport.label,
    )
    self._connection_lost = True
    self._running = False




  def _on_receive(self, packet: dict, interface=None) -> None:
    """
    Handle incoming Meshtastic packets.
    Normalizes packet data and routes to appropriate handler.

    Runs on meshtastic's reader thread, and pypubsub does not catch listener
    exceptions — anything that escapes here unwinds that thread and the
    collector never hears another packet. So nothing may escape: an ordinary
    database error is logged and the packet is lost, not the process.
    """
    self._last_activity = time.time()

    try:
      self._handle_receive(packet)
    except Exception:
      logging.exception("Error handling received packet")




  def _handle_receive(self, packet: dict) -> None:
    if not self._running or not getattr(self.storage, "conn", None):
      return

    decoded = packet.get("decoded")
    if not decoded:
      # A decision, not an oversight: an undecryptable packet gets no row and
      # no last_seen bump. Its `from` is readable, so it *could* register a
      # node — but the device is on a channel it can't read, `nodes` would
      # stop meaning "heard and understood by this radio", and the firmware
      # skips its own NodeDB update for the same packets. NEIGHBORINFO_APP
      # and TRACEROUTE_APP are excluded on the same principle: they name
      # nodes this radio has not heard from directly.
      #
      # Dropping it silently was the oversight, and it cost an afternoon: with
      # no line at any level, a channel the radio has no key for reads as a
      # channel nobody is talking on. It is counted now, and the first packet
      # per channel hash says out loud what the counter means.
      # Defaulted to 0 like every other read of this field: the proto omits a
      # field sitting at its default, so an absent `channel` is hash 0x00 and
      # not a missing answer.
      channel_hash = packet.get("channel", 0)
      via_mqtt = bool(packet.get("viaMqtt", False))

      self._undecryptable_counts[channel_hash] = (
        self._undecryptable_counts.get(channel_hash, 0) + 1
      )
      if via_mqtt:
        self._undecryptable_mqtt += 1

      if channel_hash not in self._undecryptable_seen:
        self._undecryptable_seen.add(channel_hash)
        # The sender is named because the hash on its own cannot separate the two
        # diagnoses, and they have opposite fixes: a neighbour talking on a
        # channel we were never given (nothing to do) versus one of our own
        # channels whose PSK has drifted on one side (fix the key). Comparing the
        # hash against the hash of each configured channel answers it — and when
        # it comes back "none of ours", the sender is the only remaining thread
        # to pull.
        #
        # Deliberately "first from": this fires once per hash for the life of the
        # process, so it names whichever node opened the account, not the one
        # doing the most talking. Expect an id that appears nowhere else — the
        # branch returns without a row for exactly the reason above, so there is
        # no `nodes` entry and no name to look up, and that is not a bug in this
        # line.
        logging.info(
          "Undecryptable packet on channel hash 0x%02x, first from %s%s — this "
          "radio has no key for that channel; a wrong PSK looks exactly like this",
          channel_hash,
          _sender_id(packet) or "an unidentified sender",
          " (via mqtt)" if via_mqtt else "",
        )

      self._maybe_log_rx_summary()
      return

    from_node_id = _sender_id(packet)

    if not from_node_id:
      logging.debug("Packet without node_id; skipping")
      return

    portnum = decoded.get("portnum")

    # A read racing shutdown answers None rather than raising — Storage owns
    # that now, see get_meta's comment — and the `if not existing` branch below
    # already knows what to do with None.
    #
    # This row is read once and used twice: here, to decide whether to seed, and
    # again at the bottom of this function where it is handed to
    # _on_node_update. It used to be read twice per decoded packet, which is the
    # hottest path in the process. The rules for passing it on are on
    # _apply_node_update, and the seed call below is the reason they matter.
    existing = self.storage.get_node(from_node_id)

    if not existing and portnum != "NODEINFO_APP":
      # The device's node cache may hold this node's identity from a NODEINFO
      # the device heard before this collector started. Seed from it for any
      # portnum, so a node first heard via telemetry or position — or pruned
      # and come back — contributes immediately instead of waiting hours for
      # its next NODEINFO broadcast.
      #
      # A miss is not a reason to drop the packet. The radio only solicits
      # NodeInfo while its NodeDB has room (firmware MeshService.cpp:97, and
      # MAX_NUM_NODES is compile-time), so a node the cache doesn't name may
      # never be named — waiting for an identity loses it permanently. Fall
      # through and let the normalized packet create a bare row instead.
      self._seed_node_from_interface(from_node_id)

    if portnum == "TEXT_MESSAGE_APP":
      # Gating on portnum, not decoded["text"]: the library sets text for
      # RANGE_TEST_APP and DETECTION_SENSOR_APP too, and always populates
      # portnum (UNKNOWN_APP when the proto omits it) — so range-test
      # sequence numbers and sensor trigger strings stay out of `messages`.
      # Those two fall through as ordinary traffic: the node row updates,
      # the payload is not archived (a home for it would be a schema
      # decision, not taken here).
      self._handle_text_message(packet, from_node_id, existing)
      # No return: a message is also evidence the sender is alive, so its
      # receive metrics land on the node row below like any other packet.
    else:
      # Counted for the same reason the undecryptable drop above is. This radio
      # archives text and nothing else, so a channel carrying only position or
      # telemetry frames produces an empty message list while being perfectly
      # busy — and that is a different diagnosis from a wrong key, reached by
      # reading the same summary line.
      #
      # `channel` is a real index here: this packet decrypted, so the field is
      # the channel it came in on rather than the hash the branch above deals
      # with. UNKNOWN_APP stands in when the proto omits the portnum, which is
      # the library's own convention.
      key = (packet.get("channel", 0), portnum or "UNKNOWN_APP")
      self._nontext_counts[key] = self._nontext_counts.get(key, 0) + 1

    self._maybe_log_rx_summary()

    # Normalize packet into node_data shape
    normalized = {
      "user": {"id": from_node_id},
      "decoded": decoded,
      "snr": _first_value(packet, "rxSnr", "snr"),
      "rssi": _first_value(packet, "rxRssi", "rx_rssi"),
      # The packet's own clock. rxTime is 0 or absent when the device has no
      # time fix, but a live packet is still live — the wall clock is the
      # honest fallback here, and only here; node dicts carry lastHeard or
      # nothing.
      "lastHeard": packet.get("rxTime") or int(time.time()),
      "hopsAway": _hops_taken(packet),
      # Deliberately coerced to a real bool rather than passed through. The
      # library drops proto fields sitting at their default, so a plain LoRa
      # packet carries no viaMqtt key at all — and for a packet, absence *is*
      # the answer, not a lack of one. Node dicts get no such default applied
      # (see _apply_node_update): there, absence really is silence.
      "viaMqtt": bool(packet.get("viaMqtt", False)),
      "_source": "packet",
    }

    if portnum == "TELEMETRY_APP":
      telemetry = decoded.get("telemetry", {})
      # Telemetry is a nine-armed oneof; a packet carries exactly one arm.
      # localStats reports the same radio-health numbers as deviceMetrics —
      # channel utilization, airtime, uptime — without the battery, so both
      # feed the same fields here.
      radio = telemetry.get("deviceMetrics") or telemetry.get("localStats") or {}
      normalized["deviceMetrics"] = {
        "batteryLevel": radio.get("batteryLevel"),
        "voltage": radio.get("voltage"),
        "channelUtilization": radio.get("channelUtilization"),
        "airUtilTx": radio.get("airUtilTx"),
        "uptimeSeconds": radio.get("uptimeSeconds"),
      }
      normalized["environmentMetrics"] = telemetry.get("environmentMetrics", {})

    elif portnum == "POSITION_APP":
      pos = decoded.get("position", {})
      normalized["position"] = {
        "latitude": pos.get("latitude"),
        "longitude": pos.get("longitude"),
        "altitude": pos.get("altitude"),
      }

    elif portnum == "NODEINFO_APP":
      user = decoded.get("user", {})
      normalized["user"] = {
        "id": user.get("id") or from_node_id,
        "longName": user.get("longName"),
        "shortName": user.get("shortName"),
        "hwModel": user.get("hwModel"),
        "role": user.get("role"),
        # Named unconditionally so the key is always present here, which is
        # what lets _apply_node_update tell "this NODEINFO carried no key"
        # from "no NODEINFO has ever arrived". Only presence survives into
        # the archive; the key bytes stop at this dict.
        "publicKey": user.get("publicKey"),
      }

    # The row read at the top, handed on rather than read again. Stale by one
    # seed in exactly one case, which _apply_node_update's guard catches — the
    # note there is the whole argument and should be read before changing either
    # end of this.
    self._on_node_update(normalized, existing=existing)




  def _maybe_log_rx_summary(self) -> None:
    """Account for what the receive path did not archive, once a window.

    One line with TIDY_LOGS off, which is the shape a grep expects and is left
    exactly as it was; two with it on, split by what a reader would do about
    them — see `_log_rx_summary_tidy`.

    Called from the receive path rather than from a timer, so it runs on the
    same thread that owns the counters and needs no lock. The cost is that a
    radio hearing nothing at all never emits — which is right: the line reports
    traffic that arrived and was not archived, and none arriving is not that.

    Emitted only when there is something to say. An all-zero summary every
    fifteen minutes would be noise in the exact log somebody is grepping, and
    would make the presence of the line meaningless — the point is that seeing
    it at all tells you packets are being dropped.
    """
    now = time.time()
    total_drops = sum(self._undecryptable_counts.values()) + sum(self._nontext_counts.values())

    if not total_drops:
      # Nothing to report, but the window still turns over, so a quiet quarter
      # hour doesn't leave the next drop looking like it took an hour to arrive.
      if now - self._rx_summary_at >= RX_SUMMARY_INTERVAL:
        self._rx_summary_at = now
      return

    window = now - self._rx_summary_at
    if window < RX_SUMMARY_INTERVAL and total_drops < RX_SUMMARY_MAX_DROPS:
      return

    if logfmt.tidy_logs():
      self._log_rx_summary_tidy(round(window))
    else:
      clauses = []

      if self._undecryptable_counts:
        # Sorted by count, worst first: with several hashes in play the one being
        # shouted at is the one worth reading, and it should not depend on which
        # hash happens to sort lower.
        parts = ", ".join(
          f"ch-hash 0x{channel_hash:02x} x{count}"
          for channel_hash, count in sorted(
            self._undecryptable_counts.items(), key=lambda item: -item[1]
          )
        )
        mqtt = f" ({self._undecryptable_mqtt} mqtt)" if self._undecryptable_mqtt else ""
        clauses.append(f"undecryptable {parts}{mqtt}")

      if self._nontext_counts:
        parts = ", ".join(
          f"ch{channel_index} {portnum} x{count}"
          for (channel_index, portnum), count in sorted(
            self._nontext_counts.items(), key=lambda item: -item[1]
          )
        )
        clauses.append(f"non-text {parts}")

      # One line, no newlines, no quoted fields — deliberately, so
      # logfmt.TidyLogFilter passes it through untouched (it rewrites multi-line
      # records and records carrying `field: "..."` byte fields) and so
      # `grep 'RX summary'` over a day of journal returns one row per window.
      logging.info("RX summary (last %ds): %s", round(window), "; ".join(clauses))

    self._undecryptable_counts.clear()
    self._undecryptable_mqtt = 0
    self._nontext_counts.clear()
    self._rx_summary_at = now




  def _log_rx_summary_tidy(self, window: int) -> None:
    """The same window as two lines a person can read at a glance.

    **Split because the two halves are different news.** The first line is what
    was lost — packets this radio could not read at all, which is the half with
    something to act on. The second is a ledger of what was heard, understood,
    and not archived, which is the half that explains a channel looking silent.
    Run together, as they were, the reader has to disentangle a wrong key from an
    ordinary quiet channel inside one two-hundred-character line, and the heading
    said "RX summary" — which reads as *all* traffic and is exactly what it is
    not. Nothing here is a count of what was stored.

    Counts first, itemisation second, so the "how much" is legible without
    reading the "of what". The second line is skipped entirely when there is no
    non-text traffic, which makes its presence mean something.
    """
    span = logfmt.format_duration(window)
    head = []

    if self._undecryptable_counts:
      total = sum(self._undecryptable_counts.values())
      packets = "packet" if total == 1 else "packets"

      # Split by the one question the hash can answer. `ours` is a channel this
      # radio is configured for, whose packets it nonetheless could not read —
      # a PSK that has drifted on one side, and the only case in this whole line
      # with a fix. `theirs` is a neighbour on a channel we were never given.
      ours, theirs = {}, {}
      for value, count in self._undecryptable_counts.items():
        name = self.channel_hashes.get(value)
        if name:
          ours[name] = ours.get(name, 0) + count
        else:
          theirs[value] = count

      if ours:
        # The hex is printed here and nowhere else, because here it is evidence
        # rather than decoration: it is what a reader would compare against the
        # `Channel hashes:` line from startup to check this claim.
        named = "; ".join(
          f"{count} on {name}, whose key does not match this radio's "
          f"(0x{self._hash_for(name):02x})"
          for name, count in sorted(ours.items(), key=lambda item: -item[1])
        )
        rest = ""
        if theirs:
          count = sum(theirs.values())
          channels = "channel" if len(theirs) == 1 else "channels"
          rest = f"; {count} on {len(theirs)} unknown {channels}"
        head.append(f"{total} {packets} with no key — {named}{rest}")
      else:
        # No hex at all in the ordinary case. Three hashes is three separate
        # channels nearby that this radio has no key for, which is the whole of
        # what the numbers were telling anyone, and the count says it in words.
        channels = "channel" if len(theirs) == 1 else "channels"
        head.append(f"{total} {packets} on {len(theirs)} {channels} with no key")

      if self._undecryptable_mqtt:
        head[-1] += f" ({self._undecryptable_mqtt} via mqtt)"

    if self._nontext_counts:
      total = sum(self._nontext_counts.values())
      packets = "packet" if total == 1 else "packets"
      indexes = {index for index, _ in self._nontext_counts}
      channels = "channel" if len(indexes) == 1 else "channels"
      # "tracked" is only said when it is true of all of them. The counter takes
      # any decrypted non-text packet, including one on a channel this collector
      # does not archive — normally there is no such channel and the word is
      # simply accurate, but it is the kind of word that would go on being
      # printed long after it stopped being.
      scope = "tracked " if indexes <= set(self.tracked_channels) else ""
      head.append(f"{total} non-text {packets} on {len(indexes)} {scope}{channels}")

    logging.info("Not archived, last %s: %s", span, "; ".join(head))

    if not self._nontext_counts:
      return

    # Grouped by channel, busiest channel first, busiest portnum first within it.
    # The old line repeated `ch0` once per portnum — four times in a normal
    # window — which is four times the reader has to check whether the number
    # changed.
    by_channel: dict[int, list[tuple[str, int]]] = {}
    for (index, portnum), count in self._nontext_counts.items():
      by_channel.setdefault(index, []).append((portnum, count))

    groups = []
    for index, items in sorted(
      by_channel.items(), key=lambda item: -sum(count for _, count in item[1])
    ):
      parts = ", ".join(
        f"{logfmt.portnum_label(portnum)} x{count}"
        for portnum, count in sorted(items, key=lambda item: -item[1])
      )
      groups.append(f"{self._channel_label(index)} {parts}")

    logging.info("Payloads not stored: %s", "; ".join(groups))




  def _hash_for(self, channel_name: str) -> int:
    """The hash a named channel was registered under, for printing beside it.

    A reverse lookup rather than a second map: `channel_hashes` is small, this
    runs once a quarter of an hour at most, and the alternative is two structures
    that can disagree about which channel owns a hash.
    """
    for value, name in self.channel_hashes.items():
      if name == channel_name:
        return value
    return 0




  def _initial_node_sync(self) -> None:
    """Sync all known nodes from device into database at startup."""
    if not self.interface:
      logging.warning("No interface available for initial node sync")
      return

    logging.info("Starting initial node sync for %d nodes", len(self.interface.nodes))

    inserted = 0

    for node_id, node in self.interface.nodes.items():
      try:
        # Quiet: a first run replays the device's whole node cache, and a
        # hundred "New node discovered" lines say less than the one count
        # below. Every *live* path stays loud.
        if self._on_node_update(node, quiet=True):
          inserted += 1
      except Exception:
        logging.exception("Error during initial sync for node_id=%s", node_id)

    logging.info(
      "Initial node sync complete: %d new node%s from the device cache",
      inserted,
      "" if inserted == 1 else "s",
    )




  def _on_node_updated(self, node: dict, interface=None) -> None:
    """The meshtastic.node.updated listener, with the kwargs the library sends.

    This topic is not a live feed, whatever its name and the library's own
    docstring suggest. The firmware only emits node_info during the
    want_config handshake (PhoneAPI.cpp:512), so it fires at connect and after
    a device reboot, and never for a node heard mid-session. **Live discovery
    rests entirely on meshtastic.receive.** The subscription is kept because
    it is correct and cheap for what it does cover — a reboot's worth of
    device NodeDB — not because it finds new nodes.

    meshtastic publishes this topic as `node=`/`interface=`, and pypubsub takes
    a topic's argument spec from its first *subscriber* — so when
    _on_node_update (node_data=) was subscribed directly, every publish raised
    SenderMissingReqdMsgDataError inside the library's publishing thread and
    the listener was never called, in any run. Silently: DeferredExecution._run
    catches everything, prints the traceback, and keeps going. This adapter
    gives the topic the spec the library actually publishes, the same tolerance
    for interface= that _on_receive and _on_connection_lost already have, and
    leaves _on_node_update's own signature to its internal callers.
    """
    self._last_activity = time.time()
    self._on_node_update(node)




  def _on_node_update(
    self,
    node_data: dict,
    quiet: bool = False,
    existing: Optional[dict] = None,
  ) -> bool:
    """
    Update node record in database. Returns True when a row was inserted.
    Accepts full NODEINFO or partial updates (POSITION_APP, TELEMETRY_APP).

    `quiet` suppresses the per-node INFO lines and nothing else; it is for the
    startup replay, which reports a count instead. It used to be spelled
    from_initial_sync and meant both "be quiet" and "this is the replay",
    which is why the seed path — a live discovery — logged nothing.

    `existing` is an optional row the caller has already read, passed through
    to _apply_node_update, which owns the rules for when it may be believed.
    Only the receive path supplies one; the other three callers have no prior
    read and pass nothing, which is the behaviour this function has always had.
    It crosses the try below harmlessly, but note that the read itself must
    stay outside — this is the boundary the library thread may not escape, and
    a read moved above it would be a read outside the guard.

    Reached from _on_node_updated (the meshtastic.node.updated listener), so
    the same rule as _on_receive applies: this runs on a library thread and
    nothing may escape it.
    """
    try:
      return self._apply_node_update(node_data, quiet, existing)
    except Exception:
      logging.exception("Error updating node record")
      return False




  def _apply_node_update(
    self,
    node_data: dict,
    quiet: bool,
    existing: Optional[dict] = None,
  ) -> bool:
    user_data = node_data.get("user") or {}
    raw_num = node_data.get("num")
    node_id = str(
      user_data.get("id")
      or node_data.get("id")
      or (node_num_to_hex_id(raw_num) if raw_num is not None else "")
    )

    decoded = node_data.get("decoded", {})
    portnum = decoded.get("portnum")

    # Well-formedness, not identity, is what gates a row. Anything else is a
    # bug upstream of here — including the literal "None" this used to derive
    # from a node dict carrying neither user.id nor id, which the old `if not
    # node_id` guard could never catch.
    if not NODE_ID_PATTERN.match(node_id):
      logging.warning(
        "Received node data with unusable id %r (snippet: %s)",
        node_id,
        repr(node_data)[:200],
      )
      return False

    if _is_fabricated_identity(node_id, user_data):
      # The library invented these. Drop the three fabricated fields and keep
      # everything else the record carries — position, telemetry, lastHeard
      # are real. The node stays unnamed until it says who it is.
      logging.debug("Discarding fabricated identity for %s", node_id)
      user_data = {
        k: v for k, v in user_data.items()
        if k not in ("longName", "shortName", "hwModel")
      }

    # The caller may already hold this row. Only the receive path does, and only
    # because it had to read the node before deciding whether to seed one — that
    # SELECT bought the decision and then bought nothing, because this line read
    # the same row again, once per decoded packet, on the hottest path here.
    #
    # Believed only when it is a row *for this node*. The receive path keys its
    # read on the packet header's sender; node_id above is derived from user.id,
    # which a NODEINFO may legitimately spell differently. So identity is checked
    # rather than assumed, and a mismatch just reads again.
    #
    # **Identity is not freshness, and there is a writer between the two points.**
    # _handle_receive calls _seed_node_from_interface after its read, which
    # reaches this function and writes this very row. That is safe for one
    # reason, worth stating so it is not rediscovered by someone widening this
    # test: the seed call sits inside `if not existing`, so it can only run when
    # the caller's row was falsy — and a falsy row fails the test below and is
    # re-read. A seeded identity can therefore never be merged against the None
    # that preceded it. Nothing else writes `nodes` in that window: upsert_node
    # has exactly one call site, this function, and every path to it runs on the
    # library's reader thread, which is the thread already inside this call.
    if existing is None or existing.get("node_id") != node_id:
      existing = self.storage.get_node(node_id) or {}

    is_new_node = not existing

    device_metrics = node_data.get("deviceMetrics", {})
    environment = node_data.get("environmentMetrics", {})
    position = node_data.get("position", {})

    # Three states, and the difference between two of them matters: 1 the node
    # published a PKI key, 0 a NODEINFO this collector decoded carried none,
    # None nothing has ever said either way. Hence `in` rather than `.get()` —
    # the NODEINFO branch always writes the key, so its absence here means no
    # NODEINFO reached us, and a bare row built from a telemetry packet must
    # not be filed as a node that declined PKI. Presence is all that is kept;
    # the key bytes go no further than this comparison.
    has_public_key = (
      bool(user_data.get("publicKey")) if "publicKey" in user_data else None
    )

    merged = self._merge_node_data(existing, {
      "short_name": user_data.get("shortName"),
      "long_name": user_data.get("longName"),
      "hardware": user_data.get("hwModel"),
      "role": user_data.get("role"),
      "last_seen": node_data.get("lastHeard"),
      "snr": node_data.get("snr"),
      "rssi": node_data.get("rssi"),
      "battery_level": device_metrics.get("batteryLevel"),
      "voltage": device_metrics.get("voltage"),
      "channel_util": device_metrics.get("channelUtilization"),
      "air_util_tx": device_metrics.get("airUtilTx"),
      "uptime_seconds": device_metrics.get("uptimeSeconds"),
      "temperature": environment.get("temperature"),
      "humidity": environment.get("relativeHumidity"),
      "pressure": environment.get("barometricPressure"),
      "latitude": position.get("latitude"),
      "longitude": position.get("longitude"),
      "altitude": position.get("altitude"),
      # Node dicts carry hopsAway themselves; a packet's is derived from its
      # hop headers, which is the same number by the same arithmetic.
      "hops_away": node_data.get("hopsAway"),
      "via_mqtt": node_data.get("viaMqtt"),
      "has_public_key": has_public_key,
      "lux": environment.get("lux"),
      "iaq": environment.get("iaq"),
      "gas_resistance": environment.get("gasResistance"),
    })

    self.storage.upsert_node(node_id=node_id, is_new=is_new_node, **merged)

    # A node that had no name and now has one. This is the line that answers
    # "is discovery working?" — the other half of a discovery that began as a
    # bare row hours earlier, and the only evidence the two-stage pipeline
    # closes at all.
    became_named = (
      not is_new_node
      and not (existing.get("long_name") or existing.get("short_name"))
      and bool(merged.get("long_name") or merged.get("short_name"))
    )

    if quiet:
      logging.debug("Initial sync: node %s inserted/updated", node_id)
      return is_new_node

    if is_new_node:
      # Where it was heard, and whether it arrived with a name. Node dicts out
      # of the device's own cache carry no portnum.
      logging.info(
        "New node discovered: %s (%s, %s)",
        node_id,
        portnum or "device cache",
        _identity_label(merged),
      )
      return True

    if became_named:
      logging.info("Node %s identified: %s", node_id, _identity_label(merged))
      return False

    # Log only fields that actually changed for existing nodes
    changed = {}
    for key in (
      "short_name", "long_name", "hardware", "role",
      "battery_level", "voltage", "snr", "rssi",
      "temperature", "humidity", "pressure",
      "channel_util", "air_util_tx", "uptime_seconds",
      "latitude", "longitude", "altitude",
      "hops_away", "via_mqtt", "has_public_key",
      "lux", "iaq", "gas_resistance",
    ):
      old = existing.get(key)
      new = merged.get(key)
      if old != new:
        changed[key] = new

    if changed:
      if node_id == self.local_node_id and logfmt.tidy_logs():
        # The attached device reporting on itself, which it does every minute or
        # two — frequent enough that these lines were most of a quiet mesh's
        # INFO volume. In tidy logs they are grouped into one Self line per
        # TIDY_LOG_LOCAL_NODE_PERIOD (see selflog.py); the node row above was
        # already written, so only the log is thinned. `Self` rather than `Node`
        # because the viewer colours that word to say "this is the radio the
        # frame belongs to" — and because a reader grepping for the device's id
        # still finds it, on fewer lines.
        flushed = selflog.record(
          self.storage,
          node_id,
          changed,
          period_seconds=max(0, int(Config.get("TIDY_LOG_LOCAL_NODE_PERIOD", 15))) * 60,
          now=time.time(),
        )
        if flushed is not None:
          logging.info("Self %s updated: %s", node_id, logfmt.format_changes(flushed))
        else:
          # DEBUG keeps the held updates visible to anyone watching at that
          # level, the way "Skip: (no changes)" already is.
          logging.debug(
            "Self %s held for the tidy log: %s", node_id, logfmt.format_changes(changed)
          )
      else:
        logging.info(
          "Node %s updated: %s",
          _node_label(node_id, merged),
          logfmt.format_changes(changed),
        )
    else:
      logging.debug("Skip: %s (no changes)", node_id)

    return False




  def _seed_node_from_interface(self, from_node_id: str) -> bool:
    """
    If a packet arrives from a node not yet in our DB, check the meshtastic
    interface's internal node caches. The device may have received a
    NODEINFO_APP for this node previously (even before our collector started),
    so its identity — and its last known position and telemetry — is available
    there even though we never processed that packet ourselves.

    There are two caches and they are not equivalent. `nodes` is keyed by
    "!hex" and the library only writes it from NODEINFO, so it holds real
    identities and nothing else. `nodesByNum` is keyed by int and also gains
    an entry for every telemetry, position, text or admin sender — but the
    library fabricates a name for those (see _is_fabricated_identity). Read
    the trustworthy cache first, then fall back to the other for its
    non-identity fields.

    Returns True when a cached record was found and pushed to the database.
    Whether it carried a usable identity is _apply_node_update's call — it
    applies the same fabrication check to every path, including the startup
    sync.
    """
    if not self.interface:
      return False

    node_data = (self.interface.nodes or {}).get(from_node_id)

    if not node_data and NODE_ID_PATTERN.match(from_node_id):
      by_num = getattr(self.interface, "nodesByNum", None) or {}
      node_data = by_num.get(int(from_node_id[1:], 16))

    if not node_data:
      return False

    logging.debug(
      "Seeding unknown node %s from interface cache",
      from_node_id,
    )
    # Not quiet: this is a live discovery, however the identity reached us.
    self._on_node_update(node_data)
    return True




  def _handle_text_message(
    self, packet: dict, from_node_id: str, existing: Optional[dict] = None
  ) -> None:
    """Route TEXT_MESSAGE_APP packets to channel or DM storage.

    `existing` is the sender's node row as `_handle_receive` already read it, and
    is here only so the log lines below can print a name without going back to
    the database for one. Optional because nothing else about this function needs
    it, and a caller that hasn't got one is not wrong.
    """
    decoded = packet.get("decoded", {})
    text = decoded.get("text", "")

    logging.debug(
      "Captured text message from=%s channel=%s text=%r",
      from_node_id,
      packet.get("channel"),
      text
    )

    if not text:
      logging.debug("Not archived: an empty text message from %s", from_node_id)
      return

    to_id = packet.get("toId")
    message_id = packet.get("id", 0)

    # **A message with no packet id is not archivable, and archiving it anyway
    # would cost more than the one message.** `messages.message_id` is UNIQUE, so
    # a row written under id 0 makes every *later* id-less packet look like a
    # duplicate of it — silently, since a duplicate is a DEBUG line and not an
    # error. One lost message becomes all of them. The send path already refuses
    # to archive a packet the radio gave no id for (see _handle_send_request);
    # this is the same refusal on the way in, said at WARNING because an inbound
    # packet with no id is not something this collector caused.
    if not message_id:
      logging.warning(
        "Message from %s carries no packet id; not archiving %r",
        from_node_id, text[:60],
      )
      return

    # `or`, not a `.get` default: rxTime is 0 *or absent* when the device has no
    # time fix, and `.get("rxTime", ...)` only answers the absent half. A live
    # packet is still live, so the wall clock is the honest fallback — the same
    # reasoning, and the same expression, as the `lastHeard` line in
    # _handle_receive. Written the other way here, a radio with no fix stamped
    # every message it heard at the epoch and sorted them below the archive
    # forever.
    rx_time = packet.get("rxTime") or int(time.time())
    snr = packet.get("rxSnr")
    rssi = packet.get("rxRssi")
    hop_count = _hops_taken(packet)
    channel_index = packet.get("channel", 0)
    reply_to = decoded.get("replyId")
    via_mqtt = packet.get("viaMqtt", False)

    # The firmware's own answer to "is this a reaction?", which until 0.10.0 this
    # collector threw away and both readers guessed at by looking for a message
    # whose text is nothing but an emoji. The guess is wrong in both directions:
    # a deliberate one-emoji reply reads as a tapback, and a client that reacts
    # with 🏓 or a skin-toned thumb may not.
    #
    # Coerced to 0/1 rather than stored raw. The proto drops fields at their
    # default, so an absent key is a genuine "not a reaction" and not silence —
    # the same reasoning viaMqtt gets above. Clients disagree on what they put
    # in the field (1 in some, a codepoint in others), and this column answers
    # only whether it is a reaction; the emoji itself is already in `text`.
    emoji = 1 if decoded.get("emoji") else 0

    # A DM is a message addressed to this node; a channel message is one
    # addressed to everyone — this library always renders broadcast as
    # toId "^all", never "!ffffffff". Anything else is a DM between two other
    # nodes, overheard because it wasn't PKI-encrypted (toId is None when the
    # device can't resolve the recipient — a broadcast never is). Overheard
    # traffic is archived under neither heading; the sender's node row still
    # updates in _handle_receive.
    is_dm = to_id is not None and to_id == self.local_node_id

    if not is_dm and to_id != "^all":
      logging.debug(
        "Skip: overheard DM from %s to %s (not this node, not broadcast)",
        from_node_id, to_id,
      )
      return

    metrics = logfmt.metrics_suffix(snr, rssi, hop_count)

    # The row was read on the way in, *before* the seed that may have created it,
    # so the first message from a newly discovered node arrives here with None —
    # exactly the message where a name is worth most. Re-read once in that case:
    # a text message is the rarest thing this collector handles and one indexed
    # lookup against it is nothing, where the same read on the packet path would
    # not have been.
    if existing is None and logfmt.tidy_logs():
      existing = self.storage.get_node(from_node_id)
    sender = _node_label(from_node_id, existing)

    if is_dm:
      if not Config.get("STORE_DIRECT_MESSAGES"):
        # Worth an INFO line — a DM arriving is mesh traffic whether or not this
        # collector keeps it. The text is deliberately not logged: STORE_DIRECT_
        # MESSAGES being off is a decision not to retain DM content, and writing
        # it to the log would retain it anyway, just somewhere else.
        logging.info(
          "DM  %s: (not stored; STORE_DIRECT_MESSAGES is off)%s", sender, metrics
        )
        return

      try:
        inserted = self.storage.insert_direct_message(
          message_id=message_id,
          from_node=from_node_id,
          # The DM test above is `to_id == self.local_node_id`, so for an
          # arriving direct message the recipient is this node by definition.
          # Recorded rather than left implicit, so the column reads the same way
          # in both directions once the send path starts filling it in.
          to_node=self.local_node_id,
          text=text,
          rx_time=rx_time,
          snr=snr,
          rssi=rssi,
          reply_to=reply_to,
          via_mqtt=via_mqtt,
          emoji=emoji,
        )
        if inserted:
          logging.info("DM  %s: %s%s", sender, text[:100], metrics)
        else:
          logging.debug("Duplicate DM skipped: message_id=%s", message_id)
      except Exception:
        logging.exception("Failed to insert DM from %s", from_node_id)
    else:
      if not self._should_log_channel(channel_index):
        logging.debug(
          "Not archived: a message on %s, which this collector does not track",
          self._channel_label(channel_index),
        )
        return

      try:
        inserted = self.storage.insert_message(
          message_id=message_id,
          channel_index=channel_index,
          from_node=from_node_id,
          to_node=to_id,
          text=text,
          rx_time=rx_time,
          hop_count=hop_count,
          snr=snr,
          rssi=rssi,
          reply_to=reply_to,
          via_mqtt=via_mqtt,
          emoji=emoji,
        )
        if inserted:
          logging.info(
            "%s %s: %s%s",
            self._channel_label(channel_index), sender, text[:100], metrics,
          )
        else:
          logging.debug("Duplicate message skipped: message_id=%s", message_id)
      except Exception:
        logging.exception("Failed to insert message from %s", from_node_id)




  def _merge_node_data(self, existing: dict, new_data: dict) -> dict:
    """Merge incoming node data with the existing DB record — already read by
    the caller, not re-read here — preferring new non-null values."""
    merged = {}

    fields = [
      "short_name", "long_name", "hardware", "role",
      "last_seen", "battery_level", "voltage", "snr", "rssi",
      "temperature", "humidity", "pressure",
      "channel_util", "air_util_tx", "uptime_seconds",
      "latitude", "longitude", "altitude",
      "hops_away", "via_mqtt", "has_public_key",
      "lux", "iaq", "gas_resistance"
    ]

    for field in fields:
      if field in new_data and new_data[field] is not None:
        merged[field] = new_data[field]
      else:
        merged[field] = existing.get(field)

    # last_seen never moves backwards: the initial sync replays the device's
    # whole node cache on every restart, and a stale cache entry must not
    # roll back what a live packet already recorded. A write with no time
    # signal keeps the existing stamp; only a brand-new row with no signal
    # at all falls back to the wall clock.
    incoming = new_data.get("last_seen")
    current = existing.get("last_seen")
    if incoming and current:
      merged["last_seen"] = max(current, incoming)
    else:
      merged["last_seen"] = incoming or current or int(time.time())
    return merged




  def _tracked_channel_indexes(self) -> list[int]:
    """Channel indexes this collector archives: PRIMARY_CHANNEL if
    LOG_PRIMARY_CHANNEL is true, plus any in LOG_CHANNEL_IDS."""
    tracked: set[int] = set()

    if Config.get("LOG_PRIMARY_CHANNEL", True):
      tracked.add(int(Config.get("PRIMARY_CHANNEL", 0)))

    tracked.update(int(idx) for idx in Config.get("LOG_CHANNEL_IDS") or [])

    return sorted(tracked)




  def _channel_label(self, channel_index: int) -> str:
    """A channel for a log line — its name, or `ch3` when there isn't one.

    `ch0` is a number out of a protobuf and `NCMesh` is the thing the reader
    named; between two channels the number cannot tell you which one has gone
    quiet. Falls back to the index whenever the map has no answer — before the
    sync has run, or for a channel the device did not report — because an index
    is at least true, and untidy logs keep the index in every case.
    """
    if not logfmt.tidy_logs():
      return f"ch{channel_index}"

    return self.channel_names.get(channel_index) or f"ch{channel_index}"




  def _channel_hash_name(self, channel, index: int, primary_channel: int) -> Optional[str]:
    """The name the *firmware* hashes this channel under, or None.

    Not the same string as the display name and deliberately derived apart from
    it. A channel with no name of its own hashes under the modem preset's name
    (see `_PRESET_NAMES`), while the same channel *displays* as "Primary" — hash
    it under the word this project made up for the log and every comparison it
    feeds is wrong.

    None means "cannot be computed", and that is a real answer: an unknown modem
    preset, or a firmware that stops reporting one. The caller must drop the
    channel rather than fall back, because the whole value of the hash map is
    that a hit in it is trustworthy.
    """
    settings = getattr(channel, "settings", None)
    raw_name = (getattr(settings, "name", "") or "").strip() if settings else ""
    if raw_name:
      return raw_name

    if index != primary_channel:
      # A secondary channel with no name is hashed under the empty string by the
      # firmware, which is a legal channel and a computable hash.
      return ""

    local_config = getattr(self.interface, "localConfig", None)
    if local_config is None:
      return None

    try:
      preset = config_pb2.Config.LoRaConfig.ModemPreset.Name(
        local_config.lora.modem_preset
      )
    except Exception:
      # A preset number this build of the protobuf has no name for. Same answer
      # as an unknown preset name below: no hash rather than a guessed one.
      return None

    return _PRESET_NAMES.get(preset)




  def _build_channel_labels(self, device_channels: dict, primary_channel: int) -> None:
    """Fill `channel_names` and `channel_hashes` from the device's channel list.

    Wrapped in its own try per channel for the same reason `_sync_channels` is:
    one channel the library hands back in an unexpected shape should cost that
    channel its label, not cost every channel after it theirs. A missing label is
    a log line that falls back to `ch2`; an exception here would be a collector
    that will not start over the spelling of a log line.
    """
    self.channel_names = {}
    self.channel_hashes = {}

    for index, channel in sorted(device_channels.items()):
      try:
        # A disabled channel carries no traffic and must not be able to claim a
        # hash: its name and key are still in the config, so leaving it in the
        # map would let a neighbour's packet be reported as our own key drifting.
        if getattr(channel, "role", 1) == 0:
          continue

        settings = getattr(channel, "settings", None)
        raw_name = (getattr(settings, "name", "") or "").strip() if settings else ""

        if raw_name:
          self.channel_names[index] = raw_name
        elif index == primary_channel:
          # The same fallback the tracked loop below writes to the archive, so a
          # log line and a reader's channel list say the same word.
          self.channel_names[index] = "Primary"
        else:
          self.channel_names[index] = f"Channel {index}"

        hash_name = self._channel_hash_name(channel, index, primary_channel)
        if hash_name is None:
          continue

        psk = getattr(settings, "psk", None) if settings else None
        computed = logfmt.channel_hash(hash_name, psk)
        if computed is None:
          continue

        # First writer wins. Two channels really can hash the same, and a
        # collision means neither name is a safe thing to print beside a packet —
        # but reporting the wrong one of our own channels is a far smaller wrong
        # than reporting a stranger's packet as our own key failing, so the
        # collision keeps a name and the log says which channels share it.
        if computed in self.channel_hashes:
          logging.warning(
            "Channels %s and %s both hash to 0x%02x; an undecryptable packet "
            "with that hash cannot be attributed to either",
            self.channel_hashes[computed], self.channel_names[index], computed,
          )
          continue

        self.channel_hashes[computed] = self.channel_names[index]

      except Exception:
        logging.exception("Could not label channel %s for the log", index)

    if self.channel_hashes:
      # Printed once at startup because it is the key the RX summary is read
      # against, and because it is the only evidence that the hash computation
      # agrees with the radio at all: a hash here that never appears in a
      # summary is the healthy case, and one that appears constantly is a PSK
      # that has drifted. The hashes are not secret — every packet broadcasts
      # its own in the clear.
      logging.info(
        "Channel hashes: %s",
        ", ".join(
          f"0x{value:02x} {name}" for value, name in sorted(self.channel_hashes.items())
        ),
      )




  def _sync_channels(self) -> bool:
    """
    Sync channels from device into database.
    Channels tracked: PRIMARY_CHANNEL if LOG_PRIMARY_CHANNEL=True, plus any in LOG_CHANNEL_IDS.

    Returns False when a channel was meant to be recorded and none was. start()
    turns that into a nonzero exit; see the comment there for why that is the
    right answer rather than carrying on.

    **A tracked channel with no row in `channels` is a channel whose messages
    disappear.** Every reader joins messages against that table, so the collector
    goes on archiving into it and nothing can display the result — the archive
    looks healthy and the channel looks silent. That is the failure this whole
    method is arranged around, and it is why the counting below exists rather
    than a single try/except over the lot: the old shape logged one exception and
    let start() continue, so the collector came up, subscribed, and wrote messages
    that no reader would ever join.
    """
    primary_channel = Config.get("PRIMARY_CHANNEL", 0)
    logging.info(
      "Channel config: LOG_PRIMARY_CHANNEL=%s PRIMARY_CHANNEL=%s LOG_CHANNEL_IDS=%s",
      Config.get("LOG_PRIMARY_CHANNEL"),
      primary_channel,
      Config.get("LOG_CHANNEL_IDS"),
    )

    tracked = self.tracked_channels
    logging.info("Config-tracked channel indexes: %s", tracked)

    # A collector configured to log no channels at all is a legal configuration,
    # not a failed sync. Checked before anything else so that "nothing landed"
    # below can only ever mean "nothing landed that was supposed to".
    if not tracked:
      logging.info("No channels are configured for logging; nothing to sync")
      return True

    # Reading the device's channel list is all-or-nothing — there is no partial
    # answer to salvage — so it fails as a unit, separately from the per-channel
    # writes below, and says so in its own words. The two failures want different
    # things from whoever reads the journal: this one is the radio, the one after
    # the loop is the database.
    try:
      local_node = self.interface.getNode("^local")
      if not local_node:
        logging.error(
          "The device did not return a local node, so no channel could be "
          "recorded. Exiting so the service manager retries; if this repeats, "
          "the radio is connected but not answering config requests."
        )
        return False

      channels = getattr(local_node, "channels", None)
      if channels is None:
        logging.warning("Local node has no channels list")
        channels = []

      device_channels = {}
      for ch in channels:
        idx = getattr(ch, "index", None)
        if isinstance(idx, int):
          device_channels[idx] = ch

    except Exception:
      logging.exception(
        "Could not read the channel list from the device, so no channel could "
        "be recorded. Exiting so the service manager retries."
      )
      return False

    # **Every channel the device knows gets a name and a hash, tracked or not.**
    # The loop below only walks the tracked ones, because only those get a row in
    # `channels` — but a non-text packet can arrive on any of them, and the RX
    # summary has to be able to say which. Built before that loop so a per-channel
    # write failure below cannot cost the labels; nothing here touches the
    # archive, so nothing here can fail in a way worth aborting a sync for.
    self._build_channel_labels(device_channels, primary_channel)

    # The per-channel try covers the name derivation as well as the write. A
    # channel object the library hands back in an unexpected shape raises in the
    # getattr walk, not in sqlite, and wrapping only the write would let that one
    # malformed channel cost every channel after it — the same all-or-nothing
    # failure, moved down one line.
    landed = 0
    failed = 0

    for idx in tracked:
      try:
        ch = device_channels.get(idx)

        name = None
        if ch:
          settings = getattr(ch, "settings", None)
          if settings:
            raw_name = getattr(settings, "name", None)
            if raw_name and raw_name.strip():
              name = raw_name.strip()

        if not name:
          if idx == primary_channel:
            name = "Primary"
          else:
            name = f"Channel {idx}"

        logging.info("Tracking channel: index=%s name=%s", idx, name)
        self.storage.upsert_channel(idx, name)
        landed += 1

      except Exception:
        failed += 1
        logging.exception("Failed to record channel %s", idx)

    # None landing is fatal; some landing is not. Losing one channel of four
    # should not cost the other three their collector — that channel's messages
    # are invisible, which the WARNING says, and the archive is still doing most
    # of its job. Losing all of them means the collector would come up and write
    # nothing anybody can read, which is worth a restart.
    if not landed:
      logging.error(
        "The device answered, but none of the %d tracked channels could be "
        "written to the archive. Messages would be stored into channels no "
        "reader can join against, so exiting instead; the service manager will "
        "retry. The database is the thing to look at, not the radio.",
        failed,
      )
      return False

    if failed:
      logging.warning(
        "%d of %d tracked channels could not be recorded. Messages on those "
        "channels will be archived but will not appear in any reader until "
        "their rows exist.",
        failed,
        landed + failed,
      )
    else:
      # Names rather than "successfully", because the names are the fact worth
      # having: this is the line a reader checks when a channel's messages are
      # missing from a reader, and "successfully" answers a question nobody
      # asked. The `Self` line and the RX summary both use these same words.
      logging.info(
        "Archiving messages on %s",
        ", ".join(self._channel_label(index) for index in tracked),
      )

    return True




  def _should_log_channel(self, channel_index: int) -> bool:
    """Check if channel_index is configured for logging."""
    return channel_index in self.tracked_channels




  def _restart_process(self) -> None:
    """Restart collector after device swap detection."""
    import __main__

    if hasattr(__main__, "__package__") and __main__.__package__:
      os.execv(sys.executable, [sys.executable, "-m", __main__.__package__] + sys.argv[1:])
    else:
      os.execv(sys.executable, [sys.executable] + sys.argv)




def _configure_logging() -> None:
  log_level = logging.DEBUG if Config.get("DEBUG", False) else logging.INFO
  logging.basicConfig(level=log_level, format=LOG_FORMAT)

  if logfmt.tidy_logs():
    # Attached to the handler, not the meshtastic logger: the records are
    # emitted by several modules under that name, and a handler-level filter
    # catches them all without this needing to know which.
    tidy_filter = logfmt.TidyLogFilter()
    for handler in logging.getLogger().handlers:
      handler.addFilter(tidy_filter)




def _install_signal_handlers(collector: MeshtasticCollector) -> None:
  def _handle_signal(signum, frame):
    collector.stop()
    sys.exit(0)

  signal.signal(signal.SIGINT, _handle_signal)
  signal.signal(signal.SIGTERM, _handle_signal)




def main() -> None:
  Config.load()
  _configure_logging()

  db = Storage()

  # A misconfigured transport is an operator error, and a traceback is the wrong
  # way to report one — especially under systemd, where it would be reprinted
  # every RestartSec until somebody fixes config.json. One line, exit 1, same as
  # the missing-device path in start().
  try:
    collector = MeshtasticCollector(db=db)
  except transport.TransportError as error:
    logging.error("Cannot start: %s", error)
    db.close()
    sys.exit(1)

  _install_signal_handlers(collector)
  collector.start()




