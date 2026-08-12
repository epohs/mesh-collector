from __future__ import annotations

import ast
import base64
import logging
import os
import re
import signal
import sys
import time
import sqlite3

from typing import Optional

from meshtastic.protobuf import mesh_pb2, portnums_pb2, storeforward_pb2, telemetry_pb2
from meshtastic.serial_interface import SerialInterface
from pubsub import pub

from mesh_collector.config import Config
from mesh_collector.db import Storage


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
  Collects Meshtastic packets via serial interface and persists
  node metadata, channel messages, and direct messages to SQLite.
  """

  def __init__(self, db: Storage) -> None:
    self.storage = db
    self.serial_port: str = Config.get("SERIAL_PORT")
    self.interface: Optional[SerialInterface] = None
    self._running = False
    self._connection_lost = False
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




  def start(self) -> None:
    logging.info("Starting Meshtastic collector on %s", self.serial_port)

    self.interface = SerialInterface(self.serial_port)

    self._sync_channels()
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
    self.storage.prune_stale_nodes()

    pub.subscribe(self._on_receive, "meshtastic.receive")
    pub.subscribe(self._on_node_updated, "meshtastic.node.updated")
    pub.subscribe(self._on_connection_lost, "meshtastic.connection.lost")

    self._running = True
    self._main_loop()

    # The loop only falls out here when _on_connection_lost stopped it — a
    # signal exits from inside its own handler and never returns this far. Exit
    # nonzero so Restart= in the service unit fires: the reconnect is the
    # restart. In-process reconnect-with-backoff was the alternative and was
    # not taken; systemd already owns the retry policy.
    if self._connection_lost:
      self.stop()
      sys.exit(1)




  def stop(self) -> None:
    logging.info("Stopping collector")
    self._running = False

    # Before the interface and the database, so a client cannot be granted a
    # send that then has nothing to send with.
    if self.control_server is not None:
      try:
        self.control_server.stop()
      except Exception:
        logging.exception("Failed to close the control socket")
      self.control_server = None

    if self.interface:
      try:
        self.interface.close()
      except Exception:
        # A vanished device can make close() itself raise; the database close
        # below and the nonzero exit still have to happen.
        logging.exception("Failed to close the serial interface")
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




  def _warn_if_mqtt_proxy_expected(self) -> None:
    """Say so if the device is relying on its serial client to reach MQTT.

    **The device asks whoever holds the serial port to do its publishing, and this
    collector does not.** With `mqtt.proxy_to_client_enabled` set, the firmware sends
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
      "provide it: while this collector holds the serial port the device's MQTT "
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
        continue

      pending = self.control_server.poll(timeout=CONTROL_POLL_INTERVAL)
      if pending is None:
        continue

      self._answer_control_request(pending)




  def _answer_control_request(self, pending) -> None:
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




  def _handle_send_request(self, pending, request) -> None:
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
      pending.fail(ERR_SEND_FAILED, "The collector has no serial interface open.")
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




  def _transmit_reaction(self, request, want_ack: bool):
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
    self, request, message_id: int, is_direct: bool, is_reaction: bool = False
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




  def _on_connection_lost(self, interface=None) -> None:
    """The serial device is gone and meshtastic's reader thread has exited.

    Without this the process idles forever archiving nothing — alive as far as
    the service manager can tell, so Restart= never fires. Stop the main loop
    instead; start() sees _connection_lost and turns that into a nonzero exit.
    meshtastic publishes this with interface=, tolerated the same way
    _on_receive tolerates it.
    """
    logging.error(
      "Serial connection lost on %s; shutting down so the service manager "
      "restarts this collector",
      self.serial_port,
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

    try:
      existing = self.storage.get_node(from_node_id)
    except sqlite3.ProgrammingError as e:
      # The connection is closing under us mid-shutdown. Drop the packet; do
      # not re-raise into the reader thread.
      logging.debug("Database unavailable during packet processing: %s", e)
      return

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
      self._handle_text_message(packet, from_node_id)
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

    self._on_node_update(normalized)




  def _maybe_log_rx_summary(self) -> None:
    """Account for what the receive path dropped, once a window, on one line.

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

    # One line, no newlines, no quoted fields — deliberately, so _TidyLogFilter
    # passes it through untouched (it rewrites multi-line records and records
    # carrying `field: "..."` byte fields) and so `grep 'RX summary'` over a
    # day of journal returns one row per window.
    logging.info("RX summary (last %ds): %s", round(window), "; ".join(clauses))

    self._undecryptable_counts.clear()
    self._undecryptable_mqtt = 0
    self._nontext_counts.clear()
    self._rx_summary_at = now




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
    self._on_node_update(node)




  def _on_node_update(self, node_data: dict, quiet: bool = False) -> bool:
    """
    Update node record in database. Returns True when a row was inserted.
    Accepts full NODEINFO or partial updates (POSITION_APP, TELEMETRY_APP).

    `quiet` suppresses the per-node INFO lines and nothing else; it is for the
    startup replay, which reports a count instead. It used to be spelled
    from_initial_sync and meant both "be quiet" and "this is the replay",
    which is why the seed path — a live discovery — logged nothing.

    Reached from _on_node_updated (the meshtastic.node.updated listener), so
    the same rule as _on_receive applies: this runs on a library thread and
    nothing may escape it.
    """
    try:
      return self._apply_node_update(node_data, quiet)
    except Exception:
      logging.exception("Error updating node record")
      return False




  def _apply_node_update(self, node_data: dict, quiet: bool) -> bool:
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
      logging.info("Node %s updated: %s", node_id, _format_changes(changed))
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




  def _handle_text_message(self, packet: dict, from_node_id: str) -> None:
    """Route TEXT_MESSAGE_APP packets to channel or DM storage."""
    decoded = packet.get("decoded", {})
    text = decoded.get("text", "")

    logging.debug(
      "Captured text message from=%s channel=%s text=%r",
      from_node_id,
      packet.get("channel"),
      text
    )

    if not text:
      logging.debug("Skip: empty text message from %s", from_node_id)
      return

    to_id = packet.get("toId")
    message_id = packet.get("id", 0)
    rx_time = packet.get("rxTime", int(time.time()))
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

    metrics = _metrics_suffix(snr, rssi, hop_count)

    if is_dm:
      if not Config.get("STORE_DIRECT_MESSAGES"):
        # Worth an INFO line — a DM arriving is mesh traffic whether or not this
        # collector keeps it. The text is deliberately not logged: STORE_DIRECT_
        # MESSAGES being off is a decision not to retain DM content, and writing
        # it to the log would retain it anyway, just somewhere else.
        logging.info(
          "DM  %s: (not stored; STORE_DIRECT_MESSAGES is off)%s", from_node_id, metrics
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
          logging.info("DM  %s: %s%s", from_node_id, text[:100], metrics)
        else:
          logging.debug("Duplicate DM skipped: message_id=%s", message_id)
      except Exception:
        logging.exception("Failed to insert DM from %s", from_node_id)
    else:
      if not self._should_log_channel(channel_index):
        logging.debug("Skip: message on untracked channel %d", channel_index)
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
          logging.info("CH%d %s: %s%s", channel_index, from_node_id, text[:100], metrics)
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




  def _sync_channels(self) -> None:
    """
    Sync channels from device into database.
    Channels tracked: PRIMARY_CHANNEL if LOG_PRIMARY_CHANNEL=True, plus any in LOG_CHANNEL_IDS.
    """
    primary_channel = Config.get("PRIMARY_CHANNEL", 0)
    logging.info(
      "Channel config: LOG_PRIMARY_CHANNEL=%s PRIMARY_CHANNEL=%s LOG_CHANNEL_IDS=%s",
      Config.get("LOG_PRIMARY_CHANNEL"),
      primary_channel,
      Config.get("LOG_CHANNEL_IDS"),
    )

    try:
      local_node = self.interface.getNode("^local")
      if not local_node:
        logging.warning("No local node available; cannot sync channels")
        return

      channels = getattr(local_node, "channels", None)
      if channels is None:
        logging.warning("Local node has no channels list")
        channels = []

      device_channels = {}
      for ch in channels:
        idx = getattr(ch, "index", None)
        if isinstance(idx, int):
          device_channels[idx] = ch

      logging.info("Config-tracked channel indexes: %s", self.tracked_channels)

      for idx in self.tracked_channels:
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

      logging.info("Channels synced successfully")

    except Exception:
      logging.exception("Failed to sync channels")




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




# Every 300 seconds the meshtastic library logs the same three DEBUG lines for a
# heartbeat that carries no payload and never varies. Over a normal run that is
# most of the DEBUG volume: in a 24k-line testbed log, 263 heartbeats accounted
# for 1050 lines, all byte-identical.
#
# The three come from two different modules, and only the first is heartbeat-
# specific — the other two are the generic outbound-packet logs in
# stream_interface._sendToRadio, which also fire for real traffic. So each is
# matched on its own content rather than by swallowing a fixed number of lines
# after the first. That matters: on the very first heartbeat after connect, the
# collector's own INFO lines interleave *between* the three, and a positional
# filter would eat "Channels synced successfully" along with them.
#
# Matching is exact and fails open. If the library rewords a line or the wire
# format changes, the line stops being recognised and simply shows up again.
_HEARTBEAT_INTERVAL_PREFIX = "Sending heartbeat, interval"
_HEARTBEAT_SEND = "Sending: heartbeat { }"

# The wire bytes of an empty heartbeat: a 4-byte stream header declaring a
# 2-byte payload, then the packet itself. Constant because the payload is empty,
# which is what makes this a reliable discriminator against other outbound
# packets — in the same log all 7 non-heartbeat header lines were distinct.
_HEARTBEAT_HEADER = "sending header:%r b:%r" % (b"\x94\xc3\x00\x02", b":\x00")

# The radio's reply to a heartbeat: three more records reporting an empty
# transmit queue and no packet. Recognised by content (free == maxlen, no packet
# id) rather than by position after a heartbeat, so genuine queue pressure is
# never hidden: a testbed log with 264 idle replies also held 11 with a real
# packet id and a non-full queue, and all 11 survive this filter.
_IDLE_QUEUE_BYTES = "in mesh_interface.py _handleFromRadio() fromRadioBytes: %r" % (
  b"Z\x04\x10\x10\x18\x10",
)
_QUEUE_STATUS_PREFIX = "Received from radio: queueStatus {"
_TX_QUEUE_PREFIX = "TX QUEUE free "

# The library dumps every inbound frame as a multi-line protobuf repr — a single
# text message costs 16 lines, a node_info about the same. Flattening indentation
# to one line loses nothing and is most of the readability win in DEBUG.
#
# **There is no constant for that prefix any more, and its absence is the fix.**
# The flattening used to be gated on this line starting `Received from radio:`,
# which described one instance of the problem rather than the problem: any record
# carrying a protobuf carries its newlines, and the ones that did not start with
# those three words went on arriving in pieces. See `_TidyLogFilter.filter`, which
# now asks only whether the record has a newline in it.

# Having dumped the frame, the library then restates the same packet twice more:
# once as a Python dict on the way into its handler, once on the way out to
# pubsub. These are shortened rather than dropped — the packet they restate is
# already on the line above, but the record itself still marks that dispatch
# happened, which is the part worth keeping when tracing a packet's path.
_RESTATEMENT = re.compile(
  r"^(Publishing meshtastic\.receive\.\w+|in _on\w+\(\) asDict):.*", re.DOTALL
)
_PACKET_ID = re.compile(r"'id':\s*(\d+)")

# The library logs every frame twice: once as the raw bytes it read off the
# serial port, and once — on the very next line — as the protobuf those bytes
# parsed into. The first is a Python `bytes` repr, so a nodeinfo frame arrives as
# a hundred and forty `\x..` escapes immediately above the same nodeinfo written
# out in fields with names on them.
#
# **Decoding it would produce the line that already follows it.** So this is
# collapsed rather than described, the way the two restatements above are: 210 of
# these in one eleven-minute run, 207 of them followed within three lines by their
# own decoded form, and the three that were not are frames the library itself
# could not place either. The byte count is kept because it is the one fact the
# decoded line does not carry — how much came off the wire — and because a frame
# boundary is worth marking when tracing a packet through the library.
_RAW_FRAME = re.compile(r"^(in \S+ _handleFromRadio\(\) fromRadioBytes): *(b['\"].*)$", re.DOTALL)

# Inside a dumped packet the interesting half is an opaque blob: protobuf's text
# format prints the payload as octal escapes, so a telemetry packet reads as
# `payload: "\r\2424\266i\022\026\010T..."`. It is a serialized submessage, and
# which one is named by the portnum on the line above it — so both are pulled out
# and the blob is decoded back into the numbers it already contains.
_PORTNUM = re.compile(r"\bportnum:\s*(\w+)")
_PAYLOAD = re.compile(r'\bpayload:\s*("(?:[^"\\]|\\.)*")')

# Only the payloads worth reading at a glance. Anything else keeps its escapes,
# which is the honest outcome — a blob this cannot name is a blob.
#
# `TEXT_MESSAGE_APP` is not here and is not an oversight: its payload is not a
# protobuf at all, it is the message as UTF-8 bytes, so it is handled before this
# lookup rather than through it.
_PAYLOAD_PROTOS = {
  "TELEMETRY_APP": telemetry_pb2.Telemetry,
  "POSITION_APP": mesh_pb2.Position,
  "NODEINFO_APP": mesh_pb2.User,
  "STORE_FORWARD_APP": storeforward_pb2.StoreAndForward,
}

# A text message longer than this is cut, with an ellipsis to say so. The mesh's
# own limit is 233 bytes and these lines are already long; what a reader wants
# from a DEBUG dump is which message this was, not the whole of it.
_TEXT_PREVIEW = 48




def _format_uptime(seconds: int) -> str:
  """Seconds as the largest unit that stays readable — 15948976 is not a number
  anyone reads as 184 days."""
  if seconds < 3600:
    return f"{seconds // 60}m"
  if seconds < 86400:
    return f"{seconds // 3600}h"
  return f"{seconds // 86400}d"




def _describe_telemetry(decoded) -> Optional[str]:
  """A telemetry packet's numbers, whichever of the variants it turned out to carry.

  **`Telemetry` is a `oneof` and this used to read only one arm of it.** Every
  packet decoded, `decoded.device_metrics` on one carrying something else handed
  back a default-empty submessage, every field of it was falsy, and the result was
  an empty description — which `_annotate_payload` reads as "could not decode" and
  leaves as escapes. So a temperature reading and a router's whole packet ledger
  were arriving intact and being printed as octal. Five of the forty-five telemetry
  packets in one eleven-minute run, all of them decodable the entire time.
  """
  variant = decoded.WhichOneof("variant")

  if variant == "device_metrics":
    m = decoded.device_metrics
    parts = []
    if m.battery_level:
      # The proto documents this field as "0-100 (>100 means powered)", so a
      # node on mains reports 101 and rendering that as "battery 101%" would
      # read as a bug in this decoder rather than a node that is plugged in.
      parts.append("powered" if m.battery_level > 100 else f"battery {m.battery_level}%")
    if m.voltage:
      parts.append(f"{m.voltage:.2f}V")
    if m.channel_utilization:
      parts.append(f"chUtil {m.channel_utilization:.1f}%")
    if m.air_util_tx:
      parts.append(f"airTx {m.air_util_tx:.2f}%")
    if m.uptime_seconds:
      parts.append(f"up {_format_uptime(m.uptime_seconds)}")
    return " ".join(parts) or None

  if variant == "environment_metrics":
    m = decoded.environment_metrics
    parts = []
    if m.temperature:
      # Celsius on the wire, and left in it: the rest of this archive is metric
      # and a converted number nobody asked for is a number to double-check.
      parts.append(f"{m.temperature:.1f}°C")
    if m.relative_humidity:
      parts.append(f"{m.relative_humidity:.0f}%RH")
    if m.barometric_pressure:
      parts.append(f"{m.barometric_pressure:.0f}hPa")
    if m.lux:
      parts.append(f"{m.lux:.0f}lux")
    return " ".join(parts) or None

  if variant == "local_stats":
    m = decoded.local_stats
    parts = []
    if m.uptime_seconds:
      parts.append(f"up {_format_uptime(m.uptime_seconds)}")
    if m.channel_utilization:
      parts.append(f"chUtil {m.channel_utilization:.1f}%")
    if m.air_util_tx:
      parts.append(f"airTx {m.air_util_tx:.2f}%")
    if m.num_packets_tx or m.num_packets_rx:
      # The three counts that describe the same stream — sent, heard, and the
      # share of what was heard that was noise or an echo — kept together so the
      # denominators are next to what they are denominators of.
      noise = []
      if m.num_packets_rx_bad:
        noise.append(f"{m.num_packets_rx_bad} bad")
      if m.num_rx_dupe:
        noise.append(f"{m.num_rx_dupe} dupe")
      tail = f" ({', '.join(noise)})" if noise else ""
      parts.append(f"tx {m.num_packets_tx} rx {m.num_packets_rx}{tail}")
    if m.num_total_nodes:
      parts.append(f"nodes {m.num_online_nodes}/{m.num_total_nodes}")
    if m.heap_total_bytes:
      parts.append(f"heap {100 * m.heap_free_bytes // m.heap_total_bytes}% free")
    return " ".join(parts) or None

  return None




def _describe_payload(portnum: str, payload: bytes) -> Optional[str]:
  """One line of plain numbers for a decoded payload, or None to leave it alone."""
  if portnum == "TEXT_MESSAGE_APP":
    # **The one payload that is not a protobuf**: these bytes are the message.
    # Cut to a preview rather than reproduced whole — this is a trace of a packet
    # arriving, not a second copy of the archive, and the archive is where the
    # text is kept. Undecodable bytes fall through to the escapes, which is the
    # right answer for a packet claiming to be text and not being it.
    try:
      text = payload.decode("utf-8")
    except UnicodeDecodeError:
      return None
    text = " ".join(text.split())
    if not text:
      return None
    if len(text) > _TEXT_PREVIEW:
      text = text[:_TEXT_PREVIEW - 1] + "…"
    return f"text {text!r}"

  proto = _PAYLOAD_PROTOS.get(portnum)
  if proto is None:
    return None

  try:
    decoded = proto()
    decoded.ParseFromString(payload)
  except Exception:
    # A truncated or unexpected payload is not worth an exception in a log
    # filter: leave the escapes in place and let the raw bytes speak.
    return None

  if portnum == "TELEMETRY_APP":
    return _describe_telemetry(decoded)

  if portnum == "STORE_FORWARD_APP":
    # The request/response code is the whole of what most of these say — a
    # router announcing itself, a client asking for history. `rr` is required in
    # practice and named here even when nothing else is set, because "a heartbeat
    # arrived" is the entire content of a heartbeat.
    parts = [storeforward_pb2.StoreAndForward.RequestResponse.Name(decoded.rr)]
    if decoded.HasField("heartbeat") and decoded.heartbeat.period:
      parts.append(f"every {_format_uptime(decoded.heartbeat.period)}")
    if decoded.HasField("stats") and decoded.stats.messages_total:
      parts.append(f"{decoded.stats.messages_saved}/{decoded.stats.messages_total} saved")
    return " ".join(parts) or None

  if portnum == "POSITION_APP":
    parts = []
    if decoded.latitude_i or decoded.longitude_i:
      # The wire carries degrees scaled by 1e7 as integers.
      parts.append(f"{decoded.latitude_i / 1e7:.5f},{decoded.longitude_i / 1e7:.5f}")
    if decoded.altitude:
      parts.append(f"alt {decoded.altitude}m")
    if decoded.sats_in_view:
      parts.append(f"sats {decoded.sats_in_view}")
    return " ".join(parts) or None

  if portnum == "NODEINFO_APP":
    parts = [p for p in (decoded.short_name, decoded.long_name) if p]
    if decoded.hw_model:
      parts.append(f"hw {mesh_pb2.HardwareModel.Name(decoded.hw_model)}")
    return " ".join(parts) or None

  return None




# Every field the library prints as an escaped byte string, and what is done with
# it. `payload` is absent because it needs the portnum from the same line and is
# handled by `_annotate_payload`; everything else here is context-free.
#
# **This table is the coverage check.** verify_mesh_collector.py walks a corpus of
# real records and asserts no escaped field is missing from it, so a field added
# by a firmware or library update fails a check instead of quietly arriving as a
# hundred characters of octal that everyone learns to skip over. That is how the
# ones below were found: `payload` was decoded, `payload` was the only thing
# anybody had looked at, and 171 escaped strings in one session were not payloads.
#
# **Two of them are secrets and are removed rather than rendered.** A collector
# at DEBUG logs its own `config { security { private_key } }` once at the
# handshake, and every channel's `psk` beside it. Nothing reads those back out of
# a log, and a log gets pasted into a handoff, a terminal, an issue.
_REDACT = "redact"

# **The test each field is put to is whether a person watching can use it.** That
# is what TIDY_LOGS is for, and it is the test these entries kept failing. The
# escapes went first, then they came back as base64 — a third the width and
# exactly as useless, because nobody reads a key by eye in any alphabet. What a
# watcher wants from a field they cannot read is its shape: that it is there, and
# how big. Anyone who wants the bytes wants the whole log untouched, and
# `TIDY_LOGS=false` is where that lives.
#
# So: `mac` renders, because a MAC address in colon-hex is a thing a person reads
# and recognises. `size` says `<32 bytes>`. `redact` says the same and means it.
_BYTE_FIELDS = {
  "macaddr": "mac",
  # A public key, a device id and the body of a packet encrypted for a channel
  # this collector has no key to. None of the three is legible to anybody, and
  # the two identities that matter — `!eeb826a4` and the node's names — are on
  # the same line already.
  "public_key": "size",
  "device_id": "size",
  "encrypted": "size",
  # Secrets. Same shape, different reason: these are withheld rather than merely
  # not worth printing, and `TIDY_LOGS=false` does bring them back.
  "psk": _REDACT,
  "private_key": _REDACT,
}

# `name: "escaped bytes"` as protobuf's text format writes one.
_BYTE_FIELD = re.compile(r'\b(\w+):\s*("(?:[^"\\]|\\.)*")')




def _rewrite_bytes(name: str, raw: bytes) -> str:
  """One escaped byte field as something readable, or as nothing at all."""
  style = _BYTE_FIELDS[name]

  if style is _REDACT:
    # The length is kept because it is the one fact about a key worth having in
    # a log — a truncated or absent one is a real failure and looks identical to
    # a healthy one once the bytes are gone.
    return f"{name}: <redacted {len(raw)} bytes>"

  if style == "mac" and len(raw) == 6:
    return f"{name}: {':'.join(f'{b:02x}' for b in raw)}"

  return f"{name}: <{len(raw)} bytes>"




def _annotate_fields(message: str) -> str:
  """Re-encode every escaped byte field on the line, and redact the two secrets.

  Runs over the whole message rather than one known field, because "which fields
  arrive as escapes" is a question about the library's output and not a list this
  project gets to assume it knows. A field with no entry in `_BYTE_FIELDS` is
  left exactly as it came — the same rule the payload decoder follows for a
  portnum it cannot name.
  """
  def replace(match: re.Match) -> str:
    name, literal = match.group(1), match.group(2)
    if name not in _BYTE_FIELDS:
      return match.group(0)
    try:
      raw = ast.literal_eval("b" + literal)
    except (ValueError, SyntaxError):
      # Not a byte string this can evaluate. Left alone rather than guessed at —
      # except that a secret it cannot read is still a secret it must not print.
      return f"{name}: <redacted>" if _BYTE_FIELDS[name] is _REDACT else match.group(0)
    return _rewrite_bytes(name, raw)

  return _BYTE_FIELD.sub(replace, message)




def _frame_size(literal: str) -> str:
  """` (139 bytes)` for a Python bytes repr, or nothing if it will not evaluate.

  Nothing rather than a guess: `len` of the repr would count the escapes, which
  for a frame of mostly non-printable bytes is about four times the answer.
  """
  try:
    return f" ({len(ast.literal_eval(literal))} bytes)"
  except (ValueError, SyntaxError):
    return ""




def _annotate_payload(message: str) -> str:
  """Append a decoded reading after the payload's escapes, leaving them in place.

  Additive on purpose: the escaped bytes stay the record of what arrived, and the
  decode sits beside them as a convenience. If the decode is wrong, the evidence
  that shows it is still on the same line.
  """
  portnum_match = _PORTNUM.search(message)
  payload_match = _PAYLOAD.search(message)
  if not portnum_match or not payload_match:
    return message

  try:
    payload = ast.literal_eval("b" + payload_match.group(1))
  except (ValueError, SyntaxError):
    return message

  description = _describe_payload(portnum_match.group(1), payload)

  # **When the reading exists, it replaces the bytes rather than trailing them.**
  # The bytes were kept as the evidence that would show a wrong reading, which was
  # a good reason for a log nobody reads and a bad one for a log somebody watches:
  # a reader who wants to check a decode wants the whole record untouched, and
  # that is what `TIDY_LOGS=false` is. What stays is the size, which the reading
  # does not carry and which is how a truncated payload looks different from a
  # short one.
  #
  # With no reading, the bytes are the only account of the packet there is, so
  # they stay — in base64, because at that point compact and complete is the best
  # available and there is nothing to say about them instead.
  carried = (f"<{len(payload)} bytes>" if description
             else base64.b64encode(payload).decode("ascii"))
  suffix = f" ({description})" if description else ""

  # **base64, and the escapes go.** This was additive for a while — the decoded
  # reading appended and the octal left in front of it — on the reasoning that a
  # summary can be wrong and its evidence should stay on the line. The reasoning
  # holds. The form was the mistake: a hundred and ten characters of `\\207Lvj`
  # does not read as evidence, it reads as something the collector failed to
  # handle, and Jason asked four separate times whether these lines were being
  # parsed. They were, every time. A line that has to be explained four times is
  # a line that is not saying what it means.
  #
  # base64 keeps every byte — it round-trips — so nothing that made the escapes
  # worth keeping is given up, and it is a third shorter and reads as data. It is
  # also what `_BYTE_FIELDS` already does to every other byte field on the line,
  # so the payload stops being the one field written in a different alphabet.
  return (
    message[:payload_match.start(1)]
    + carried
    + suffix
    + message[payload_match.end(1):]
  )




class _TidyLogFilter(logging.Filter):
  """Make the meshtastic library's DEBUG output readable.

  Installed only when TIDY_LOGS is on, so switching it off gives back the
  library's raw output byte for byte — this filter is the only thing that
  touches those records.
  """

  def filter(self, record: logging.LogRecord) -> bool:
    message = record.getMessage()

    if message.startswith(_HEARTBEAT_INTERVAL_PREFIX):
      # The one line kept. Rewriting msg and clearing args means the record
      # formats to exactly this, whatever the configured interval was.
      return self._rewrite(record, "Heartbeat")

    if message == _HEARTBEAT_SEND or message == _HEARTBEAT_HEADER:
      return False

    if self._is_idle_queue_reply(message):
      return False

    match = _RESTATEMENT.match(message)
    if match:
      packet_id = _PACKET_ID.search(message)
      suffix = f" (id {packet_id.group(1)})" if packet_id else ""
      return self._rewrite(record, f"{match.group(1)}{suffix}")

    frame = _RAW_FRAME.match(message)
    if frame:
      return self._rewrite(record, f"{frame.group(1)}{_frame_size(frame.group(2))}")

    if "\n" in message:
      # **One record is one line.** A protobuf's `str()` carries newlines, and the
      # library embeds protobufs in things it logs — under `Received from radio:`,
      # and under the `'raw'` key of the position dicts it traces as `p:{...}`.
      # Python's logging prefixes the record once and lets the rest of the lines
      # out bare, so what was one event reads as a dozen: nine `longitude_i:` and
      # `altitude:` lines with no level on them, and, under the testbed's
      # supervisor — which stamps every line it reads from the child — nine
      # timestamps identical to the millisecond. Jason read that as a quoting bug
      # in the collector, which is exactly what it looks like.
      #
      # **This was `Received from radio:` only, and the prefix was the bug.** The
      # flattening was written for the one multi-line record anyone had looked at
      # rather than for the shape it has, so every other one kept coming apart.
      #
      # Below the two collapses above, deliberately: those match on a prefix and
      # their records are multi-line too, so flattening first would take the
      # branch that keeps the whole restatement instead of the one that throws it
      # away. Per-line strip rather than a blanket whitespace collapse — it drops
      # the indentation without touching spacing inside a quoted payload.
      #
      # `_annotate_payload` returns what it was given when there is no payload to
      # read, so this stays the one place a flattened record is described.
      flattened = " ".join(
        line.strip() for line in message.splitlines() if line.strip()
      )
      return self._rewrite(record, _annotate_fields(_annotate_payload(flattened)))

    # **Single-line records carry escaped fields too**, and for a while nothing
    # looked at them: a `config { security { ... } }` handshake is one line, and
    # it is the line with the radio's own private key on it.
    if _BYTE_FIELD.search(message):
      annotated = _annotate_fields(_annotate_payload(message))
      if annotated != message:
        return self._rewrite(record, annotated)

    return True




  @staticmethod
  def _rewrite(record: logging.LogRecord, message: str) -> bool:
    record.msg = message
    record.args = ()
    return True




  def _is_idle_queue_reply(self, message: str) -> bool:
    if message == _IDLE_QUEUE_BYTES:
      return True

    # One record, not four: the protobuf repr embeds its own newlines, so the
    # whole "queueStatus { free: N maxlen: N }" block is a single line here.
    if message.startswith(_QUEUE_STATUS_PREFIX):
      fields = dict(
        (key.strip(), value.strip())
        for key, _, value in (
          line.partition(":") for line in message.splitlines()
        )
        if key.strip() in ("free", "maxlen")
      )
      return (
        len(fields) == 2
        and fields["free"] == fields["maxlen"]
        and "mesh_packet_id" not in message
      )

    if message.startswith(_TX_QUEUE_PREFIX):
      # "TX QUEUE free 16 of 16, res = 0, id = 00000000" (library emits a
      # trailing space, hence the strip).
      head, _, tail = message.partition(", ")
      parts = head.split()
      return (
        len(parts) == 6
        and parts[4] == "of"
        and parts[3] == parts[5]
        and tail.strip() == "res = 0, id = 00000000"
      )

    return False




def _tidy_logs() -> bool:
  return bool(Config.get("TIDY_LOGS", True))




def _metrics_suffix(snr, rssi, hop_count) -> str:
  """The radio quality behind a message, as a trailing bracket.

  The collector already computes all three to store them; they were simply never
  logged, so an INFO line said a message arrived but not how well. Returns "" when
  TIDY_LOGS is off, which leaves the traffic lines exactly as they were.
  """
  if not _tidy_logs():
    return ""

  parts = []
  for label, value, spec in (
    ("snr", snr, ".1f"), ("rssi", rssi, ".0f"), ("hops", hop_count, "d"),
  ):
    if value is None:
      continue
    try:
      parts.append(f"{label} {value:{spec}}")
    except (TypeError, ValueError):
      # Metrics come straight off the packet dict; a surprising type should
      # cost that one field, not the log line it was decorating.
      continue

  return f"  [{' '.join(parts)}]" if parts else ""




def _format_changes(changed: dict) -> str:
  """Node field changes as `voltage 3.91, rssi -30` rather than a Python dict."""
  if not _tidy_logs():
    return repr(changed)

  return ", ".join(f"{key} {value}" for key, value in changed.items())




def _configure_logging() -> None:
  log_level = logging.DEBUG if Config.get("DEBUG", False) else logging.INFO
  logging.basicConfig(level=log_level, format=LOG_FORMAT)

  if _tidy_logs():
    # Attached to the handler, not the meshtastic logger: the records are
    # emitted by several modules under that name, and a handler-level filter
    # catches them all without this needing to know which.
    tidy_filter = _TidyLogFilter()
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

  collector = MeshtasticCollector(db=db)
  _install_signal_handlers(collector)
  collector.start()




