"""The tidy log: the meshtastic library's DEBUG output, made readable.

The library at DEBUG is mostly repetition. A heartbeat every 300 seconds logs
the same three byte-identical lines; every inbound frame is logged once as the
raw bytes read off the serial port and again, on the very next line, as the
protobuf those bytes parsed into; then the same packet is restated twice more on
its way through the handler and out to pubsub. TIDY_LOGS is the setting that
says a person is watching, and everything below is what that setting buys — the
repetitions collapsed, the multi-line protobuf dumps flattened to one record per
event, the escaped byte fields rewritten into something a reader can use, and
the payload decoded back into the numbers it already contains.

**Two of those byte fields are secrets, and they are removed rather than
rendered.** A collector at DEBUG logs its own `config { security { private_key
} }` once at the handshake, with every channel's `psk` beside it, and a log gets
pasted into a handoff, a terminal, an issue. `_BYTE_FIELDS` is where that is
decided, and `TIDY_LOGS=false` — the whole filter uninstalled — is the only way
to get those bytes back.

**Nothing here touches the archive, and nothing here imports the radio library
at module scope.** That is why this lives up beside config.py and selflog.py
rather than inside mesh_collector.collector, whose __init__ imports meshtastic
the moment anything under it is touched: a log formatter that cannot be imported
without a serial library is a log formatter with no tests, which is what this
was. The one function that genuinely needs the protobuf definitions,
`_describe_payload`, imports them inside itself — see the comment there, because
that indent is load-bearing rather than a matter of taste.

The collector imports `tidy_logs`, `metrics_suffix` and `format_changes` back
out of here, and its `_configure_logging` installs `TidyLogFilter`. Those four
are this module's entire surface and are unprefixed to say so; everything else
below is private to the formatting and keeps its underscore.
"""

from __future__ import annotations

import ast
import base64
import logging
import re

from typing import Optional

from mesh_collector.config import Config




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
# those three words went on arriving in pieces. See `TidyLogFilter.filter`, which
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

  # **The radio library, imported here rather than at the top of the file — and
  # this one indented import is what the move out of the collector was for.**
  # These four protobuf definitions and the two enum names below are the only
  # things in this module that need meshtastic installed, and at module scope
  # they took the whole file hostage: importing `_BYTE_FIELDS` to check that a
  # private key is redacted meant importing a serial library, which the testbed
  # deliberately does not install, which is why that redaction had never been
  # tested. Past this line the function needs the radio; the module, and
  # everything else in it, imports and runs without one.
  #
  # A repeated `from … import` is a sys.modules lookup, and building four dict
  # entries is nothing against the regex and the protobuf parse this function is
  # already doing — so this is per-call rather than cached behind module state,
  # because a lazy import is easy to read and a half-initialised module global is
  # not.
  from meshtastic.protobuf import mesh_pb2, storeforward_pb2, telemetry_pb2

  # Only the payloads worth reading at a glance. Anything else keeps its escapes,
  # which is the honest outcome — a blob this cannot name is a blob.
  #
  # `TEXT_MESSAGE_APP` is not here and is not an oversight: its payload is not a
  # protobuf at all, it is the message as UTF-8 bytes, so it is handled above
  # this lookup rather than through it — which is also what keeps the text branch
  # working with no radio library present.
  proto = {
    "TELEMETRY_APP": telemetry_pb2.Telemetry,
    "POSITION_APP": mesh_pb2.Position,
    "NODEINFO_APP": mesh_pb2.User,
    "STORE_FORWARD_APP": storeforward_pb2.StoreAndForward,
  }.get(portnum)
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
# **Nothing checks this table for completeness, and this comment used to say
# something did.** A one-off script walked a corpus of real records and asserted no
# escaped field was missing from it — that is how the ones below were found:
# `payload` was decoded, `payload` was the only thing anybody had looked at, and 171
# escaped strings in one session were not payloads. That script is in no repo, on no
# machine anyone can find, and in no git history, and nothing has replaced it.
# `test_logfmt.py` covers what the entries below *do*, which is a different claim
# and a weaker one: it cannot notice a field the table has never heard of. So a
# field added by a firmware or library update arrives as a hundred characters of
# octal that everyone learns to skip over — and if it is a secret, it arrives in
# full. Adding an entry here is still a manual job done by reading a DEBUG session.
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




class TidyLogFilter(logging.Filter):
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




def tidy_logs() -> bool:
  """Whether TIDY_LOGS is on — the one setting everything above answers to.

  Public because the collector asks it too, before it decides whether to hold a
  Self line, and because `_configure_logging` asks it before installing the
  filter at all. The default is True in the config surface and repeated here so
  that a caller reaching this with no config loaded gets the shipped behaviour
  rather than a quietly untidy log.
  """
  return bool(Config.get("TIDY_LOGS", True))




def metrics_suffix(snr, rssi, hop_count) -> str:
  """The radio quality behind a message, as a trailing bracket.

  The collector already computes all three to store them; they were simply never
  logged, so an INFO line said a message arrived but not how well. Returns "" when
  TIDY_LOGS is off, which leaves the traffic lines exactly as they were.
  """
  if not tidy_logs():
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




def format_changes(changed: dict) -> str:
  """Node field changes as `voltage 3.91, rssi -30` rather than a Python dict."""
  if not tidy_logs():
    return repr(changed)

  return ", ".join(f"{key} {value}" for key, value in changed.items())
