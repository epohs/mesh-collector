"""How to reach the node: the config-to-constructor decision, made in one place.

The collector spoke `SerialInterface` to a USB-attached radio and nothing else.
Two deployments needed more — a node running `meshtasticd` on the same Pi, reached
over TCP, and BLE for a node no cable reaches — and all three interface classes
subclass `MeshInterface`, which the collector already types every downstream
touchpoint against. So the whole difference between the modes is *which class to
construct and with what*, and that is what this module answers: a validated
`TransportSpec` the collector can act on without a branch per call site.

**This module imports no meshtastic**, which is the reason it lives up here beside
config.py and selflog.py rather than inside `mesh_collector.collector`, whose
`__init__` imports the radio library the moment anything under it is touched. The
testbed deliberately has no meshtastic and no interface mock (the reasoning is
written out at `testbed/tests/test_self_log.py:11-15`), so a transport-free module
is the only way the mode-resolution logic gets tested at all. `kwargs` names the
constructor's parameters as plain strings; the collector subpackage does the
import and the call.

**Validation here fails loudly on purpose, because the config loader will not.**
`Config` ignores keys it doesn't know and keeps the default when a value has the
wrong *type* — but `CONNECTION_MODE=tpc` is a perfectly good string, so it passes
every check config.py makes and arrives here intact. If this module were lenient
too, a typo would silently fall through to serial and the operator would be left
wondering why their TCP settings did nothing.
"""

from __future__ import annotations

from typing import Any, Mapping, NamedTuple

from .config import Config


SERIAL = "serial"
TCP = "tcp"
BLE = "ble"

MODES = (SERIAL, TCP, BLE)


class TransportError(ValueError):
  """A CONNECTION_MODE the collector cannot act on.

  A ValueError because that is what it is — a bad config value — and a named type
  so `start()` can catch exactly this and exit with the message rather than a
  traceback. Raised at resolution time, before any hardware is touched, so a
  misconfigured collector fails in its first second instead of half-open.
  """


class TransportSpec(NamedTuple):
  """A resolved decision: what to construct, what to call it in the log, and
  whether to watch it.

  `label` is the human-facing name of the link — `/dev/ttyACM0`,
  `tcp://localhost:4403`, `ble://AA:BB:CC:DD:EE:FF`. It exists so log lines and
  warnings can name the transport without every one of them re-deriving it from
  the mode, which is how "Failed to open the serial port" ended up hardcoded in
  messages that now have three ways to be wrong.

  `kwargs` are keyword arguments for the mode's interface class. All three
  classes accept their connection detail by keyword (`devPath`, `hostname` +
  `portNumber`, `address`), so the caller never has to remember which is
  positional.
  """

  mode: str
  label: str
  kwargs: dict[str, Any]
  liveness_timeout: int




def resolve(settings: Mapping[str, Any]) -> TransportSpec:
  """Turn a settings mapping into a validated spec, or raise TransportError.

  Pure — it reads the mapping it is handed and touches no global state, which is
  what makes it the testable half of this module. `from_config()` is the wrapper
  that supplies `Config`.
  """
  # Case and whitespace are the operator's slip, not a different mode: an
  # env var set to "TCP " should work, and a mode we can normalize into a
  # real one is not worth failing on.
  raw_mode = settings.get("CONNECTION_MODE", SERIAL)
  mode = str(raw_mode).strip().lower()

  if mode not in MODES:
    raise TransportError(
      f"CONNECTION_MODE={raw_mode!r} is not a connection mode; "
      f"expected one of {', '.join(MODES)}"
    )

  # 0 disables the watchdog and is the default for every mode. Negative is
  # meaningless rather than harmless — it would fire on every pass of the main
  # loop — so it is a typo worth naming instead of silently clamping.
  liveness_timeout = _positive_int(settings.get("LIVENESS_TIMEOUT", 0), "LIVENESS_TIMEOUT")

  if mode == SERIAL:
    port = str(settings.get("SERIAL_PORT", "")).strip()
    if not port:
      raise TransportError("CONNECTION_MODE=serial needs SERIAL_PORT set to a device path")
    return TransportSpec(SERIAL, port, {"devPath": port}, liveness_timeout)

  if mode == TCP:
    host = str(settings.get("TCP_HOST", "")).strip()
    if not host:
      raise TransportError("CONNECTION_MODE=tcp needs TCP_HOST set to a hostname or IP address")
    port_number = _positive_int(settings.get("TCP_PORT", 0), "TCP_PORT")
    if not 1 <= port_number <= 65535:
      raise TransportError(f"TCP_PORT={port_number} is outside the port range 1-65535")
    return TransportSpec(
      TCP,
      f"tcp://{host}:{port_number}",
      {"hostname": host, "portNumber": port_number},
      liveness_timeout,
    )

  # BLE. The address is required: BLEInterface(None) is legal and means "probe
  # for any device", which on a mesh with more than one node in Bluetooth range
  # attaches to whichever answered first. A collector that archives from a
  # different node each restart is worse than one that refuses to start.
  address = str(settings.get("BLE_ADDRESS", "")).strip()
  if not address:
    raise TransportError(
      "CONNECTION_MODE=ble needs BLE_ADDRESS set to a MAC address or advertised "
      "name; run `meshtastic --ble-scan` to find it"
    )
  return TransportSpec(BLE, f"ble://{address}", {"address": address}, liveness_timeout)




def from_config() -> TransportSpec:
  """Resolve from `Config`. The collector's entry point into this module."""
  Config.load()  # Load-once; a no-op if the entry point already called it.
  return resolve(Config.values)




def _positive_int(value: Any, key: str) -> int:
  """A non-negative int that is not a bool, or a TransportError naming the key.

  The env path already casts to int (`_cast_env_value`) and the file path already
  rejects a mismatched type, so this catches the third door: a default edited by
  hand, or a settings mapping handed straight to `resolve` by a test or a caller
  that isn't `Config`. bool is excluded before int for the reason config.py gives
  at `_matches_default_type` — it is a subclass, and True is not a port number.
  """
  if isinstance(value, bool) or not isinstance(value, int):
    raise TransportError(f"{key}={value!r} is not a whole number")
  if value < 0:
    raise TransportError(f"{key}={value} cannot be negative")
  return value
