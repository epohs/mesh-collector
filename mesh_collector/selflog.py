"""The tidy log's Self entry: the attached device's updates, one line per period.

The attached device reports its own metrics over the serial API every minute or
two, and each report used to land in the log as its own `Node !eeb826a4 updated:`
line — so a quiet mesh produced a log that was mostly the collector talking about
its own radio. TIDY_LOGS is for exactly this kind of noise, and this module is
its answer here: the device's field changes are folded into one held record, and
a single `Self !eeb826a4 updated:` line goes out once per
TIDY_LOG_LOCAL_NODE_PERIOD, carrying every field that changed during the window
at its newest value. Only the *log* is grouped — the node row in the archive
still updates on every report, at full frequency, for the readers.

**The held state lives in the archive's meta table rather than in memory**, as a
JSON value under `PENDING_KEY`, so a restart continues the window instead of
resetting it: systemd restarting the collector every few minutes (an unplugged
radio, say) would otherwise turn "one line per fifteen minutes" back into a line
per restart. The state names the node it describes, and `record` discards state
naming any other node — the belt to `reset`'s braces, which the device-swap path
calls explicitly. Either way, a Self line can never be assembled from a device
that is no longer plugged in.

**This module never logs and never reads the clock or the config.** The caller
passes `now` and the period and decides what to do with what comes back, which
is also what keeps this importable and testable without the meshtastic library
the collector subpackage needs — and why it lives up here beside config.py and
db rather than inside mesh_collector.collector, whose __init__ imports the
radio library the moment anything under it is touched.
"""

from __future__ import annotations

import json

from typing import Optional


# The meta key the held state lives under. Named after the setting that governs
# it (TIDY_LOG_LOCAL_NODE_PERIOD) so a reader of config.json and a reader of the
# meta table find each other. Documented in schema.sql as writer-private: no
# reader selects it, and its shape is this module's to change.
PENDING_KEY = "tidy_log_local_node"




def record(
  storage,
  node_id: str,
  changed: dict,
  period_seconds: float,
  now: float,
) -> Optional[dict]:
  """Fold one update into the held state. Returns what to log now, or None to hold.

  The returned dict is every field that changed since the last Self line, each at
  its newest value — a field that changed twice in the window appears once, as
  what it is now. None means the window is still open and the caller should say
  nothing (or say it at DEBUG, which is the caller's business).

  A first-ever update — fresh archive, state cleared by a swap, state naming a
  different node — logs immediately rather than opening a silent window: the
  first Self line is what says the grouping is working at all, and after a swap
  it is what names the new device.
  """
  held, logged_at = _load(storage, node_id)
  held.update(changed)

  # `logged_at <= now` is the clock guard, and it is for the Pi specifically: no
  # RTC, so a boot before NTP arrives reads a time years behind the stamp the
  # last run wrote. `now - logged_at` is then hugely negative and the window
  # would hold for the difference — silence measured in years. A stamp from the
  # future means the clock stepped back, and the honest response is to treat the
  # window as expired: one extra Self line around a reboot, never a mute log.
  if logged_at is not None and logged_at <= now and now - logged_at < period_seconds:
    _store(storage, node_id, held, logged_at)
    return None

  _store(storage, node_id, {}, now)
  return held




def reset(storage) -> None:
  """Forget the held state entirely. For the device-swap path: the state
  describes a device that is gone, and deleting the row — rather than emptying
  it — leaves nothing that could name the old node under the new one's id."""
  storage.delete_meta(PENDING_KEY)




def _load(storage, node_id: str) -> tuple[dict, Optional[float]]:
  """The held changes and the last Self line's timestamp, or a fresh start.

  Everything that is not a well-formed record for *this* node comes back as
  `({}, None)` — absent key, unparseable JSON, a different node's state. Fresh
  means `record` logs immediately, so a corrupted row costs one early line and
  never a stuck window.
  """
  raw = storage.get_meta(PENDING_KEY)
  if not raw:
    return {}, None

  try:
    state = json.loads(raw)
  except ValueError:
    return {}, None

  if not isinstance(state, dict) or state.get("node_id") != node_id:
    return {}, None

  held = state.get("changes")
  logged_at = state.get("logged_at")
  return (
    held if isinstance(held, dict) else {},
    # bool is an int and `True` is not a timestamp; the same discrimination
    # config.py applies to its own values.
    logged_at if isinstance(logged_at, (int, float)) and not isinstance(logged_at, bool) else None,
  )




def _store(storage, node_id: str, held: dict, logged_at: float) -> None:
  storage.set_meta(PENDING_KEY, json.dumps({
    "node_id": node_id,
    "logged_at": logged_at,
    "changes": held,
  }))
