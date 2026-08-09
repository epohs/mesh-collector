from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


# Settings are grouped by which process owns them. A process loads only its own
# surface, so a setting can't be read — or set from the environment — by a
# process that has no business acting on it.
#
# These groups were the seam the project split followed. This project kept
# SHARED_CONFIG plus COLLECTOR_CONFIG; the presentation group left with the
# readers, and this file is free to diverge from their copies.

# Needed by anything that opens the archive.
SHARED_CONFIG = {
  "DEBUG": False,                     # Enable verbose logging and disable css/js minification
  "DB_PATH": "data/db.sqlite",        # Path to SqLite database
}

# Acquiring mesh data and storing it. Owned by the collector, which is the only writer.
COLLECTOR_CONFIG = {
  "MAX_MESSAGES": 1000,               # Max channel messages to keep across all channels
  "MAX_DIRECT_MESSAGES": 1000,        # Max direct messages to keep
  "PRUNE_INTERVAL": 5,                # Only attempt pruning every X writes
  "NODE_PRUNE_DAYS": 14,              # Nodes unseen in X days will be pruned
  "SERIAL_PORT": "/dev/ttyACM0",      # Meshtastic serial device location
  "STORE_DIRECT_MESSAGES": False,     # Should the collector archive direct messages
  "LOG_PRIMARY_CHANNEL": True,        # Should we track primary channel messages
  "PRIMARY_CHANNEL": 0,               # Primary channel index (usually 0)
  "LOG_CHANNEL_IDS": [],              # Additional channel indexes to track
  "ALLOW_DESTRUCTIVE_REBUILD": False, # Permit wiping the database when schema.sql version changes
  "TIDY_LOGS": True,                  # Format mesh traffic and collapse library chatter; off logs raw
}

# Transmitting. Its own group because it is the one thing in this project that
# is not archiving, and because it is off by default and meant to stay that way
# unless somebody has decided otherwise. With ENABLE_TX false no socket is
# created, nothing can ask this process to transmit, and a collector is exactly
# what it was before this group existed.
TRANSMIT_CONFIG = {
  "ENABLE_TX": False,                 # Host a control socket and transmit for clients that ask
  "CONTROL_SOCKET_PATH": "",          # Socket location; empty uses the platform default
}

COLLECTOR_SETTINGS = {**SHARED_CONFIG, **COLLECTOR_CONFIG, **TRANSMIT_CONFIG}

CONFIG_FILE_PATH = Path(__file__).parent / "config.json"
SAMPLE_CONFIG_FILE_PATH = Path(__file__).parent / "config-sample.json"

# Environment variable names are prefixed per process, so co-hosted processes
# don't share one namespace: this collector reads MESH_COLLECTOR_DB_PATH, while
# a reader on the same host reads its own RXONLY_DB_PATH or MESH_CONSOLE_DB_PATH.
# Keys in config.json stay unprefixed, which is what lets one shared config.json
# serve every process while each still sees only its own surface.
DEFAULT_ENV_PREFIX = "MESH_COLLECTOR_"




class Config:
  """
  Central configuration loader.
  Priority: environment variables > config.json > defaults.
  """

  values: dict[str, Any] = {}
  env_prefix: str = DEFAULT_ENV_PREFIX
  _loaded: bool = False




  @classmethod
  def load(
    cls,
    env_prefix: str = DEFAULT_ENV_PREFIX,
    settings: dict[str, Any] = COLLECTOR_SETTINGS,
  ) -> None:
    """Load configuration values. Only runs once.

    This project has one entry point and one surface, so both arguments default
    to it. They stay parameters because the filtering is what keeps a setting
    exported or written for another co-hosted process from reconfiguring this
    one. Keys outside the surface are ignored, wherever they came from.
    """
    if cls._loaded:
      return

    cls.env_prefix = env_prefix
    cls.values = settings.copy()

    if CONFIG_FILE_PATH.exists():
      try:
        with open(CONFIG_FILE_PATH, "r") as f:
          file_config = json.load(f)
        for key, value in file_config.items():
          if key not in settings:
            continue
          if not cls._matches_default_type(value, settings[key]):
            # Keeping the default fails closed; coercing would let the *string*
            # "false" turn ALLOW_DESTRUCTIVE_REBUILD or ENABLE_TX on, because
            # any non-empty string is truthy.
            print(
              f"Warning: config.json {key}={value!r} is not a "
              f"{type(settings[key]).__name__}; keeping the default {settings[key]!r}"
            )
            continue
          cls.values[key] = value
      except Exception as e:
        print(f"Warning: Failed to read {CONFIG_FILE_PATH}: {e}")

    for key, default_val in settings.items():
      env_key = f"{cls.env_prefix}{key}"
      env_val = os.getenv(env_key)
      if env_val is not None:
        try:
          cls.values[key] = cls._cast_env_value(env_val, default_val)
        except Exception:
          print(f"Warning: Could not cast environment variable {env_key}='{env_val}'")

    # config.json may set these to null explicitly; fall back to the default.
    if "LOG_PRIMARY_CHANNEL" in cls.values and cls.values["LOG_PRIMARY_CHANNEL"] is None:
      cls.values["LOG_PRIMARY_CHANNEL"] = True
    if "LOG_CHANNEL_IDS" in cls.values and cls.values["LOG_CHANNEL_IDS"] is None:
      cls.values["LOG_CHANNEL_IDS"] = []

    cls._loaded = True




  @classmethod
  def get(cls, key: str, default: Any = None) -> Any:
    """Retrieve a config value by key."""
    if not cls._loaded:
      cls.load()
    return cls.values.get(key, default)




  @staticmethod
  def _matches_default_type(value: Any, default_val: Any) -> bool:
    """Whether a config.json value has the type its default establishes.

    None passes everywhere: JSON null means "explicitly unset", and the
    settings that accept it (LOG_PRIMARY_CHANNEL, LOG_CHANNEL_IDS) are
    normalized back to their defaults after the merge. bool is checked before
    int because it is a subclass of it, in both directions: True is not a
    channel index, and 1 is not an authorization.
    """
    if value is None:
      return True
    if isinstance(default_val, bool):
      return isinstance(value, bool)
    if isinstance(default_val, int):
      return isinstance(value, int) and not isinstance(value, bool)
    if isinstance(default_val, float):
      return isinstance(value, (int, float)) and not isinstance(value, bool)
    if isinstance(default_val, list):
      # Channel index lists: every element an int, and not a bool pretending.
      return isinstance(value, list) and all(
        isinstance(item, int) and not isinstance(item, bool) for item in value
      )
    if isinstance(default_val, str):
      return isinstance(value, str)
    return True




  @staticmethod
  def _cast_env_value(env_val: str, default_val: Any) -> Any:
    """Cast environment variable string to the type of default_val."""
    if isinstance(default_val, bool):
      return env_val.lower() in ("true", "1", "yes")
    if isinstance(default_val, int):
      return int(env_val)
    if isinstance(default_val, float):
      return float(env_val)
    if isinstance(default_val, list):
      # Channel index lists arrive comma-separated: "0,2,3"
      return [int(part) for part in env_val.split(",") if part.strip()]
    return env_val
