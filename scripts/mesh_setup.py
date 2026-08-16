#!/usr/bin/env python3
"""MeshSuite setup — clone, interview, configure, and render deploy artifacts.

Usage:
    python3 scripts/mesh_setup.py [--probe]

Stdlib only: no venv needed. Run from anywhere; the script locates itself and
the collector checkout when it is inside one.
"""

from __future__ import annotations

import collections
import copy
import datetime
import itertools
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import textwrap
import uuid

from pathlib import Path
from typing import Any, Callable


# ---------------------------------------------------------------------------
# Layer 1 — the interview, as data
# ---------------------------------------------------------------------------

Question = tuple[
  str,                                    # key
  str,                                    # prompt
  Any,                                    # default
  Callable[[str], Any] | None,            # cast (None = identity)
  Callable[[Any], str | None] | None,     # validate (None = always valid)
  Callable[[dict, set, bool], bool],      # asked_when(answers, selections, deploying)
]

REPO_URLS = {
  "collector": "https://github.com/epohs/mesh-collector.git",
  "rxonly":    "https://github.com/epohs/RxOnly.git",
  "console":   "https://github.com/epohs/mesh-console.git",
  "mesh-link": "https://github.com/epohs/mesh-link.git",
}

# Projects that consume mesh-link and therefore need the uv.toml override
# when mesh-link is not a sibling.
CONSUMES_MESH_LINK = {"collector", "console"}

# The extras each project needs for transmit/send
EXTRAS = {
  "collector": "tx",
  "console": "send",
}

PRESETS: dict[str, tuple[set[str], str]] = {
  "1": ({"collector"}, "Archive only"),
  "2": ({"collector", "rxonly"}, "Archive + public dashboard"),
  "3": ({"collector", "rxonly", "console"}, "Archive + dashboard + terminal console"),
  "4": ({"collector", "rxonly", "console", "mesh-link"}, "Full suite with transmit"),
}


def _always(_a: dict, _s: set, _d: bool) -> bool:
  return True


def _never(_a: dict, _s: set, _d: bool) -> bool:
  return False


def _when_deploying(_a: dict, _s: set, deploying: bool) -> bool:
  return deploying


def _when_transmit(_a: dict, selections: set, _d: bool) -> bool:
  return "mesh-link" in selections


def _when_rxonly(_a: dict, selections: set, _d: bool) -> bool:
  return "rxonly" in selections


def _when_console(_a: dict, selections: set, _d: bool) -> bool:
  return "console" in selections


def _when_transmit_deploy(_a: dict, selections: set, deploying: bool) -> bool:
  return "mesh-link" in selections and deploying


def _cast_int(val: str) -> int:
  return int(val.strip())


def _cast_int_list(val: str) -> list[int]:
  val = val.strip()
  if not val:
    return []
  parts = [p.strip() for p in val.split(",") if p.strip()]
  return [int(p) for p in parts]


def _validate_not_empty(val: Any) -> str | None:
  if not val or (isinstance(val, str) and not val.strip()):
    return "cannot be empty"
  return None


def _validate_int_range(lo: int, hi: int) -> Callable[[Any], str | None]:
  def _check(val: Any) -> str | None:
    if not isinstance(val, int):
      return "must be an integer"
    if val < lo or val > hi:
      return f"must be between {lo} and {hi}"
    return None
  return _check


def _validate_domain(val: Any) -> str | None:
  if not isinstance(val, str) or not val.strip():
    return "cannot be empty"
  if "." not in val:
    return "must be a domain name (e.g. mesh.example.com)"
  return None


def _parse_bool(val: str) -> bool:
  return val.strip().lower() in ("y", "yes", "true", "1")


# Question table.
#
# Each entry: (key, prompt, default, cast, validate, asked_when)
# `cast` converts the raw input string; `validate` returns None if ok, error str if not.
# `asked_when(answers, selections, deploying) -> bool` controls visibility.
#
# Prompts show the default in square brackets. A bare enter means "accept default",
# which under the write rule means the key is OMITTED from config.json (deferring
# to the checkout's shipped default). Typing any value, even one equal to the
# default, counts as a decision and writes the key.
QUESTIONS: list[Question] = [
  # NOTE: parent_dir, custom_locations, and preset are handled by main()
  # in the Layout phase, not via the QUESTIONS table.

  # --- Collector ---
  ("serial_port",
     "Serial port for the Meshtastic device",
     "/dev/ttyACM0",
     None,
     _validate_not_empty,
     _always),

  ("db_path",
     "Archive location (relative to collector checkout)",
     "data/db.sqlite",
     None,
     _validate_not_empty,
     _always),

  ("primary_channel",
     "Primary channel index",
     0,
     _cast_int,
     _validate_int_range(0, 255),
     _always),

  ("additional_channels",
     "Additional tracked channel indexes (comma-separated, enter=none)",
     "",
     _cast_int_list,
     None,
     _always),

  ("archive_dms",
     "Archive direct messages? (y/n)",
     "n",
     _parse_bool,
     None,
     _always),

  # Combined retention prompt — shown as one question, stored under multiple keys.
  # The prompt handler renders the three defaults and stores answers in _retention_map.
  ("retention",
     "Retention — max messages / max direct messages / node prune days\n"
     "  Press enter to accept all defaults (1000 / 1000 / 14)",
     "accept",
     None,
     None,
     _always),

  # --- Readers ---
  ("serve_dms",
     "RxOnly: serve direct messages over the web? (y/n)",
     "n",
     _parse_bool,
     None,
     _when_rxonly),

  ("console_show_dms",
     "Console: show direct messages? (y/n)",
     "n",
     _parse_bool,
     None,
     _when_console),

  # --- Transmit ---
  ("enable_tx",
     "Enable transmit on the collector? (y/n)",
     "n",
     _parse_bool,
     None,
     _when_transmit),

  ("enable_send",
     "Enable sending from the console? (y/n)",
     "n",
     _parse_bool,
     None,
     lambda a, s, d: "console" in s and "mesh-link" in s),

  ("control_socket_path",
     "Control socket path (enter for /run/mesh-collector/control.sock)",
     "/run/mesh-collector/control.sock",
     None,
     _validate_not_empty,
     _when_transmit_deploy),

  # --- Deployment ---
  ("system_user",
     "System user for the services (enter for current user)",
     "",  # filled at runtime via getpass
     None,
     _validate_not_empty,
     _when_deploying),

  ("system_group",
     "System group for the collector service (enter for current primary group)",
     "",  # filled at runtime
     None,
     _validate_not_empty,
     _when_deploying),

  ("domain_name",
     "Domain name for the dashboard",
     "",
     None,
     _validate_domain,
     lambda a, s, d: d and "rxonly" in s),

  ("behind_cloudflare",
     "Behind Cloudflare? (y/n)",
     "n",
     _parse_bool,
     None,
     lambda a, s, d: d and "rxonly" in s),
]


# ---------------------------------------------------------------------------
# Layer 2 — pure generators
# ---------------------------------------------------------------------------

def collector_config(answers: dict) -> dict:
  """Build the collector's config.json content.

  Write rule:
  - Typed → always written.
  - Accepted-default → omitted.
  - Computed (DB_PATH, CONTROL_SOCKET_PATH) → always written.
  - ENABLE_TX written only when true.
  """
  result = {}

  # DB_PATH is always written, absolute
  result["DB_PATH"] = _absolute_db_path(answers)

  result["SERIAL_PORT"] = answers["serial_port"]

  if answers.get("primary_channel") != 0:
    result["PRIMARY_CHANNEL"] = answers["primary_channel"]
  extra_channels = answers.get("additional_channels")
  if extra_channels and isinstance(extra_channels, list) and len(extra_channels) > 0:
    result["LOG_CHANNEL_IDS"] = extra_channels

  if _bool_answer(answers, "archive_dms"):
    result["STORE_DIRECT_MESSAGES"] = True

  retention = answers.get("_retention_map", {})
  if "max_messages" in retention and retention["max_messages"] != 1000:
    result["MAX_MESSAGES"] = retention["max_messages"]
  if "max_direct_messages" in retention and retention["max_direct_messages"] != 1000:
    result["MAX_DIRECT_MESSAGES"] = retention["max_direct_messages"]
  if "node_prune_days" in retention and retention["node_prune_days"] != 14:
    result["NODE_PRUNE_DAYS"] = retention["node_prune_days"]

  if _bool_answer(answers, "enable_tx"):
    result["ENABLE_TX"] = True
    cp = answers.get("control_socket_path")
    if cp and cp != "/run/mesh-collector/control.sock":
      result["CONTROL_SOCKET_PATH"] = cp

  _strip_accepted_defaults(result, _COLLECTOR_DEFAULTS)
  return result


_COLLECTOR_DEFAULTS = {
  "SERIAL_PORT": "/dev/ttyACM0",
  "PRIMARY_CHANNEL": 0,
  "LOG_CHANNEL_IDS": [],
  "STORE_DIRECT_MESSAGES": False,
  "MAX_MESSAGES": 1000,
  "MAX_DIRECT_MESSAGES": 1000,
  "NODE_PRUNE_DAYS": 14,
  "ENABLE_TX": False,
}


def rxonly_config(answers: dict, db_path: str | None = None) -> dict:
  """Build the RxOnly reader's config.json content.

  DB_PATH is always written (computed, never a default). SERVE_DIRECT_MESSAGES
  is written only when true.
  """
  result = {}
  result["DB_PATH"] = db_path or _absolute_db_path(answers)

  if _bool_answer(answers, "serve_dms"):
    result["SERVE_DIRECT_MESSAGES"] = True

  _strip_accepted_defaults(result, _RXONLY_DEFAULTS)
  return result


_RXONLY_DEFAULTS = {
  "SERVE_DIRECT_MESSAGES": False,
}


def console_config(answers: dict, db_path: str | None = None) -> dict:
  """Build the console reader's config.json content.

  DB_PATH is always written (computed). ENABLE_SEND written only when true.
  CONTROL_SOCKET_PATH is written for readers only when transmit is enabled and
  the socket path differs from the platform default.
  LOG_COMMAND is not overridden in v1 (same-host only) — the default matches
  the shipped unit.
  """
  result = {}
  result["DB_PATH"] = db_path or _absolute_db_path(answers)

  if _bool_answer(answers, "console_show_dms"):
    result["SHOW_DIRECT_MESSAGES"] = True

  if _bool_answer(answers, "enable_send"):
    result["ENABLE_SEND"] = True

  _strip_accepted_defaults(result, _CONSOLE_DEFAULTS)
  return result


_CONSOLE_DEFAULTS = {
  "SHOW_DIRECT_MESSAGES": False,
  "ENABLE_SEND": False,
}


def _absolute_db_path(answers: dict) -> str:
  """Resolve DB_PATH to absolute, using layout_plan's collector path."""
  collector_path = answers.get("_collector_path")
  if collector_path:
    raw = answers.get("db_path", "data/db.sqlite")
    return str((Path(collector_path) / raw).resolve())
  return answers.get("db_path", "data/db.sqlite")


def _bool_answer(answers: dict, key: str) -> bool:
  val = answers.get(key)
  if isinstance(val, bool):
    return val
  if isinstance(val, str):
    return val.lower() in ("y", "yes", "true", "1")
  return False


def _strip_accepted_defaults(config: dict, defaults: dict) -> None:
  """Remove keys whose value matches the shipped default (accepted-default → omitted)."""
  for key, default in list(config.items()):
    if key in defaults and config[key] == defaults[key]:
      del config[key]


def merge_config(existing: dict, new: dict) -> dict:
  """Merge new config values into existing, preserving unknown keys."""
  merged = dict(existing)
  merged.update(new)
  return merged


def clone_plan(answers: dict) -> list[tuple[str, str, str]]:
  """Return list of (project_name, url, dest_path) for repos that need cloning.

  Skips repos whose destination already exists and matches the expected remote URL.
  Assumes layout_plan has already run and answers["_layout"] is populated.
  """
  selections = answers.get("_selections", set())
  layout = answers.get("_layout", {})
  plan = []

  for project in selections:
    dest = layout.get(project)
    if not dest:
      continue
    url = REPO_URLS.get(project)
    if not url:
      continue
    plan.append((project, url, str(dest)))

  return plan


def layout_plan(parent: str, overrides: dict[str, str],
                selections: set[str]) -> dict[str, Path]:
  """Resolve each selected project's install path.

  Default is sibling under parent. Per-project overrides from question 1a are
  honored. Raises ValueError on collisions.
  """
  layout: dict[str, Path] = {}
  parent_path = Path(parent).resolve()

  for project in sorted(selections):
    if project in overrides and overrides[project].strip():
      path = Path(overrides[project]).resolve()
    else:
      shortcut = {"collector": "mesh-collector",
                        "rxonly": "RxOnly",
                        "console": "mesh-console",
                        "mesh-link": "mesh-link"}
      path = parent_path / shortcut.get(project, project)

    if path in layout.values():
      raise ValueError(
        f"Collision: two projects would install to {path}. "
        "Use custom locations to separate them."
      )
    layout[project] = path

  return layout


def guess_install_paths(system: str | None = None) -> dict[str, Any]:
  """Return best-guess deploy paths for the host OS.

  v1 is same-host only, so the host OS == the deploy target. Linux gets
  concrete /etc/... paths; non-Linux gets degraded guidance.
  """
  os_name = system or platform.system()

  if os_name == "Linux":
    return {
      "systemd_dir": "/etc/systemd/system",
      "nginx_available": "/etc/nginx/sites-available",
      "nginx_enabled": "/etc/nginx/sites-enabled",
      "nginx_managed": True,
      "has_systemd": True,
      "systemd_user_group_hint": "dialout for serial access",
    }

  # macOS or other non-Linux
  arch = platform.machine()
  if arch in ("arm64", "aarch64"):
    nginx_homebrew = "/opt/homebrew/etc/nginx"
  else:
    nginx_homebrew = "/usr/local/etc/nginx"

  return {
    "systemd_dir": None,
    "nginx_available": nginx_homebrew,
    "nginx_enabled": nginx_homebrew + "/servers",
    "nginx_managed": False,
    "has_systemd": False,
    "systemd_user_group_hint": "N/A — no systemd on this host",
    "note": "This host does not use systemd. Deploy artifacts are for reference "
        "or for copying to a Linux target. On macOS, run interactively or "
        "use a Linux VM for persistent deployment.",
  }


def render_uv_toml(mesh_link_path: str, consumer_path: str) -> str | None:
  """Generate a uv.toml override for a project whose mesh-link isn't a sibling.

  Returns None when mesh-link IS a sibling of the consumer (no override needed).
  """
  consumer = Path(consumer_path).resolve()
  mesh_link = Path(mesh_link_path).resolve()
  sibling = (mesh_link.parent == consumer.parent)

  if sibling:
    return None

  return (
    "# Written by mesh_setup.py \u2014 this project\u2019s mesh-link is not a sibling checkout.\n"
    "# Safe to delete: a re-run regenerates it, and removing it reverts to the\n"
    "# sibling default in pyproject.toml, which will then fail to resolve here.\n"
    "[sources]\n"
    f'mesh-link = {{ path = "{mesh_link}" }}\n'
  )


# ---------------------------------------------------------------------------
# Renderers — template substitution on shipped .example files
# ---------------------------------------------------------------------------

_EXPECTED_COLLECTOR_PLACEHOLDERS = {
  "YOURUSERNAME",
  "YOURGROUP",
  "/path/to/mesh-collector",
}

_EXPECTED_WWW_PLACEHOLDERS = {
  "YOURUSERNAME",
  "/path/to/RxOnly",
}

_EXPECTED_NGINX_PLACEHOLDERS = {
  "rxonly.your-domain.com",
  "/path/to/RxOnly",
}

# Gunicorn config: no placeholders — copies verbatim.
_EXPECTED_GUNICORN_PLACEHOLDERS: set[str] = set()

# Lines in the collector unit that activate transmit.
_TX_RUNTIME_DIRECTORY_COMMENT = "# RuntimeDirectory=mesh-collector"
_TX_RUNTIME_DIR_MODE_COMMENT = "# RuntimeDirectoryMode=0700"
_TX_ENV_TX_COMMENT = "# Environment=MESH_COLLECTOR_ENABLE_TX=true"
_TX_ENV_SOCKET_COMMENT = "# Environment=MESH_COLLECTOR_CONTROL_SOCKET_PATH=/run/mesh-collector/control.sock"


def _check_placeholders(template: str, expected: set[str], name: str) -> None:
  """Assert every expected placeholder is present in template. Runtime guard."""
  missing = {p for p in expected if p not in template}
  if missing:
    raise ValueError(
      f"{name}: expected placeholders {missing} not found in template. "
      "This copy of mesh_setup.py is out of step with the checkout\u2019s "
      "templates \u2014 run scripts/mesh_setup.py from the clone."
    )


def _any_placeholder_survives(text: str) -> bool:
  """Check if any known placeholder pattern survives in text."""
  placeholders = [
    "YOURUSERNAME", "YOURGROUP", "rxonly.your-domain.com",
  ]
  for ph in placeholders:
    if ph in text:
      return True
  # Check for /path/to/ patterns
  if re.search(r"/path/to/", text):
    return True
  return False


def _substitute(template: str, replacements: dict[str, str]) -> str:
  """Replace all occurrences of each placeholder."""
  result = template
  for placeholder, value in replacements.items():
    result = result.replace(placeholder, value)
  return result


def render_collector_unit(template: str, answers: dict) -> str:
  """Fill in the collector systemd unit template."""
  _check_placeholders(template, _EXPECTED_COLLECTOR_PLACEHOLDERS,
                        "mesh-collector.service")

  collector_path = answers.get("_layout", {}).get("collector", "")
  collector_path = str(collector_path)

  replacements = {
    "YOURUSERNAME": answers.get("system_user", "root"),
    "YOURGROUP": answers.get("system_group", "root"),
    "/path/to/mesh-collector": collector_path,
  }

  result = _substitute(template, replacements)

  # Transmit activation: uncomment RuntimeDirectory lines if transmit is on.
  if _bool_answer(answers, "enable_tx"):
    result = result.replace(_TX_RUNTIME_DIRECTORY_COMMENT, "RuntimeDirectory=mesh-collector")
    result = result.replace(_TX_RUNTIME_DIR_MODE_COMMENT, "RuntimeDirectoryMode=0700")
    # The Environment= lines stay commented (config.json is sole authority).
  else:
    # Ensure they remain commented (template is already commented, but be idempotent)
    pass

  if _any_placeholder_survives(result):
    raise ValueError(
      "collector unit: placeholders survived substitution. "
      "Template may have changed since mesh_setup.py was written."
    )

  return result


def render_www_unit(template: str, answers: dict) -> str:
  """Fill in the RxOnly gunicorn systemd unit template."""
  _check_placeholders(template, _EXPECTED_WWW_PLACEHOLDERS,
                        "rxonly-www.service")

  rxonly_path = answers.get("_layout", {}).get("rxonly", "")
  rxonly_path = str(rxonly_path)

  replacements = {
    "YOURUSERNAME": answers.get("system_user", "www-data"),
    "/path/to/RxOnly": rxonly_path,
  }

  result = _substitute(template, replacements)

  if _any_placeholder_survives(result):
    raise ValueError(
      "rxonly-www unit: placeholders survived substitution. "
      "Template may have changed since mesh_setup.py was written."
    )

  return result


def render_nginx(template: str, answers: dict) -> str:
  """Fill in the nginx server block template."""
  _check_placeholders(template, _EXPECTED_NGINX_PLACEHOLDERS,
                        "nginx.conf")

  rxonly_path = answers.get("_layout", {}).get("rxonly", "")
  rxonly_path = str(rxonly_path)
  domain = answers.get("domain_name", "rxonly.your-domain.com")
  cloudflare = _bool_answer(answers, "behind_cloudflare")

  replacements = {
    "rxonly.your-domain.com": domain,
    "/path/to/RxOnly": rxonly_path,
  }

  result = _substitute(template, replacements)

  # Cloudflare: uncomment the include line if enabled.
  if cloudflare:
    result = result.replace("#include cloudflare;", "include cloudflare;")

  if _any_placeholder_survives(result):
    raise ValueError(
      "nginx.conf: placeholders survived substitution. "
      "Template may have changed since mesh_setup.py was written."
    )

  return result


def render_gunicorn_conf(template: str, _answers: dict) -> str:
  """Copy gunicorn config verbatim — no placeholders in this template."""
  _check_placeholders(template, _EXPECTED_GUNICORN_PLACEHOLDERS,
                        "gunicorn.conf.py")
  return template


TEMPLATE_EXPECTATIONS: dict[str, tuple[set[str], Callable[[str, dict], str]]] = {
  "mesh-collector.service": (_EXPECTED_COLLECTOR_PLACEHOLDERS, render_collector_unit),
  "rxonly-www.service": (_EXPECTED_WWW_PLACEHOLDERS, render_www_unit),
  "rxonly.nginx.conf": (_EXPECTED_NGINX_PLACEHOLDERS, render_nginx),
  "gunicorn.conf.py": (_EXPECTED_GUNICORN_PLACEHOLDERS, render_gunicorn_conf),
}


def render_guide_readme(answers: dict, install_paths: dict | None = None) -> str:
  """Generate the deploy-guide README mentioning only what this install chose."""
  paths = install_paths or guess_install_paths()
  selections = answers.get("_selections", set())
  has_systemd = paths.get("has_systemd", False)
  has_rxonly = "rxonly" in selections
  has_console = "console" in selections
  has_transmit = "mesh-link" in selections
  cloudflare = _bool_answer(answers, "behind_cloudflare")

  lines: list[str] = []

  lines.append("# MeshSuite Deploy Guide\n")
  lines.append("This directory was generated by mesh_setup.py. "
                 "Everything here is generated \u2014 re-run the script rather than editing.\n")

  lines.append("## What\u2019s in this directory\n")

  unit_dir = paths.get("systemd_dir", "/etc/systemd/system") if has_systemd else "(no systemd)"
  nginx_avail = paths.get("nginx_available", "/etc/nginx/sites-available")
  nginx_target = paths.get("nginx_enabled", "/etc/nginx/sites-enabled")
  lines.append(f"- `mesh-collector.service` \u2192 {unit_dir}/")

  if has_rxonly:
    lines.append(f"- `rxonly-www.service` \u2192 {unit_dir}/")

    nginx_target = paths.get("nginx_enabled", "/etc/nginx/sites-enabled")
    nginx_avail = paths.get("nginx_available", "/etc/nginx/sites-available")
    lines.append(f"- `rxonly.nginx.conf` \u2192 {nginx_avail}/, "
                     f"then symlink into {nginx_target}/")
    # gunicorn conf is optional
    lines.append(f"- `gunicorn.conf.py` \u2192 {unit_dir}/ (optional \u2014 "
                     f"the unit works without it)")

  lines.append("")

  # Install-path review reminder — unconditional.
  lines.append("**These are best guesses \u2014 review every path and every file "
                 "before you install anything.**\n")

  if not has_systemd:
    lines.append(paths.get("note", ""))
    lines.append("")

  # Order of operations
  lines.append("## Order of operations\n")
  lines.append("1. **Serial-permission check** \u2014 the unit\u2019s user must be "
                 "able to open the serial port. Run:")
  lines.append("")
  lines.append("       id -nG <user>")
  lines.append("")
  lines.append("   The output must list `dialout`. If it does not:")
  lines.append("")
  lines.append("       sudo usermod -aG dialout <user>")
  lines.append("")
  lines.append("   Then log out and back in.\n")

  lines.append("2. **Install the units**:")
  lines.append("")
  lines.append(f"       sudo cp mesh-collector.service {unit_dir}/")
  if has_rxonly:
    lines.append(f"       sudo cp rxonly-www.service {unit_dir}/")
  lines.append(f"       sudo cp rxonly.nginx.conf {nginx_avail}/")
  lines.append(f"       sudo ln -s {nginx_avail}/rxonly.nginx.conf {nginx_target}/")
  lines.append("")

  lines.append("3. **Reload systemd and enable the collector**:")
  lines.append("")
  lines.append("       sudo systemctl daemon-reload")
  lines.append("       sudo systemctl enable --now mesh-collector")
  lines.append("")
  lines.append("   **The collector must run once before either reader starts** "
                 "\u2014 the readers open the archive read-only, and the file does "
                 "not exist until the collector creates it.\n")

  if has_rxonly or has_console:
    lines.append("4. **Start the reader**:")
    if has_rxonly:
      lines.append("       sudo systemctl enable --now rxonly-www")
    if has_console:
      pass  # console is interactive, no systemd unit
    lines.append("")

  if has_rxonly:
    lines.append("5. **Reload nginx**:")
    domain = answers.get("domain_name", "your-domain.com")
    lines.append("")
    lines.append("       sudo nginx -t")
    lines.append("       sudo systemctl reload nginx")
    lines.append("")
    lines.append("   **TLS ordering trap:** The server block references "
                     f"`/etc/letsencrypt/live/{domain}/` which does not exist until "
                     "certbot runs. Either deploy HTTP first, or run:")
    lines.append("")
    lines.append(f"       sudo certbot --nginx -d {domain}")
    lines.append("")
    lines.append("   then verify the cert paths match, reload nginx, and test.\n")

  if has_console:
    lines.append("### Journal group note\n")
    lines.append("Reading `journalctl -u` on a system unit requires membership in "
                     "`systemd-journal` or `adm`. If the console shows no log entries, "
                     "this is likely why:\n")
    lines.append("       sudo usermod -aG systemd-journal <user>\n")

  # Warnings
  lines.append("## Warnings\n")
  lines.append("### Socket permissions\n")
  lines.append("The control socket\u2019s file permissions are the entire authorization "
                 "model. Whoever can write to the socket can transmit on your radio. "
                 "The socket is created mode 0600.\n")
  lines.append("### Logs contain secrets\n")
  lines.append("A log captured with `TIDY_LOGS=false` contains the radio\u2019s "
                 "private key and channel keys. Treat any log as a secret.\n")

  if cloudflare:
    lines.append("## Cloudflare\n")
    lines.append("Two scripts in `rxonly/deploy/` help keep Cloudflare working:")
    lines.append("- `cloudflare-sync-ips.sh` \u2014 syncs Cloudflare\u2019s IP ranges "
                     "into your nginx allow list")
    lines.append("- `cloudflare-dyndns.sh` \u2014 updates a DNS record if your "
                     "server IP changes")
    lines.append("Review and run them from the checkout root.\n")

  # Undo
  lines.append("## Undo\n")
  lines.append("To remove everything this guide had you install:")
  lines.append("")
  lines.append("       sudo systemctl disable --now mesh-collector")
  if has_rxonly:
    lines.append("       sudo systemctl disable --now rxonly-www")
  lines.append("       sudo rm /etc/systemd/system/mesh-collector.service")
  if has_rxonly:
    lines.append("       sudo rm /etc/systemd/system/rxonly-www.service")
    lines.append(f"       sudo rm {nginx_avail}/rxonly.nginx.conf")
    lines.append(f"       sudo rm {nginx_target}/rxonly.nginx.conf")
  lines.append("       sudo systemctl daemon-reload")
  lines.append("")

  return "\n".join(lines)


# ---------------------------------------------------------------------------
# I/O helpers (Layer 3 infrastructure, not the full shell)
# ---------------------------------------------------------------------------

def _find_collector_checkout() -> Path | None:
  """Detect if we're running from inside a collector checkout.

  Checks if mesh_collector/config.py exists adjacent to scripts/.
  """
  self_path = Path(__file__).resolve()
  if self_path.parent.name == "scripts":
    candidate = self_path.parent.parent
    if (candidate / "mesh_collector" / "config.py").exists():
      return candidate
  return None


def _prerequisite_check() -> None:
  """Check git and uv are on PATH. Exits with instructions if not."""
  missing = []
  for cmd in ("git", "uv"):
    if not shutil.which(cmd):
      missing.append(cmd)

  if missing:
    print("Missing required tools:", ", ".join(missing))
    print()
    for cmd in missing:
      if cmd == "git":
        print("  Install git: https://git-scm.com/downloads")
        print("  Or:  apt install git   # Debian/Ubuntu")
        print("       brew install git  # macOS")
      elif cmd == "uv":
        print("  Install uv: https://docs.astral.sh/uv/#installation")
        print("  Or:  curl -LsSf https://astral.sh/uv/install.sh | sh")
    sys.exit(1)


def _git_clone(url: str, dest: Path) -> None:
  """Clone a repo. Verifies existing dir matches expected remote."""
  if dest.exists():
    try:
      result = subprocess.run(
        ["git", "-C", str(dest), "remote", "get-url", "origin"],
        capture_output=True, text=True, timeout=30,
      )
      if result.returncode == 0 and result.stdout.strip() == url:
        return  # already the right repo
    except (subprocess.TimeoutExpired, FileNotFoundError):
      pass
    print(f"Warning: {dest} exists and is not the expected repo.")
    sys.exit(1)

  print(f"Cloning {url} into {dest}...")
  try:
    subprocess.run(
      ["git", "clone", url, str(dest)],
      check=True, timeout=300,
    )
  except subprocess.CalledProcessError:
    print(f"Error: git clone failed for {url}.")
    print(f"  Destination: {dest}")
    print("  Check your internet connection and try again.")
    sys.exit(1)
  except subprocess.TimeoutExpired:
    print(f"Error: git clone timed out for {url}.")
    print(f"  Destination: {dest}")
    print("  Try again with a stable connection, or increase the timeout.")
    sys.exit(1)


def _uv_sync(project_path: Path, extra: str | None = None) -> None:
  """Run uv sync inside a project, optionally with an extra."""
  cmd = ["uv", "sync"]
  if extra:
    cmd.append(f"--extra={extra}")
  label = f"uv sync {'--extra=' + extra if extra else ''} in {project_path.name}"
  print(f"  {label}...")
  try:
    subprocess.run(cmd, cwd=str(project_path), check=True, timeout=1200)
  except subprocess.CalledProcessError:
    print(f"Error: {label} failed.")
    print(f"  Directory: {project_path}")
    print("  Check the output above for details, then retry.")
    sys.exit(1)
  except subprocess.TimeoutExpired:
    print(f"Error: {label} timed out.")
    print(f"  Directory: {project_path}")
    print("  Try again with a faster connection, or increase the timeout.")
    sys.exit(1)


def _write_with_backup(path: Path, content: str, backup_dir: Path | None = None) -> bool:
  """Write a file, backing up any existing version. Returns True if wrote new."""
  if path.exists():
    if backup_dir:
      backup_dir.mkdir(parents=True, exist_ok=True)
      stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
      # Use the last two path components (e.g. "mesh_collector/config.json")
      # to distinguish same-named configs from different projects.
      relative = path.relative_to(path.anchor) if path.is_absolute() else path
      parts = relative.parts
      suffix = "_".join(parts[-3:]) if len(parts) >= 3 else "_".join(parts[-2:])
      backup_path = backup_dir / f"{suffix}.{stamp}.bak"
      shutil.copy2(path, backup_path)
      print(f" Backed up {path} -> {backup_path}")

  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_text(content)
  return True


def _find_serial_ports() -> list[str]:
  """Glob for serial ports on common paths. Returns list of candidates."""
  candidates = []
  patterns = ["/dev/serial/by-id/*", "/dev/ttyACM*", "/dev/ttyUSB*", "/dev/cu.usbmodem*"]
  for pattern in patterns:
    candidates.extend(str(p) for p in Path("/").glob(pattern.lstrip("/")))
  return sorted(set(candidates))


def _prompt_with_default(prompt: str, default: Any) -> str:
  """Prompt user with a default in brackets. Returns raw input."""
  if default is not None and str(default).strip():
    p = f"{prompt} [{default}]: "
  else:
    p = f"{prompt}: "
  try:
    return input(p).strip()
  except (EOFError, KeyboardInterrupt):
    print()
    sys.exit(1)


_JSON_TO_ANSWER_KEY: dict[str, str] = {
  "SERIAL_PORT": "serial_port",
  "PRIMARY_CHANNEL": "primary_channel",
  "LOG_CHANNEL_IDS": "additional_channels",
  "STORE_DIRECT_MESSAGES": "archive_dms",
  "ENABLE_TX": "enable_tx",
  "CONTROL_SOCKET_PATH": "control_socket_path",
  "SERVE_DIRECT_MESSAGES": "serve_dms",
  "SHOW_DIRECT_MESSAGES": "console_show_dms",
  "ENABLE_SEND": "enable_send",
}

_JSON_TO_RETENTION_KEY: dict[str, str] = {
  "MAX_MESSAGES": "max_messages",
  "MAX_DIRECT_MESSAGES": "max_direct_messages",
  "NODE_PRUNE_DAYS": "node_prune_days",
}


def _load_existing_configs(layout: dict[str, Path],
                           selections: set[str]) -> dict[str, Any]:
  """Read existing config.json files to pre-populate interview defaults.

  Returns a flat dict of key->value. Unknown keys are skipped — they'll be
  preserved by merge_config at write time, but aren't question defaults.
  """
  pkg_paths = {
    "collector": "mesh_collector",
    "rxonly": "rxonly",
    "console": "mesh_console",
  }
  result: dict[str, Any] = {}
  retention_map: dict[str, int] = {}

  for project in selections:
    subdir = pkg_paths.get(project)
    if not subdir:
      continue
    cfg_path = layout.get(project, Path()) / subdir / "config.json"
    if not cfg_path.exists():
      continue
    try:
      data = json.loads(cfg_path.read_text())
      for k, v in data.items():
        if k in _JSON_TO_ANSWER_KEY:
          result[_JSON_TO_ANSWER_KEY[k]] = v
        elif k in _JSON_TO_RETENTION_KEY and isinstance(v, int):
          retention_map[_JSON_TO_RETENTION_KEY[k]] = v
    except (json.JSONDecodeError, OSError):
      pass

  if retention_map:
    result["_retention_map"] = retention_map

  return result


def _ask_serial_port(answers: dict, selections: set[str],
                     deploying: bool) -> str:
  """Serial port question with pick-list from attached devices."""
  candidates = _find_serial_ports()
  port = answers.get("serial_port", "/dev/ttyACM0")

  if candidates:
    print(" Detected serial ports:")
    for i, c in enumerate(candidates, 1):
      marker = " (current)" if c == port else ""
      print(f"   {i}. {c}{marker}")
    print(" Enter number, or type a custom path:")
    raw = input(f"Serial port [{port}]: ").strip()
    if not raw:
      return port
    try:
      idx = int(raw) - 1
      if 0 <= idx < len(candidates):
        return candidates[idx]
    except ValueError:
      pass
    return raw
  else:
    return _prompt_with_default(
      "Serial port for the Meshtastic device", port) or port


def _ask_retention(answers: dict) -> dict[str, int]:
  """Retention: one prompt showing three defaults, enter to accept all."""
  existing = answers.get("_retention_map", {})
  cur_max = existing.get("max_messages", 1000)
  cur_dm = existing.get("max_direct_messages", 1000)
  cur_prune = existing.get("node_prune_days", 14)

  print(f" Retention defaults \u2014 max messages: {cur_max}"
          f" / max direct messages: {cur_dm}"
          f" / node prune days: {cur_prune}")
  raw = input("Press enter to accept all, or type 'custom': ").strip().lower()

  if not raw or raw == "accept":
    return {"max_messages": cur_max, "max_direct_messages": cur_dm,
                "node_prune_days": cur_prune}

  mm = int(input(" Max messages: ").strip() or str(cur_max))
  md = int(input(" Max direct messages: ").strip() or str(cur_dm))
  np = int(input(" Node prune days: ").strip() or str(cur_prune))
  return {"max_messages": mm, "max_direct_messages": md, "node_prune_days": np}


def _run_interview(answers: dict, selections: set[str],
                   deploying: bool, collector_checkout: Path | None = None) -> dict:
  """Run the question table, return populated answers dict.

  Pre-populated with answers (from existing config, layout, etc.).
  Second-run behavior: existing config.json values become prompt defaults.
  Returns a flat dict of all answers plus computed values (_collector_path,
  _retention_map, etc.).
  """
  layout = answers.get("_layout", {})

  # Load existing configs for defaults
  existing_config = _load_existing_configs(layout, selections)
  for k, v in existing_config.items():
    answers.setdefault(k, v)

  _asked_warned_transmit = False

  for key, prompt_text, default, cast_fn, validate_fn, asked_when in QUESTIONS:
    if not asked_when(answers, selections, deploying):
      continue

    # Resolve dynamic defaults: existing config takes priority, then
    # the question's static default, then a runtime default.
    effective_default = default
    if key in existing_config:
      effective_default = existing_config[key]
    elif key == "parent_dir" and not default:
      effective_default = str(Path.cwd())
    elif key == "system_user" and not default:
      import getpass
      effective_default = getpass.getuser()
    elif key == "system_group" and not default:
      effective_default = _guess_primary_group()

    # Special question handlers
    if key == "serial_port":
      port = _ask_serial_port(answers, selections, deploying)
      answers["serial_port"] = port
      if collector_checkout and layout.get("collector"):
        probe_raw = _prompt_with_default(
          "Run serial probe? Opens device, reads node info, closes. (y/n)", "n")
        if probe_raw.lower() in ("y", "yes", "true", "1"):
          _run_probe(str(layout["collector"]), port)
      continue

    if key == "retention":
      answers["_retention_map"] = _ask_retention(answers)
      continue

    if key == "additional_channels":
      # Show the existing value if any
      existing_val = answers.get("additional_channels", "")
      if isinstance(existing_val, list):
        existing_str = ",".join(str(i) for i in existing_val)
      else:
        existing_str = str(existing_val) if existing_val else ""
      raw = _prompt_with_default(prompt_text, existing_str or "none")
      parsed = _cast_int_list(raw) if raw.strip() else []
      answers["additional_channels"] = parsed
      continue

    if key == "enable_tx" or key == "enable_send":
      if not _asked_warned_transmit:
        print("\n--- Transmit Warning ---")
        print(" The socket's file permissions are the entire authorization model.")
        print(" Whoever can write to the socket can transmit on your radio.")
        print(" The socket is created mode 0600, owned by the service user.")
        print("---\n")
        _asked_warned_transmit = True
      existing_val = existing_config.get(key, False)
      display = "y" if existing_val else "n"
      raw = _prompt_with_default(prompt_text, display)
      answers[key] = raw.lower() in ("y", "yes", "true", "1")
      continue

    # Generic prompt loop with retry on validation failure
    while True:
      raw = _prompt_with_default(prompt_text, effective_default)
      if not raw:
        val = cast_fn(str(effective_default)) if cast_fn else effective_default
      else:
        try:
          val = cast_fn(raw) if cast_fn else raw
        except (ValueError, TypeError) as e:
          print(f"  Invalid input: {e}")
          continue
      if validate_fn:
        err = validate_fn(val)
        if err:
          print(f"  {err}")
          continue
      answers[key] = val
      break

  # Compute DB_PATH for all three projects
  collector_path = layout.get("collector")
  if collector_path:
    answers["_collector_path"] = str(collector_path)

  return answers


def _guess_primary_group() -> str:
  """Try to determine the current user's primary group name."""
  try:
    import grp
    import pwd
    uid = os.getuid()
    user = pwd.getpwuid(uid)
    return grp.getgrgid(user.pw_gid).gr_name
  except (ImportError, KeyError, AttributeError):
    return "root"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

_COMPONENT_LABELS = {
  "rxonly": "Public web dashboard (RxOnly)",
  "console": "Terminal console (Mesh Console)",
  "mesh-link": "Transmit support (Mesh Link)",
}


def _pick_components(collector_found: bool, collector_path: str | None = None) -> set[str]:
  """Interactive component selection. Collector is toggled when one already exists."""
  options_list = ["rxonly", "console", "mesh-link"]
  toggled: set[str] = set()

  if collector_found and collector_path:
    collector_selected = True  # default: clone fresh
  else:
    collector_selected = False  # no existing collector to reuse

  print("\nSelect additional components:")
  while True:
    if collector_found and collector_path:
      mark = "x" if collector_selected else " "
      print(f"  0. Collector (clone fresh) [{mark}]")
      print(f"     (uncheck to use existing at {collector_path})")
    for i, comp in enumerate(options_list, 1):
      mark = "x" if comp in toggled else " "
      label = _COMPONENT_LABELS.get(comp, comp)
      print(f"  {i}. {label} [{mark}]")
    raw = input("Enter numbers to toggle (e.g. 1,3), or press enter when done: ").strip()
    if not raw:
      break
    for part in raw.split(","):
      part = part.strip()
      try:
        idx = int(part) - 1
        if idx == -1 and collector_found and collector_path:
          # 0 toggles collector
          collector_selected = not collector_selected
        elif 0 <= idx < len(options_list):
          comp = options_list[idx]
          if comp in toggled:
            toggled.remove(comp)
          else:
            toggled.add(comp)
      except ValueError:
        pass
    # +1 accounts for the input prompt's own line, which the cursor moves
    # past once enter is pressed -- without it the first printed line
    # (furthest from the cursor) never gets cleared.
    lines_printed = len(options_list) + (2 if collector_found and collector_path else 0)
    sys.stdout.write(f"\033[{lines_printed + 1}A\r\033[J")
    sys.stdout.flush()

  selections = {"collector"} if collector_selected else set()
  selections |= toggled
  return selections


def main() -> None:
  collector_checkout = _find_collector_checkout()
  script_dir = Path(__file__).resolve().parent
  if collector_checkout:
    parent_default = str(collector_checkout.parent)
    print(f"Found existing collector at {collector_checkout}")
  else:
    parent_default = str(script_dir)
    print("No existing collector found")

  _prerequisite_check()

  # --- 1. Component selection ---
  selections: set[str] = _pick_components(bool(collector_checkout), str(collector_checkout) if collector_checkout else None)

  collector_reused = "collector" not in selections
  if collector_checkout and collector_reused:
    print(f"\nUsing existing collector at {collector_checkout}")
  elif "collector" in selections:
    print(f"\nCollector will be cloned fresh into install directory")
  else:
    print(f"\nSelected: {', '.join(sorted(selections))}" if selections else "\nSelected: none")

  has_transmit = "mesh-link" in selections

  default_dir = parent_default
  if collector_checkout:
    print(f"\nCollector will stay at {collector_checkout}")
    print(f"Other repos will install as siblings under {default_dir}")
  else:
    print(f"\nInstall directory: {default_dir}")
  custom_dir_raw = _prompt_with_default(
    "Use a different directory? (y/n)", "n")
  if custom_dir_raw.lower() in ("y", "yes", "true", "1"):
    raw_dir = _prompt_with_default(
      "Install directory for all selected tools", default_dir) or default_dir
    install_dir = str(Path(raw_dir).expanduser().resolve())
    if "collector" in selections:
      collector_reused = False  # user chose a different directory, clone fresh
  else:
    install_dir = default_dir

  # --- 3. Custom paths per component ---
  custom_locations: dict[str, str] = {}
  custom_raw = _prompt_with_default(
    "Custom install location for any selected project? (y/n)", "n")
  if custom_raw.lower() in ("y", "yes", "true", "1"):
    for project in sorted(selections):
      loc = input(f"  Path for {project} (enter for sibling default): ").strip()
      if loc:
        custom_locations[project] = loc

  # Resolve layout
  layout = layout_plan(install_dir, custom_locations, selections)

  if collector_checkout and "collector" not in selections:
    layout["collector"] = collector_checkout.resolve()
    collides = [p for p in layout if p != "collector" and layout[p] == layout["collector"]]
    if collides:
      print(f"Error: collector checkout at {layout['collector']} collides with "
                  f"{collides[0]} install path. Choose a different install directory.")
      sys.exit(1)

  answers: dict = {
    "parent_dir": install_dir,
    "_layout": layout,
    "_selections": selections,
    "_collector_path": str(layout["collector"]),
  }

  print()
  for proj, path in sorted(layout.items()):
    print(f"  {proj}: {path}")
  if has_transmit:
    print("  extras: tx (collector), send (console)")

  # --- Clone ---
  for project in selections:
    url = REPO_URLS[project]
    dest = layout[project]
    if dest.exists():
      print(f"  {project}: already exists at {dest}")
    else:
      _git_clone(url, dest)

  # --- uv.toml overrides ---
  mesh_link_path = layout.get("mesh-link")
  for project in selections:
    if project not in CONSUMES_MESH_LINK or not mesh_link_path:
      continue
    project_path = layout[project]
    toml = render_uv_toml(str(mesh_link_path), str(project_path))
    if toml:
      # Write uv.toml and append to .gitignore (narrow tracked-file edit)
      toml_path = project_path / "uv.toml"
      toml_path.write_text(toml)
      print(f"  Wrote {toml_path}")

      gitignore = project_path / ".gitignore"
      marker = "# mesh_setup.py: uv.toml override"
      if gitignore.exists():
        existing = gitignore.read_text()
        if marker not in existing:
          with gitignore.open("a", encoding="utf-8") as f:
            f.write(f"\n{marker}\nuv.toml\n")
          print(f"  Appended uv.toml to {gitignore}")
      else:
        gitignore.write_text(f"{marker}\nuv.toml\n")
        print(f"  Created {gitignore} with uv.toml entry")

  # --- Sync ---
  for project in selections:
    if project == "mesh-link":
      continue
    project_path = layout[project]
    extra = EXTRAS.get(project) if has_transmit else None
    _uv_sync(project_path, extra)

  # Deploy persistent?
  deploy_raw = _prompt_with_default(
    "Deploy persistently with systemd/nginx? (y/n)", "n")
  deploying = deploy_raw.lower() in ("y", "yes", "true", "1")

  # --- Interview ---
  answers["_deploying"] = deploying
  answers = _run_interview(answers, selections, deploying, collector_checkout)

  # --- Write configs ---
  backup_dir = script_dir / "mesh-setup-backups"
  stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
  this_backup = backup_dir / stamp

  db_path = _absolute_db_path(answers)

  configs: dict[str, tuple[str, dict]] = {}

  # collector
  collector_cfg = collector_config(answers)
  collector_pkg = layout["collector"] / "mesh_collector"
  configs["collector"] = (str(collector_pkg / "config.json"), collector_cfg)

  # rxonly
  if "rxonly" in selections:
    rxonly_cfg = rxonly_config(answers, db_path)
    rxonly_pkg = layout["rxonly"] / "rxonly"
    configs["rxonly"] = (str(rxonly_pkg / "config.json"), rxonly_cfg)

  # console
  if "console" in selections:
    console_cfg = console_config(answers, db_path)
    console_pkg = layout["console"] / "mesh_console"
    configs["console"] = (str(console_pkg / "config.json"), console_cfg)

  # Merge with existing and write
  written_files: list[str] = []
  for name, (cfg_path_str, cfg) in configs.items():
    cfg_path = Path(cfg_path_str)
    existing = {}
    if cfg_path.exists():
      try:
        existing = json.loads(cfg_path.read_text())
      except (json.JSONDecodeError, OSError):
        existing = {}
    merged = merge_config(existing, cfg)
    content = json.dumps(merged, indent=2, sort_keys=True) + "\n"
    _write_with_backup(cfg_path, content, this_backup)
    written_files.append(str(cfg_path))

  deploy_guide_path = None
  # --- Deploy guide ---
  if deploying:
    # When the collector was freshly cloned, place the deploy-guide inside
    # the new checkout rather than beside the original script location.
    deploy_base = layout["collector"] / "scripts" if not collector_reused else script_dir
    deploy_dir = deploy_base / "deploy-guide"
    if deploy_dir.exists():
      # Backup existing deploy-guide wholesale
      this_backup.mkdir(parents=True, exist_ok=True)
      deploy_bak = this_backup / "deploy-guide"
      shutil.copytree(deploy_dir, deploy_bak)
      shutil.rmtree(deploy_dir)
      print(f" Moved existing deploy-guide/ -> {deploy_bak}")

    deploy_dir.mkdir(parents=True, exist_ok=True)

    # Render deploy artifacts
    unit_path = layout["collector"] / "deploy" / "mesh-collector.service.example"
    if unit_path.exists():
      template = unit_path.read_text()
      rendered = render_collector_unit(template, answers)
      (deploy_dir / "mesh-collector.service").write_text(rendered)
      written_files.append(str(deploy_dir / "mesh-collector.service"))

    if "rxonly" in selections:
      rxonly_deploy = layout["rxonly"] / "deploy"
      www_unit = rxonly_deploy / "rxonly-www.service.example"
      if www_unit.exists():
        rendered = render_www_unit(www_unit.read_text(), answers)
        (deploy_dir / "rxonly-www.service").write_text(rendered)
        written_files.append(str(deploy_dir / "rxonly-www.service"))

      nginx_tpl = rxonly_deploy / "nginx.conf.example"
      if nginx_tpl.exists():
        rendered = render_nginx(nginx_tpl.read_text(), answers)
        (deploy_dir / "rxonly.nginx.conf").write_text(rendered)
        written_files.append(str(deploy_dir / "rxonly.nginx.conf"))

      gunicorn_tpl = rxonly_deploy / "gunicorn.conf.py.example"
      if gunicorn_tpl.exists():
        rendered = render_gunicorn_conf(gunicorn_tpl.read_text(), answers)
        (deploy_dir / "gunicorn.conf.py").write_text(rendered)
        written_files.append(str(deploy_dir / "gunicorn.conf.py"))

    # Write guide README
    guide = render_guide_readme(answers)
    (deploy_dir / "README.md").write_text(guide)
    written_files.append(str(deploy_dir / "README.md"))

    deploy_guide_path = deploy_dir.resolve()
    print(f" Deploy guide: {deploy_guide_path}/")

  # --- Summary ---
  print("\n--- Setup Complete ---")
  for path in written_files:
    if "config.json" in path:
      p = Path(path)
      try:
        data = json.loads(p.read_text())
        print(f" {path}")
        for k, v in sorted(data.items()):
          print(f"   {k}: {v!r}")
      except (json.JSONDecodeError, OSError):
        print(f" {path}")

  if this_backup.exists():
    print(f" Backups: {this_backup}/")

  if deploying:
    print(f"\nNext: review files in {deploy_guide_path}/README.md before installing.")
  else:
    collector_venv = layout["collector"] / ".venv"
    if collector_venv.exists():
      print(f"\nNext: run the collector:")
      print(f"  cd {layout['collector']}")
      print(f"  uv run scripts/run_collector.py")
    else:
      print(f"\nNext: sync and run the collector:")
      print(f"  cd {layout['collector']}")
      print(f"  uv sync")
      print(f"  uv run scripts/run_collector.py")

  if not collector_reused:
    print(f"\nNote: future runs should use the cloned checkout:")
    print(f"  cd {layout['collector']}")
    print(f"  python3 scripts/mesh_setup.py")


def _run_probe(collector_path: str, port: str) -> None:
  """Run opt-in probe through collector's venv.

  Opens the device, reads node info, closes. Sends nothing over LoRa.
  """
  venv_python = Path(collector_path) / ".venv" / "bin" / "python3"
  if not venv_python.exists():
    print("  Probe: no venv at", collector_path, "/.venv, skipping")
    return

  code = textwrap.dedent(f"""\
    import sys
    try:
        from meshtastic.serial_interface import SerialInterface
        iface = SerialInterface(devPath={port!r})
        long_name = iface.getLongName() or "unknown"
        short_name = iface.getShortName() or "unknown"
        my_info = iface.getMyNodeInfo() or {{}}
        fw = my_info.get("firmwareVersion", "unknown")
        iface.close()
        print(f"OK  Node: {{long_name}} ({{short_name}}), firmware {{fw}}")
    except Exception as e:
        msg = str(e)
        if "could not open port" in msg.lower():
            print(f"FAIL Port {{port!r}} not available \\u2014 "
                  "a running collector may own it (check systemctl status mesh-collector)")
        elif "No module named" in msg:
            print(f"FAIL Probe needs meshtastic (from uv sync --extra tx), but it is not installed.")
        else:
            print(f"FAIL {{msg}}")
    """)

  print("  Running serial probe...")
  result = subprocess.run(
    [str(venv_python), "-c", code],
    capture_output=True, text=True, timeout=30,
  )
  out = result.stdout.strip()
  if out:
    print("  " + out.replace(chr(10), "\n  "))
  err = result.stderr.strip()
  if err:
    print("  (stderr: " + err[:200] + ")")


if __name__ == "__main__":
  main()
