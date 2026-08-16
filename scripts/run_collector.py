#!/usr/bin/env python3
"""
Entry point for the mesh-collector Meshtastic collector.

This script exists for:
  - systemd service execution
  - local development convenience

All real logic lives in mesh_collector/collector/__init__.py
"""

import sys
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from mesh_collector.collector import main

if __name__ == "__main__":
  main()
