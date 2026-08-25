# Example scripts and configuration files for deploying Mesh Collector

<!-- TODO(seam): short intro in the shape of deploy/README.md:3-7. Only the collector
     service lives here now; the nginx, gunicorn and Cloudflare examples moved to
     RxOnly. -->


## [mesh-collector.service](./mesh-collector.service.example)

### Meshtastic Collector Service

This systemd unit runs the RxOnly Meshtastic collector as a persistent
background service. The collector connects to a Meshtastic node — over a local
serial interface, over TCP to a `meshtasticd` daemon, or over BLE, whichever
`CONNECTION_MODE` names — and stores received messages and selected channel
data in a SQLite database.

The unit below is written for the serial default. TCP and BLE each want one
extra line in it; both are noted in the unit's own comments.

The service is designed to run continuously and is automatically restarted
by systemd if the collector exits or crashes.

<!-- TODO(seam): the paragraph above calls this "the RxOnly Meshtastic collector". -->

## Reading the collector's log from Mesh Console

The console's log viewer ("View raw logs", under `ctrl+p`) runs a shell command
and streams it. Its default is this unit:

```
journalctl -u mesh-collector -f -n 200 -o short-iso-precise --no-hostname
```

which needs nothing configured if the console runs **on the Pi**. Three notes if
it does not, or if the viewer comes up empty.

**Reading another user's journal takes a group.** `journalctl -u` on a system
unit is readable by root and by members of `systemd-journal` or `adm`. On
Raspberry Pi OS the first user is in `adm` already; a user you created later may
not be. `id -nG | tr ' ' '\n' | grep -E 'adm|systemd-journal'` answers it. The
viewer does not fail silently — it prints what the shell said and the exit
status into the stream — but "no entries" from a permissions problem reads a lot
like a quiet collector.

**Running the console somewhere else means saying so.** `LOG_COMMAND` is handed
to a shell on whatever machine the console runs on, so a console on a laptop
wants `MESH_CONSOLE_LOG_COMMAND="ssh pi@raspberrypi journalctl -u mesh-collector
-f -n 200 -o short-iso-precise --no-hostname"`. Nothing else about the console
changes; the archive is read over its own path.

**`-o short-iso-precise` earns its place**, and dropping it costs one thing
only. That format stamps each line with an explicit UTC offset, which is what
lets the viewer restate the time in the reader's own timezone — the same thing
it does to every other timestamp on screen. journald's default `short` format
prints local time already and carries no offset, so the viewer leaves those
lines exactly as they came. Levels and level filtering read the `[LEVELNAME]`
marker, which is the collector's own and is there either way.

### What the log actually contains

**The unit does not set `MESH_COLLECTOR_DEBUG`, so a deployed collector logs at
INFO** — startup, the published policy, channel and node updates, warnings. The
per-packet protobuf traffic, and everything the tidy filter does to it, is DEBUG
and will not appear. Add `Environment=MESH_COLLECTOR_DEBUG=true` to see it, and
expect roughly a hundred lines a minute on a busy mesh.

**At DEBUG the radio hands over its own keys.** The config handshake includes
`config { security { private_key } }`, and each channel's `psk` arrives with it.
mesh-collector redacts both — they print as `<redacted 32 bytes>` — but that
redaction is part of `TIDY_LOGS`, which is on by default and which
`MESH_COLLECTOR_TIDY_LOGS=false` turns off. **A log captured with tidy logs off
contains the private key of the radio and the keys to its channels.** Treat one
as a secret: do not paste it into an issue, and do not keep it where the archive
is served from.

**Nothing here audits a captured log for fields that arrived neither decoded nor
redacted.** A one-off script did once and is not in this repo; there is no
`verification/` directory, and this paragraph told you to run one for longer than
it should have. What decides whether a field is redacted is `_BYTE_FIELDS` in
`mesh_collector/logfmt.py`, and that table's comment is where the guarantee — and
what is not guaranteed — is written down. A session captured after a firmware or
library update is still the one worth reading, because that is when a field the
table has never heard of shows up.
