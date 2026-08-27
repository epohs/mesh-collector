# Mesh Collector

<!-- TODO(seam): one-line bold premise, in the shape of README.md:3. The collector's
     version of that sentence — it logs what the node hears to SQLite. -->

<!-- TODO(seam): what-it-is paragraphs. README.md:6 described the whole suite
     ("RxOnly consists of two main parts...") and needs a collector-first rewrite that
     mentions RxOnly and Mesh Console as optional consumers of this database.
     README.md:8's first sentence (lightweight, low-dependency, security focused)
     applies here; the rest of that line was about the web layer and moved to RxOnly. -->

This project is built for personal use and experimentation, prioritizing clarity, safety, and ease of maintenance over features.

<!-- TODO(seam): the archiving half of the private-channel/DM warning at README.md:20.
     The publishing half stayed with RxOnly. This one is about choosing to store DMs
     at all. -->


## Installation & Getting Started

### Setup Script (recommended)

[`scripts/mesh_setup.py`](scripts/mesh_setup.py) automates the full setup: clones the four MeshSuite repos, runs `uv sync` with the right extras, walks through an interactive interview, writes each project's `config.json`, and optionally renders deploy artifacts (systemd units, nginx config, and a deploy guide) with best-guess install paths for your OS.

**One-line bootstrap** — download and run in an empty directory:

```
python3 <(curl -s https://raw.githubusercontent.com/epohs/mesh-collector/main/scripts/mesh_setup.py)
```

Or clone the collector and run from its checkout:

```
git clone https://github.com/epohs/mesh-collector.git
cd mesh-collector
python3 scripts/mesh_setup.py
```

**What it is not:** `mesh_setup.py` is a setup interview, not a service manager, updater, uninstaller, or health check. It never writes to `/etc`, never runs `systemctl`, and — with one narrow, documented exception (a single `.gitignore` line for the `uv.toml` override) — never edits a tracked file. It has no non-interactive mode; the interview is the product.

After setup, see the summary for the next action — either `uv run scripts/run_collector.py` for interactive use or `deploy-guide/README.md` for persistent deployment.

---

### Manual setup

This project uses [uv](https://docs.astral.sh/uv/) for Python dependency management and virtual environments.

#### Prerequisites
- Python 3.13 or newer
- `uv` installed globally
- A Meshtastic-compatible node (for live data collection)

### Clone the repository

```
git clone https://github.com/epohs/mesh-collector.git
cd mesh-collector
```

### Create the virtual environment


Each project in this suite uses its own virtual environment.

```
# Create environment
uv init
# Install dependencies
uv sync
```

### Customize your `mesh_collector/config.json` file

Copy the [`mesh_collector/config-sample.json`](/mesh_collector/config-sample.json) file and create a new `config.json` file. I think the values are fairly self-explanatory with one exception.

#### How the collector reaches your node

`CONNECTION_MODE` chooses one of three, and it is `serial` unless you say otherwise. Only the settings for the mode you pick are read, so the other two can stay at whatever the sample has in them.

#### Serial — a node on USB (default)

The setting `SERIAL_PORT` in the example uses the device name for maximum compatibility, but this could be unreliable. For more reliable connectivity to your Meshtastic device, first ensure that it is connected to the host computer, then run: `ls -l /dev/serial/by-id/`

You should see something like: `usb-RAKwireless_WisCore_RAK4631_Board_1X2X3X4X5X6X-if00 -> ../../ttyACM0`

Use that value in your own `config.json` using `“SERIAL_PORT”: “/dev/serial/by-id/YOUR_DEVICE_ID”,` instead of the `/dev/DEVICE_NAME` that I have in my example.

##### Local development on macOS

Everything above assumes the Raspberry Pi, which is where this actually runs. If you are developing on a Mac, `/dev/serial/by-id/` will not be there — it is a Linux `udev` feature, and macOS has no equivalent. Your device shows up directly in `/dev` instead, so run `ls /dev/cu.usbmodem*` and use what you find, something like `“SERIAL_PORT”: “/dev/cu.usbmodem101”,`.

Use the `cu.` name and not the `tty.` one beside it. They are the same device, but opening the `tty.` form blocks waiting for carrier detect, and the collector will simply hang at startup with no error to explain itself.

The port number is assigned when the device is plugged in and is not stable across reboots or a different USB port, so expect to update it occasionally. That is a good reason to keep macOS a development-only arrangement and leave deployment on the Pi, where `by-id` gives you a name that does not move.

#### TCP — a node running `meshtasticd`

Set `CONNECTION_MODE` to `tcp` and point `TCP_HOST` and `TCP_PORT` at the daemon; `localhost` and `4403` are its defaults, which is what you want when `meshtasticd` runs on the same machine as the collector. Nothing needs to be in `/dev` and no group membership is involved.

`meshtasticd` serves one client at a time, exactly as a serial port does, so the same rule applies: while this collector is attached, nothing else can be.

If both run on the same host under systemd, give the collector unit `After=meshtasticd.service`. Without it a reboot can start the collector against a daemon that has not bound its port yet, which costs a restart cycle every boot.

#### BLE — a node over Bluetooth

Set `CONNECTION_MODE` to `ble` and `BLE_ADDRESS` to the node's address or its advertised name. `meshtastic --ble-scan` lists what is in range. There is no default worth shipping, so this one has to be filled in — a blank `BLE_ADDRESS` stops the collector at startup rather than letting it attach to whichever node happened to answer first.

Pair the host with the node before the first run. The collector cannot answer a PIN prompt, and an unpaired node fails with the library's own message about the `bluetooth` group and the PIN — which on Linux is also the hint worth taking literally: the user running the collector needs to be in `bluetooth` and able to reach BlueZ over DBus.

**A node serves serial or BLE, not both.** While something holds the USB serial port, the node stops advertising over Bluetooth entirely — a scan finds nothing, and it reappears within seconds of the serial client letting go. No power cycle is needed. So BLE mode is for a node no cable is attached to, which is what it was always for; pointing a BLE collector at a node that a serial collector is already archiving from does not give you two readers, it gives you one reader and one scan that finds nothing.

#### When a BLE link drops

BLE drops and comes back — someone carries the node out of range, the firmware reboots, the host's Bluetooth cycles — and unlike the other two transports the library neither reports it nor recovers from it. `BLEInterface` has no reconnect at all, and a dropped link publishes no event, so a collector left to itself sits there alive and archiving nothing.

So on BLE the collector supervises the link itself, and it watches three ways at once. First and strongest is a disconnect callback of our own, put in place of the library's: the host's Bluetooth stack says the peripheral is gone and we hear it directly. That one is worth understanding, because it is not only the detector — the callback it displaces is what wedges the library on a dropped link, so installing ours is half the cure as well. On hardware it has caught every drop so far, within a millisecond of the radio going.

Behind it, once a second, is a plain read of whether the host still considers the peripheral connected. It asks nothing of the library and waits on nothing, so it answers even when the rest of the interface will not, and it is what still notices a drop if the callback ever fails to install against some future version of bleak. Third is the library's own read thread: if it has died, the link died under it. Whichever signal speaks first, the collector says so in the journal, throws the dead interface away and opens a new one. `BLE_RECONNECT_ATTEMPTS` (default 5) caps how many times, and `BLE_RECONNECT_BACKOFF` (default 5 seconds) is the first gap, doubling up to a minute. The first attempt is immediate.

Anything the collector cannot read, it treats as healthy. A library that has changed shape underneath us reads as a missing attribute, and a supervisor that tears down a working link because it could not find a field is worse than one that misses a drop the other two signals catch a moment later.

Running out of attempts exits nonzero, which is what serial and TCP do on the first drop — the retry loop is in front of that behaviour, not instead of it. A radio that is genuinely gone still becomes the service manager's problem rather than a process that looks healthy forever. Setting `BLE_RECONNECT_ATTEMPTS` to `0` gives BLE the same one-drop-one-exit policy the other transports have.

**Recovery is not continuity.** Packets that arrived while the link was down are gone; nothing replays LoRa. What recovery buys is that archiving resumes in seconds without a restart, and that the gap is one line in the journal instead of a silence you find days later.

#### Watching for a link that goes quiet

`LIVENESS_TIMEOUT` is off (`0`) by default and can stay that way for serial, where a disconnect is noticed immediately. It is there for TCP over a network that can disappear without closing the socket: after that many seconds of hearing nothing at all, the collector sends a heartbeat, and a failure exits so the service manager restarts it. The library eventually notices such a link on its own — this only makes it prompt.

It is not BLE's drop detector and does not need to be switched on for BLE — the supervision above is always on, costs nothing, and cannot mistake a quiet mesh for a dead link the way a silence timer can. On BLE a failed probe feeds the reconnect above rather than exiting.

#### When the radio's clock is wrong

Every timestamp in the archive is a Unix epoch integer, and the ones on arriving traffic come from `rxTime` — written by the receiving radio, off the radio's own clock. A Meshtastic node with no GPS and no client to set its clock has no way to know the time and no way to find out it is wrong, so that stamp is a claim rather than a fact, and it is worth checking against the one thing that can check it: a packet being handled right now.

`RX_TIME_TOLERANCE` (default `900`, in seconds) is how far the radio's stamp may sit from this machine's clock before the collector stops believing it and stamps the arrival with its own clock instead. The window is sized for delay, not for drift — MQTT relay and the collector's own queue cost real seconds, and a healthy radio here measured 15 of them, against a fault that measured 32,870. Setting it to `0` disables the check and trusts the radio unconditionally, which is what this collector did before the check existed.

A rejected stamp is reported, at WARNING, on transitions only: when the clock goes wrong, again if it changes how wrong it is, and once at INFO when it comes back. Every packet passes through the check, so a line per packet would bury the mesh traffic the log is for — and the recovery line is there because after fixing a clock the question is "did that work", which the log should answer without a query.

#### Setting the radio's clock

The check above keeps the archive honest, but it treats the symptom — the radio stays wrong until something sets it, and nothing in `meshtastic-python` does that on connect. So `SET_NODE_TIME` (default on) hands the radio this host's clock as the link opens, at startup and again on every BLE reopen. A firmware reboot is one of the things that drops a BLE link, and a radio that has just rebooted is exactly the one whose clock needs setting.

This is an admin message to the *local* node, so it is not a transmit and is not gated on `ENABLE_TX`: it travels the link the collector already owns, puts nothing on the air, and no other node can see it. `SET_NODE_TIME` exists anyway, because writing to the device at all is a change of posture for an archive-only collector and that should be a decision rather than a surprise.

**It will not set the clock from a clock it cannot vouch for.** This Pi has no hardware RTC — `timedatectl` reports `RTC time: n/a` — so between boot and the first NTP reply it does not know the time either, and pushing unconditionally would write a fabricated boot-time date into the one device with no way to argue, on exactly the reboot that most needs fixing. The collector asks the kernel via `ntp_adjtime` (a read-only query, no privileges, and it answers whichever NTP daemon is running, which the systemd-timesyncd marker file does not) and declines with a WARNING while the answer is no. If the question itself cannot be asked — a platform without the call — it proceeds and says so, since declining there would mean the feature silently never fires on hosts whose clock was fine all along.

A failure to set the clock is a log line, never an exit. Archiving with a wrong clock is worse than archiving with a right one and far better than not archiving, so the radio keeps whatever clock it had and the `rxTime` check above goes on covering it.

The check is on the packet path only. The initial sync replays the device's node cache, whose `lastHeard` values genuinely refer to the past; substituting the wall clock there would claim the radio just heard a node it last heard yesterday. Those are stored as the device reported them, and `nodes.last_seen` corrects itself on the next live packet.

All config options are documented in [`config.py`](/mesh_collector/config.py).


### Running the Collector script

The collector script connects to the Meshtastic node and stores received packets in SQLite.

```
source .venv/bin/activate
python scripts/run_collector.py
```

The database will be created and migrated automatically if needed. Nodes and messages will be written to the database as long as this script is running.

> [!WARNING]
> If you pull the repo and the database schema ([`schema.sql`](/mesh_collector/db/schema.sql)) version changes your database will be dropped and recreated, wiping all existing data.
>
> As of schema version 0.6.0 the collector refuses to start rather than do this. To allow the rebuild, set `ALLOW_DESTRUCTIVE_REBUILD` to true in your `config.json`. A timestamped backup of the database is written beside it before any table is dropped.


### Transmitting (optional, off by default)

The collector is the node's attached client, so it is also the only thing that can send — over serial and over TCP that is because the node accepts one client at a time, and over BLE because it is the client that is there. It does not do that unless you ask it to, and asking takes two separate steps — a collector that skips either one is receive-only, exactly as it was before this existed.

First install it with the transmit extra, which pulls in [Mesh Link](https://github.com/epohs/mesh-link):

```
uv sync --extra tx
```

Then set `ENABLE_TX` to true in your `config.json`. On startup the collector opens a Unix socket and listens on it; [Mesh Console](https://github.com/epohs/mesh-console) is what talks to it. Without the extra, `ENABLE_TX` has nothing to switch on and the collector says so and stops, rather than starting up quietly unable to send.

> [!WARNING]
> **The socket's file permissions are the entire authorization model.** Anyone who can write to it can transmit on your radio, under your node's identity. It is created mode `0600` and owned by the user the collector runs as, and there is no second check behind that — no password, no token. Do not loosen those permissions to make something else work.

`CONTROL_SOCKET_PATH` decides where it lives. Left empty it uses `$XDG_RUNTIME_DIR/mesh-collector/control.sock`, or the per-user temporary directory on a Mac. Under `systemd`, use `RuntimeDirectory=mesh-collector` and point it at `/run/mesh-collector/control.sock` — see [the unit example](/deploy/mesh-collector.service.example), which also explains why `PrivateTmp=true` and a socket in `/tmp` do not mix.

Messages the collector sends are written to the archive by the send path itself, because nothing else can see them: LoRa is half-duplex so the radio never hears itself, and the firmware drops the rebroadcast of a packet this node originated. They are stored with `from_node` set to your own node, which is how a reader tells them apart from everything received.


## One Real Use Case

<!-- TODO(seam): README.md:103-115 was one continuous narrative that ran straight through
     the boundary — Pi, USB cable, two systemd services, nginx, Let's Encrypt, Cloudflare.
     The collector half is below. README.md:107 ties the deploy directory to both projects
     and needs splitting; README.md:111's "two systemd services" is now one per project. -->

On my local network, this project runs on a [Raspberry Pi](https://www.raspberrypi.com/). My Meshtastic device is connected directly to the Pi via a USB-C cable. The Pi is responsible for both collecting data from the node and for serving the web dashboard.

<!-- TODO(seam): the last clause of the line above ("and for serving the web dashboard")
     is only true if RxOnly is also installed. -->


## Helpful commands

- `journalctl -u mesh-collector -f` View the logs output by the collector process (Assuming you have it running as a `systemd` daemon).

- `sudo systemctl restart mesh-collector` Restart the collector script.


Licensed under the GNU AGPL-3.0
Copyright (c) 2026 epohs
