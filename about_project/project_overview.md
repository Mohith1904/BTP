# Hybrid LiFi ↔ WiFi Project Overview

## What is This Project?

A **custom dual-interface file transfer protocol** built on two laptops:

- **Laptop A (Sender)** — shares files from a `shared/` folder
- **Laptop B (Receiver)** — requests and receives files, runs a web dashboard

The physical interfaces are:
| Name in Code | Physical Interface | Use |
|---|---|---|
| **LiFi** | Ethernet cable (169.254.x.x range) | **Primary** high-speed channel |
| **WiFi** | Windows Mobile Hotspot (192.168.137.x) | **Backup/fallback** channel |

The whole point: LiFi is simulated via Ethernet here. When that link goes down (cable pulled, etc.), the system **automatically fails over to WiFi** — mid-transfer — and switches back when LiFi recovers.

---

## Architecture Overview

```
Sender (Laptop A)                          Receiver (Laptop B)
┌──────────────────┐                       ┌──────────────────────┐
│  shared/ folder  │                       │   received/ folder   │
│       ↓          │                       │         ↑            │
│  FileChunker     │                       │  ChunkReassembler    │
│  (chunk_manager) │                       │  (chunk_manager)     │
│       ↓          │                       │         ↑            │
│  sender.py       │──── LiFi PRIMARY ───→ │  receiver.py         │
│  (UDP port 5000) │─ ─ WiFi BACKUP ─ ─ →│  (UDP port 5000/5001)│
│  Heartbeat Loop  │                       │  Dashboard :8080     │
└──────────────────┘                       │  cli.py (terminal)   │
                                           └──────────────────────┘
```

---

## File-by-File Breakdown

### `config.py`
Central configuration. All tunable parameters:
- IP addresses for both sender/receiver on both interfaces
- Ports: `DATA_PORT=5000`, `CONTROL_PORT=5001`, `DASHBOARD_PORT=8080`
- Protocol tuning: chunk size (60KB), window size (16 in-flight), heartbeat interval (100ms), ACK timeout (1s), max retries (5)
- Folder paths: `shared/` (sender) and `received/` (receiver)

---

### `protocol/packet.py` — Binary Packet Format
Custom binary UDP protocol. Every packet has:
- **28-byte header**: magic bytes `LF`, version, packet type, sequence number, chunk ID, total chunks, session ID, payload length, flags
- **Payload**: 0 to 60KB of data
- **CRC32 checksum**: 4 bytes at the end for integrity

**Packet types defined:**
| Type | Purpose |
|---|---|
| `DATA (0x01)` | One 60KB file chunk |
| `ACK (0x02)` | Receiver confirms a chunk |
| `HEARTBEAT (0x03)` | Sender pings LiFi link every 100ms |
| `HEARTBEAT_ACK (0x04)` | Receiver replies to confirm LiFi is alive |
| `SWITCH_NOTIFY (0x05)` | Sender tells receiver: "switching to WiFi" |
| `SWITCH_ACK (0x06)` | Receiver confirms the switch |
| `SWITCH_BACK (0x07)` | Sender: "LiFi is back, switching back" |
| `SWITCH_BACK_ACK (0x08)` | Receiver confirms switch-back |
| `FILE_META (0x09)` | File metadata before transfer starts (name, size, hash, total chunks) |
| `TRANSFER_COMPLETE (0x0A)` | Sender signals all chunks sent |
| `FILE_LIST_REQUEST (0x0B)` | Receiver asks for list of shared files |
| `FILE_LIST_RESPONSE (0x0C)` | Sender responds with JSON file list |
| `FILE_REQUEST (0x0D)` | Receiver requests a specific file |

---

### `protocol/chunk_manager.py` — File Splitting & Reassembly

**`FileChunker` (Sender side):**
- Splits a file into numbered 60KB chunks
- Computes SHA-256 hash for the entire file
- `get_chunk(chunk_id)` → reads the correct offset from disk

**`ChunkReassembler` (Receiver side):**
- Pre-allocates the full file on disk immediately
- Receives chunks out of order (UDP doesn't guarantee ordering)
- Writes each chunk directly to the correct byte offset
- Tracks progress, contiguous bytes written (for streaming), and detects duplicates
- `verify()` → SHA-256 hash check at the end

---

### `network/manager.py` — Dual Interface Network Manager
- Manages **two UDP sockets** (data socket on port 5000, control socket on port 5001)
- Both sockets bind to `0.0.0.0` (listen on ALL interfaces)
- Knows both IPs for LiFi and WiFi for the peer
- `active_interface` tracks which interface is currently active (`"lifi"` or `"wifi"`)
- `switch_to(interface)` → atomically flips which IP to route data to
- Handler registration: `on_data(ptype, fn)` and `on_ctrl(ptype, fn)` → callbacks by packet type
- `send_data(packet)` and `send_ctrl(packet)` send to the active interface (or specified one)
- Uses `psutil` to inspect real OS network interfaces

---

### `sender.py` — The Sender (runs on Laptop A)

**Startup:**
1. Validates both interfaces are detected
2. Starts NetworkManager (2 receive threads)
3. Starts heartbeat loop thread
4. Waits for receiver file requests

**File Transfer Flow:**
1. Receiver requests a file (`FILE_REQUEST`)
2. Sender sends `FILE_META` on **both** interfaces (so receiver gets it regardless of which is active)
3. Windowed send: up to 16 chunks in-flight simultaneously (sliding window)
4. For each ACK received → removes chunk from in-flight window
5. If chunk times out (>1s) → retransmits (up to 5 retries, then continues retrying at capped rate)
6. When all chunks ACK'd → sends `TRANSFER_COMPLETE` on both interfaces
7. Cleans up session state

**Heartbeat/Failover Logic:**
- Sends `HEARTBEAT` on LiFi every 100ms
- If no `HEARTBEAT_ACK` for 300ms → LiFi is declared dead
- Triggers `_initiate_failover()`: sends `SWITCH_NOTIFY` over WiFi, waits up to 1s for `SWITCH_ACK`, then switches active interface to WiFi
- When LiFi recovers (heartbeats resume) → triggers `_initiate_switchback()`: sends `SWITCH_BACK` over WiFi, waits for `SWITCH_BACK_ACK`, switches back to LiFi
- Tracks stats: bytes sent, chunks sent, failover count

---

### `receiver.py` — The Receiver (runs on Laptop B)

**Startup:**
1. Validates interfaces
2. Starts NetworkManager
3. Starts heartbeat monitor thread
4. Starts HTTP dashboard server thread (port 8080)
5. Auto-requests file list from sender

**Receiving Logic:**
- `_on_data`: receives a chunk → ACKs it immediately → writes to disk via `ChunkReassembler`
- `_on_file_meta`: creates a new `ChunkReassembler` for the session
- `_on_transfer_complete`: verifies hash if all chunks received, or enters 2-second grace period for late UDP packets
- `_on_heartbeat`: replies with `HEARTBEAT_ACK` on LiFi
- `_on_switch_notify` / `_on_switch_back`: updates active interface, ACKs the switch

**Built-in HTTP Server (Dashboard at :8080):**
- `GET /api/status` → JSON of all stats, transfer progress, events
- `GET /api/files` → cached file list
- `GET /api/refresh_files` → triggers new `FILE_LIST_REQUEST` to sender, waits 5s
- `POST /api/download/<filename>` → triggers `FILE_REQUEST` to sender
- `GET /api/stream/<filename>` → serves file with **HTTP Range request support** (for video streaming). Supports streaming mid-transfer (only exposes contiguous bytes so video player doesn't read zero-filled preallocated space)
- `GET /api/events` → **Server-Sent Events (SSE)** stream, pushes live stats every 500ms

---

### `dashboard/` — Web Dashboard (Receiver's Browser UI)
- `index.html` — Layout with: header (interface pill, connection status, uptime), file browser, active transfers, stats, event log, video modal
- `style.css` — Dark themed, Inter font, card-based layout
- `app.js` — Polls/subscribes SSE, renders file list, download buttons, progress bars, video player modal

**Dashboard features:**
- Live interface status indicator (LiFi 🔵 / WiFi 🟠)
- File browser with download button for each file
- Real-time transfer progress bars
- Stats: total bytes received, chunk count, failover count, bandwidth
- Event log (failovers, transfer completions, etc.)
- In-browser video player (click to stream video files immediately, even mid-transfer)

---

### `cli.py` — Terminal CLI for the Receiver
A separate terminal tool that talks to the receiver's HTTP API:
- `list / ls` — fetch and print file list in a table
- `get <filename>` — request file download
- `status / st` — show active/completed transfers with progress bars
- `events` — show recent event log (last 20 events)
- `help / ?` — help text
- `exit / quit / q` — exit

---

## Key Technical Features

| Feature | Detail |
|---|---|
| **Protocol** | Custom binary UDP over two interfaces |
| **Failover detection** | Heartbeat-based, ~300ms detection time |
| **Automatic switch-back** | Returns to LiFi when it recovers |
| **Reliability** | Sliding window (16), ACK timeout, retransmit, CRC32 per packet |
| **Integrity** | SHA-256 of full file verified on completion |
| **Streaming** | HTTP Range requests, progressive playback during transfer |
| **Real-time UI** | SSE-based live dashboard |
| **Path safety** | Path traversal protection on both sender and receiver |
| **Session-based** | Random 32-bit session IDs to multiplex concurrent transfers |
| **Concurrent transfers** | Multiple files can be in-flight simultaneously |
| **Grace period** | 2s wait for late UDP packets after TRANSFER_COMPLETE |

---

## Current Limitations / Things Not Yet Built
- No bi-directional transfer (only Sender → Receiver)
- No upload from receiver back to sender
- No authentication or encryption
- File list pagination (truncates at ~60KB worth of JSON if too many files)
- No per-transfer bandwidth/speed display (stat field placeholder exists)
- Dashboard bandwidth stat shows "—" (not computed yet)
