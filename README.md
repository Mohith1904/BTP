# LiFi ↔ WiFi Seamless Failover Protocol

Custom dual-interface communication protocol that transmits data over **LiFi (Ethernet)** as the primary channel and seamlessly fails over to **WiFi** when the light link breaks — then switches back when it recovers.

## Architecture

```
Sender (Laptop A)                          Receiver (Laptop B)
┌─────────────────┐                        ┌─────────────────┐
│  shared/ folder  │                        │  received/ folder│
│       ↓          │                        │       ↑          │
│  Chunk Manager   │                        │  Reassembler     │
│       ↓          │                        │       ↑          │
│  Protocol Layer  │──── LiFi (Ethernet) ──→│  Protocol Layer  │
│  (UDP packets)   │                        │  (UDP packets)   │
│       ↓          │                        │       ↑          │
│  Heartbeat Mon.  │─ ─ WiFi (backup) ─ ─ →│  Dashboard :8080 │
└─────────────────┘                        └─────────────────┘
```

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 1b. Install FFmpeg Essentials for timestamp-based video streaming
The stream index uses `ffprobe.exe` from FFmpeg. On Windows, install the essentials build:
```powershell
winget install "FFmpeg (Essentials Build)"
```
Then open a new terminal and verify:
```powershell
ffprobe -version
```

### 2. Configure IPs
Edit `config.py` with your actual network IPs:
```python
SENDER_LIFI_IP   = "192.168.1.1"    # Sender Ethernet IP
SENDER_WIFI_IP   = "192.168.0.101"  # Sender WiFi IP
RECEIVER_LIFI_IP = "192.168.1.2"    # Receiver Ethernet IP
RECEIVER_WIFI_IP = "192.168.0.102"  # Receiver WiFi IP
```

### 3. Place files to share
Put files in the `shared/` folder on the sender laptop.

### 4. Run
**On Sender laptop:**
```bash
python sender.py
```

**On Receiver laptop:**
```bash
python receiver.py
```

### 5. Open Dashboard
On the receiver, open: **http://localhost:8080**

## How It Works

1. **Normal operation**: Data flows over LiFi (Ethernet), heartbeats sent every 100ms
2. **Light blocked**: 3 missed heartbeats (~300ms) → sender sends `SWITCH_NOTIFY` over WiFi
3. **Failover**: Receiver ACKs, data resumes over WiFi from the last acknowledged chunk
4. **Light restored**: Heartbeats resume → sender sends `SWITCH_BACK`, data returns to LiFi

## Protocol Packet Types

| Type | Description |
|------|-------------|
| `DATA` | File chunk (up to 60KB) |
| `ACK` | Acknowledge received chunk |
| `HEARTBEAT` / `HEARTBEAT_ACK` | LiFi link monitoring |
| `SWITCH_NOTIFY` / `SWITCH_ACK` | Failover to WiFi |
| `SWITCH_BACK` / `SWITCH_BACK_ACK` | Return to LiFi |
| `FILE_META` | File metadata before transfer |
| `FILE_LIST_REQUEST` / `FILE_LIST_RESPONSE` | Browse shared files |
| `FILE_REQUEST` | Request a specific file |
| `TRANSFER_COMPLETE` | All chunks delivered |

## Key Features

- **Seamless failover** — video buffers briefly during switch (~300ms)
- **Automatic switch-back** — returns to LiFi when light recovers
- **Windowed sending** — 16 chunks in flight for maximum throughput
- **File integrity** — SHA-256 verification on complete transfers
- **Video streaming** — HTTP Range requests for progressive playback
- **Live dashboard** — real-time stats via Server-Sent Events
