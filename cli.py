"""
CLI — Terminal interface for the receiver.

Run this in a SEPARATE terminal while receiver.py is running.

Usage:
    python cli.py

Commands:
    list / ls           — List files available on the sender
    get <filename>      — Request download of a file
    stream <filename>   — Stream a video file (HLS)
    streams             — Show active streaming sessions
    close <session_id>  — Close a streaming session
    status / st         — Show active transfers and progress
    events              — Show recent event log
    help / ?            — Show this help
    exit / quit / q     — Exit the CLI
"""

import sys
import json
import time
import urllib.request
import urllib.error

import config

API_BASE = f"http://localhost:{config.DASHBOARD_PORT}"


# ── helpers ──────────────────────────────────────────────────

def _api_get(path: str) -> dict | None:
    """GET request to the receiver's local HTTP API."""
    try:
        req = urllib.request.Request(f"{API_BASE}{path}")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError:
        print("\n  ✗ Cannot reach receiver — is receiver.py running?\n")
        return None
    except Exception as e:
        print(f"\n  ✗ Error: {e}\n")
        return None


def _api_post(path: str) -> dict | None:
    """POST request to the receiver's local HTTP API."""
    try:
        req = urllib.request.Request(f"{API_BASE}{path}", method="POST", data=b"")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError:
        print("\n  ✗ Cannot reach receiver — is receiver.py running?\n")
        return None
    except Exception as e:
        print(f"\n  ✗ Error: {e}\n")
        return None


def _format_size(size_bytes: int) -> str:
    """Human-readable file size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def _progress_bar(fraction: float, width: int = 30) -> str:
    """Render a text progress bar."""
    filled = int(width * fraction)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {fraction * 100:5.1f}%"


# ── commands ─────────────────────────────────────────────────

def cmd_list():
    """Fetch and display the file list from the sender."""
    print("  Requesting file list from sender...")
    data = _api_get("/api/refresh_files")
    if data is None:
        return

    files = data.get("files", [])
    if not files:
        print("  No files found in sender's shared folder.\n")
        return

    # Table header
    print()
    print(f"  {'#':<4} {'Filename':<45} {'Size':>10}  {'Type'}")
    print(f"  {'─'*4} {'─'*45} {'─'*10}  {'─'*8}")

    for i, f in enumerate(files, 1):
        name = f.get("name", "?")
        size = _format_size(f.get("size", 0))
        ext = f.get("ext", "")
        print(f"  {i:<4} {name:<45} {size:>10}  {ext}")

    print(f"\n  {len(files)} file(s) available. Use 'get <filename>' to download.\n")


def cmd_download(filename: str):
    """Request the sender to transfer a file."""
    if not filename:
        print("  Usage: get <filename>\n")
        return

    print(f"  Requesting download: {filename}")
    result = _api_post(f"/api/download/{urllib.request.quote(filename, safe='')}")
    if result and result.get("status") == "ok":
        print(f"  ✓ Transfer initiated. Use 'status' to track progress.\n")
    elif result:
        print(f"  ✗ Unexpected response: {result}\n")


def cmd_status():
    """Show current transfer status and receiver stats."""
    data = _api_get("/api/status")
    if data is None:
        return

    iface = data.get("active_interface", "?").upper()
    bytes_rx = _format_size(data.get("bytes_received", 0))
    chunks_rx = data.get("chunks_received", 0)
    failovers = data.get("failover_count", 0)
    uptime = time.time() - data.get("start_time", time.time())
    uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))

    print()
    print(f"  Interface : {iface}  {'🔵' if iface == 'LIFI' else '🟠'}")
    print(f"  Received  : {bytes_rx}  ({chunks_rx} chunks)")
    print(f"  Failovers : {failovers}")
    print(f"  Uptime    : {uptime_str}")

    transfers = data.get("transfers", {})
    active = {k: v for k, v in transfers.items() if not v.get("completed")}
    completed = {k: v for k, v in transfers.items() if v.get("completed")}

    if active:
        print(f"\n  ── Active Transfers ──")
        for sid, t in active.items():
            name = t.get("filename", "?")
            progress = t.get("progress", 0)
            received = t.get("received", 0)
            total = t.get("total_chunks", "?")
            size = _format_size(t.get("file_size", 0))
            print(f"  {name}")
            print(f"    {_progress_bar(progress)}  {received}/{total} chunks  ({size})")

    if completed:
        print(f"\n  ── Completed Transfers ──")
        for sid, t in completed.items():
            name = t.get("filename", "?")
            size = _format_size(t.get("file_size", 0))
            progress = t.get("progress", 0)
            status = "✓" if progress >= 1.0 else f"⚠ {progress*100:.0f}%"
            print(f"  {status} {name}  ({size})")

    if not transfers:
        print("\n  No transfers yet.")

    print()


def cmd_events():
    """Show recent event log."""
    data = _api_get("/api/status")
    if data is None:
        return

    events = data.get("events", [])
    if not events:
        print("  No events yet.\n")
        return

    print()
    print(f"  {'Time':<10} Event")
    print(f"  {'─'*10} {'─'*50}")
    for ev in events[-20:]:  # show last 20
        print(f"  {ev.get('time', '?'):<10} {ev.get('msg', '?')}")
    print()


def cmd_help():
    print("""
  ┌───────────────────────────────────────────────────────┐
  │         LiFi-WiFi Receiver CLI                        │
  ├────────────────────┬────────────────────────────────────┤
  │  list, ls          │  List files on the sender          │
  │  get <file>        │  Download a file                   │
  │  stream <file>     │  Stream a video (HLS)              │
  │  streams           │  Show active stream sessions       │
  │  close <sid>       │  Close a streaming session         │
  │  status, st        │  Show transfer progress            │
  │  events            │  Show event log                    │
  │  help, ?           │  Show this help                    │
  │  exit, quit, q     │  Exit                              │
  └────────────────────┴────────────────────────────────────┘
""")


def cmd_stream(filename: str):
    """Start streaming a video file via HLS."""
    if not filename:
        print("  Usage: stream <filename>\n")
        return

    print(f"  Requesting stream: {filename}")
    print("  Waiting for sender to prepare HLS segments (may take a few seconds)...")
    result = _api_post(f"/api/stream_start/{urllib.request.quote(filename, safe='')}")
    if result is None:
        return

    if "session_id" in result:
        sid = result["session_id"]
        vlc_url = result.get("vlc_url", f"http://localhost:{config.DASHBOARD_PORT}/api/hls/{sid}/playlist.m3u8")
        duration = result.get("duration", 0)
        segs = result.get("segment_count", 0)
        res_w = result.get("width", 0)
        res_h = result.get("height", 0)

        print()
        print(f"  ✓ Stream ready!")
        print(f"  Session ID : {sid}")
        if res_w and res_h:
            print(f"  Resolution : {res_w}×{res_h}")
        if duration:
            mins = int(duration // 60)
            secs = int(duration % 60)
            print(f"  Duration   : {mins}m {secs}s")
        print(f"  Segments   : {segs}")
        print()
        print(f"  ┌─ Play in VLC ─────────────────────────────────────────┐")
        print(f"  │  vlc {vlc_url}")
        print(f"  │                                                       │")
        print(f"  │  Or open browser dashboard: http://localhost:{config.DASHBOARD_PORT}  │")
        print(f"  └───────────────────────────────────────────────────────┘")
        print(f"\n  Use 'close {sid}' to end this stream session.\n")
    else:
        print(f"  ✗ Stream failed: {result}\n")


def cmd_streams():
    """Show active streaming sessions."""
    data = _api_get("/api/status")
    if data is None:
        return

    sessions = data.get("stream_sessions", {})
    if not sessions:
        print("  No active streams.\n")
        return

    print()
    print(f"  {'SID':<12} {'Filename':<35} {'Duration':<12} {'Segments'}")
    print(f"  {'─'*12} {'─'*35} {'─'*12} {'─'*10}")
    for sid, s in sessions.items():
        dur = s.get('duration', 0)
        dur_str = f"{int(dur//60)}m {int(dur%60)}s" if dur else "—"
        print(f"  {sid:<12} {s.get('filename','?'):<35} {dur_str:<12} {s.get('segment_count','?')}")

    print(f"\n  {len(sessions)} active stream(s). Use 'close <session_id>' to end.\n")


def cmd_close_stream(session_id_str: str):
    """Close a streaming session."""
    if not session_id_str:
        print("  Usage: close <session_id>\n")
        return

    try:
        sid = int(session_id_str)
    except ValueError:
        print(f"  Invalid session ID: {session_id_str}\n")
        return

    result = _api_post(f"/api/stream_close/{sid}")
    if result and result.get("status") == "ok":
        print(f"  ✓ Stream session {sid} closed.\n")
    elif result:
        print(f"  ✗ Unexpected response: {result}\n")

# ── main loop ────────────────────────────────────────────────

def main():
    print()
    print("  ╔═══════════════════════════════════════════╗")
    print("  ║     LiFi ↔ WiFi  Receiver CLI             ║")
    print("  ╚═══════════════════════════════════════════╝")
    print()
    print("  Type 'help' for commands. Receiver must be running.\n")

    while True:
        try:
            raw = input("  lifi> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Bye!\n")
            break

        if not raw:
            continue

        parts = raw.split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""

        if cmd in ("list", "ls"):
            cmd_list()
        elif cmd in ("get", "download", "dl"):
            cmd_download(arg)
        elif cmd == "stream":
            cmd_stream(arg)
        elif cmd == "streams":
            cmd_streams()
        elif cmd == "close":
            cmd_close_stream(arg)
        elif cmd in ("status", "st"):
            cmd_status()
        elif cmd in ("events", "ev"):
            cmd_events()
        elif cmd in ("help", "?"):
            cmd_help()
        elif cmd in ("exit", "quit", "q"):
            print("  Bye!\n")
            break
        else:
            print(f"  Unknown command: {cmd}. Type 'help' for options.\n")


if __name__ == "__main__":
    main()
