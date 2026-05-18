"""
Configuration for LiFi-WiFi Failover Protocol.
Edit the IP addresses below to match your network setup.
"""

import os

# ============================================================
#  NETWORK CONFIGURATION — EDIT THESE FOR YOUR SETUP
# ============================================================

# Sender (Laptop A) IP addresses
SENDER_LIFI_IP = "169.254.157.236"   # Ethernet IP (LiFi interface)
SENDER_WIFI_IP = "192.168.137.1"    # WiFi IP (Windows Mobile Hotspot host)

# Receiver (Laptop B) IP addresses
RECEIVER_LIFI_IP = "169.254.159.224" # Ethernet 2 IP (LiFi interface)
RECEIVER_WIFI_IP = "192.168.137.67" # WiFi IP (connected to sender's hotspot)

# ============================================================
#  PORTS
# ============================================================
DATA_PORT = 5000          # DATA + ACK packets
CONTROL_PORT = 5001       # Heartbeat, switch signals, file control

# ============================================================
#  PROTOCOL TUNING
# ============================================================
CHUNK_SIZE = 60 * 1024          # 60 KB per chunk
WINDOW_SIZE = 16                # Send up to 16 chunks before waiting for ACKs
HEARTBEAT_INTERVAL = 0.1        # Send heartbeat every 100ms
MAX_MISSED_HEARTBEATS = 3       # 3 missed = link dead (~300ms detection)
ACK_TIMEOUT = 1.0               # Seconds to wait for ACK before retransmit
MAX_RETRIES = 5                 # Max retransmits per chunk
RECV_BUFFER = 65536             # UDP receive buffer size

# ============================================================
#  PATHS
# ============================================================
SHARED_FOLDER = os.path.join(os.path.dirname(__file__), "shared")
RECEIVE_FOLDER = os.path.join(os.path.dirname(__file__), "received")

# ============================================================
#  DASHBOARD
# ============================================================
DASHBOARD_PORT = 8080
DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "dashboard")

# ============================================================
#  HLS STREAMING — Configurable parameters
# ============================================================
# Each HLS segment is a time-based slice of the video.
# At 1080p ~5 Mbps, a 4-second segment ≈ 2.5 MB on disk.
# The segment is then transferred over UDP in CHUNK_SIZE (60 KB) pieces.

HLS_SEGMENT_DURATION = 4        # seconds per HLS segment (must be integer)
HLS_BUFFER_BEHIND = 7           # keep N segments behind current playback
HLS_BUFFER_AHEAD = 17           # pre-fetch N segments ahead of playback
HLS_PREFETCH_BATCH = 2          # max new segment requests to start per HLS request
HLS_MAX_PENDING_SEGMENTS = 4    # max stream segment transfers in flight
HLS_TRANSCODE_FALLBACK = True   # re-encode to browser-safe H.264/AAC if copy output is invalid
HLS_CACHE_DIR = os.path.join(os.path.dirname(__file__), ".hls_cache")
STREAM_PREPARE_TIMEOUT = 300    # max seconds to wait while sender prepares/transcodes HLS
STREAM_SEGMENT_TIMEOUT = 10     # max seconds to wait for a segment from sender
FFMPEG_PATH = "ffmpeg"          # full path if not in system PATH
FFPROBE_PATH = "ffprobe"        # full path if not in system PATH

# Supported video extensions for streaming.
# Only H.264 video + AAC audio in MP4/TS containers are guaranteed
# to work with HLS browser playback (hls.js).
# VLC supports almost all codecs natively.
# If ffmpeg fails with -codec copy, the file likely uses an
# incompatible codec (HEVC/VP9/AV1) — re-encode it manually:
#   ffmpeg -i input.mkv -c:v libx264 -crf 23 -c:a aac output.mp4
STREAM_VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".ts"}

# Create folders if they don't exist
os.makedirs(SHARED_FOLDER, exist_ok=True)
os.makedirs(RECEIVE_FOLDER, exist_ok=True)
os.makedirs(HLS_CACHE_DIR, exist_ok=True)
