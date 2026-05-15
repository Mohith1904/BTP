"""
Configuration for LiFi-WiFi Failover Protocol.
Edit the IP addresses below to match your network setup.
"""

import os

# ============================================================
#  NETWORK CONFIGURATION — EDIT THESE FOR YOUR SETUP
# ============================================================

# Sender (Laptop A) IP addresses
SENDER_LIFI_IP = "169.254.123.186"   # Ethernet IP (LiFi interface)
SENDER_WIFI_IP = "192.168.137.1"    # WiFi IP (Windows Mobile Hotspot host)

# Receiver (Laptop B) IP addresses
RECEIVER_LIFI_IP = "169.254.159.224" # Ethernet 2 IP (LiFi interface)
RECEIVER_WIFI_IP = "192.168.137.116" # WiFi IP (connected to sender's hotspot)

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
STREAM_CACHE_FOLDER = os.path.join(RECEIVE_FOLDER, ".stream_cache")

# ============================================================
#  DASHBOARD
# ============================================================
DASHBOARD_PORT = 8080
DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "dashboard")

# ============================================================
#  VIDEO STREAMING
# ============================================================
# The receiver now proxies HLS playlists and media segments, matching the
# newer Hybrid WiFi/LiFi implementation. FFmpeg prepares the sender-side
# playlist once, then the receiver asks for only the segments the player needs.
FFMPEG_BIN = "ffmpeg"
HLS_TIME_SECONDS = 4
HLS_CACHE_FOLDER = os.path.join(os.path.dirname(__file__), ".hls_cache")
STREAM_WAIT_TIMEOUT = 120.0

# Legacy byte-range stream knobs are kept so old packet handlers fail gently if
# an older client sends those control messages. The dashboard uses HLS now.
STREAM_START_CHUNKS = 48
STREAM_MIN_START_CHUNKS = 8
STREAM_REQUEST_CHUNKS = 96
STREAM_LOW_WATER_CHUNKS = 32
STREAM_WINDOW_SECONDS = 20.0
STREAM_START_SECONDS = 12.0
FFPROBE_BIN = "ffprobe"

# Create folders if they don't exist
os.makedirs(SHARED_FOLDER, exist_ok=True)
os.makedirs(RECEIVE_FOLDER, exist_ok=True)
os.makedirs(STREAM_CACHE_FOLDER, exist_ok=True)
os.makedirs(HLS_CACHE_FOLDER, exist_ok=True)
