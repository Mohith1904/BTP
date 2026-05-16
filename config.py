"""
Configuration for LiFi-WiFi Failover Protocol.
Edit the IP addresses below to match your network setup.
"""

import os

# ============================================================
#  NETWORK CONFIGURATION — EDIT THESE FOR YOUR SETUP
# ============================================================

# Sender (Laptop A) IP addresses
SENDER_LIFI_IP = "169.254.99.107"   # Ethernet IP (LiFi interface)
SENDER_WIFI_IP = "192.168.137.1"    # WiFi IP (Windows Mobile Hotspot host)

# Receiver (Laptop B) IP addresses
RECEIVER_LIFI_IP = "169.254.68.254" # Ethernet 2 IP (LiFi interface)
RECEIVER_WIFI_IP = "192.168.137.42" # WiFi IP (connected to sender's hotspot)

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

# Create folders if they don't exist
os.makedirs(SHARED_FOLDER, exist_ok=True)
os.makedirs(RECEIVE_FOLDER, exist_ok=True)
