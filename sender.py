"""
Sender — serves files from a shared folder over LiFi with WiFi failover.

Usage:
    python sender.py
"""

import os
import sys
import json
import time
import random
import logging
import threading

import config
from protocol.packet import Packet, PType
from protocol.chunk_manager import FileChunker
from network.manager import NetworkManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sender")


class Sender:
    def __init__(self):
        self.net = NetworkManager(
            my_lifi_ip=config.SENDER_LIFI_IP,
            my_wifi_ip=config.SENDER_WIFI_IP,
            peer_lifi_ip=config.RECEIVER_LIFI_IP,
            peer_wifi_ip=config.RECEIVER_WIFI_IP,
            data_port=config.DATA_PORT,
            control_port=config.CONTROL_PORT,
        )

        # ── transfer state ──────────────────────────────────
        self.active_transfers: dict[int, dict] = {}   # session_id -> state
        self.acked_chunks: dict[int, set] = {}         # session_id -> set of acked chunk_ids
        self._seq = 0

        # ── heartbeat state ─────────────────────────────────
        self.hb_seq = 0
        self.last_hb_ack_time = time.time()
        self.missed_hb = 0
        self.lifi_alive = True
        self.lifi_was_down = False

        # ── stats (for dashboard) ───────────────────────────
        self.stats = {
            "active_interface": "lifi",
            "bytes_sent": 0,
            "chunks_sent": 0,
            "failover_count": 0,
            "start_time": time.time(),
            "transfers": {},
        }

        self._register_handlers()

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    # ── handler registration ────────────────────────────────
    def _register_handlers(self):
        # Data port: receive ACKs
        self.net.on_data(PType.ACK, self._on_ack)

        # Control port: heartbeat ACKs, file requests, switch signals
        self.net.on_ctrl(PType.HEARTBEAT_ACK, self._on_heartbeat_ack)
        self.net.on_ctrl(PType.FILE_LIST_REQUEST, self._on_file_list_request)
        self.net.on_ctrl(PType.FILE_REQUEST, self._on_file_request)
        self.net.on_ctrl(PType.SWITCH_ACK, self._on_switch_ack)
        self.net.on_ctrl(PType.SWITCH_BACK_ACK, self._on_switchback_ack)

    # ── handlers ────────────────────────────────────────────
    def _on_ack(self, pkt: Packet, addr, iface):
        sid = pkt.session_id
        cid = pkt.chunk_id
        if sid in self.acked_chunks:
            self.acked_chunks[sid].add(cid)

    def _on_heartbeat_ack(self, pkt: Packet, addr, iface):
        self.last_hb_ack_time = time.time()
        self.missed_hb = 0
        if not self.lifi_alive:
            log.info("LiFi heartbeat restored!")
            self.lifi_alive = True

    def _on_file_list_request(self, pkt: Packet, addr, iface):
        log.info("File list requested from %s via %s", addr, iface)
        files = self._scan_shared_folder()
        # Safety: if file list is too large for one UDP packet (~60KB limit),
        # truncate and warn.  Full browsing can be added later with pagination.
        payload = Packet.make_json_payload({"files": files})
        while len(payload) > 60000 and len(files) > 1:
            files = files[: len(files) // 2]
            payload = Packet.make_json_payload({"files": files, "truncated": True})
            log.warning("File list too large, truncated to %d files", len(files))
        resp = Packet(PType.FILE_LIST_RESPONSE, seq_num=self._next_seq(), payload=payload)
        self.net.send_ctrl(resp, interface=iface)

    def _on_file_request(self, pkt: Packet, addr, iface):
        info = pkt.json_payload()
        filename = info.get("filename", "")
        filepath = os.path.join(config.SHARED_FOLDER, filename)

        if not os.path.isfile(filepath):
            log.error("Requested file not found: %s", filename)
            return

        log.info("File requested: %s (via %s)", filename, iface)
        session_id = random.randint(1, 0xFFFFFFFF)
        t = threading.Thread(target=self._send_file, args=(filepath, session_id), daemon=True)
        t.start()

    def _on_switch_ack(self, pkt: Packet, addr, iface):
        log.info("Receiver acknowledged switch to WiFi")

    def _on_switchback_ack(self, pkt: Packet, addr, iface):
        log.info("Receiver acknowledged switch back to LiFi")

    # ── shared folder scanning ──────────────────────────────
    def _scan_shared_folder(self) -> list[dict]:
        files = []
        for root, dirs, filenames in os.walk(config.SHARED_FOLDER):
            for fname in filenames:
                fpath = os.path.join(root, fname)
                rel = os.path.relpath(fpath, config.SHARED_FOLDER).replace("\\", "/")
                files.append({
                    "name": rel,
                    "size": os.path.getsize(fpath),
                    "ext": os.path.splitext(fname)[1].lower(),
                })
        return files

    # ── file transfer ───────────────────────────────────────
    def _send_file(self, filepath: str, session_id: int):
        chunker = FileChunker(filepath, config.CHUNK_SIZE)
        meta = chunker.metadata()
        total = chunker.total_chunks

        self.acked_chunks[session_id] = set()
        self.active_transfers[session_id] = {
            "filename": meta["filename"],
            "total_chunks": total,
            "started": time.time(),
        }
        self.stats["transfers"][session_id] = {
            "filename": meta["filename"],
            "progress": 0.0,
            "interface": self.net.active_interface,
        }

        # 1) Send FILE_META
        meta_pkt = Packet(
            PType.FILE_META,
            seq_num=self._next_seq(),
            session_id=session_id,
            total_chunks=total,
            payload=Packet.make_json_payload(meta),
        )
        self.net.send_ctrl(meta_pkt)
        # Also send via wifi so receiver has metadata on both
        self.net.send_ctrl(meta_pkt, interface="wifi")
        time.sleep(0.05)

        # 2) Windowed send
        log.info("Sending %s (%d chunks, %d bytes)", meta["filename"], total, meta["file_size"])
        next_chunk = 0
        unacked: dict[int, float] = {}   # chunk_id -> last_send_time
        retries: dict[int, int] = {}     # chunk_id -> retry_count

        while len(self.acked_chunks[session_id]) < total:
            if not self.net.running:
                return

            # Fill send window
            while (len(unacked) < config.WINDOW_SIZE
                   and next_chunk < total):
                if next_chunk not in self.acked_chunks[session_id]:
                    data = chunker.get_chunk(next_chunk)
                    pkt = Packet(
                        PType.DATA,
                        seq_num=self._next_seq(),
                        chunk_id=next_chunk,
                        total_chunks=total,
                        session_id=session_id,
                        payload=data,
                    )
                    self.net.send_data(pkt)
                    unacked[next_chunk] = time.time()
                    retries.setdefault(next_chunk, 0)
                    self.stats["bytes_sent"] += len(data)
                    self.stats["chunks_sent"] += 1
                next_chunk += 1

            # Process acked chunks
            for cid in list(unacked):
                if cid in self.acked_chunks[session_id]:
                    del unacked[cid]

            # Retransmit timed-out chunks
            now = time.time()
            for cid, send_time in list(unacked.items()):
                if now - send_time > config.ACK_TIMEOUT:
                    retries[cid] = retries.get(cid, 0) + 1
                    if retries[cid] > config.MAX_RETRIES:
                        log.error("Chunk %d exceeded max retries", cid)
                        unacked.pop(cid, None)
                        continue
                    data = chunker.get_chunk(cid)
                    pkt = Packet(
                        PType.DATA,
                        seq_num=self._next_seq(),
                        chunk_id=cid,
                        total_chunks=total,
                        session_id=session_id,
                        payload=data,
                    )
                    self.net.send_data(pkt)
                    unacked[cid] = now
                    log.debug("Retransmit chunk %d (attempt %d)", cid, retries[cid])

            # Update stats
            acked_count = len(self.acked_chunks[session_id])
            self.stats["transfers"][session_id]["progress"] = acked_count / total
            self.stats["transfers"][session_id]["interface"] = self.net.active_interface

            time.sleep(0.001)  # yield

        # 3) All done
        done = Packet(PType.TRANSFER_COMPLETE, seq_num=self._next_seq(), session_id=session_id)
        self.net.send_ctrl(done)
        self.net.send_ctrl(done, interface="wifi")  # send on both to be sure
        elapsed = time.time() - self.active_transfers[session_id]["started"]
        log.info("Transfer complete: %s in %.1fs", meta["filename"], elapsed)
        del self.active_transfers[session_id]

    # ── heartbeat ───────────────────────────────────────────
    def _heartbeat_loop(self):
        while self.net.running:
            self.hb_seq += 1
            pkt = Packet(PType.HEARTBEAT, seq_num=self.hb_seq)
            self.net.send_ctrl(pkt, interface="lifi")

            time.sleep(config.HEARTBEAT_INTERVAL)

            # Check for missed heartbeats
            elapsed = time.time() - self.last_hb_ack_time
            if elapsed > config.HEARTBEAT_INTERVAL * config.MAX_MISSED_HEARTBEATS:
                if self.lifi_alive:
                    self.missed_hb += 1
                    if self.missed_hb >= config.MAX_MISSED_HEARTBEATS:
                        self.lifi_alive = False
                        self.lifi_was_down = True
                        log.warning("LiFi DOWN — %d heartbeats missed", self.missed_hb)
                        self._initiate_failover()
            else:
                if self.lifi_was_down and self.lifi_alive:
                    self._initiate_switchback()
                    self.lifi_was_down = False

    def _initiate_failover(self):
        """Switch data transfer to WiFi."""
        log.warning(">>> FAILOVER: LiFi → WiFi <<<")
        self.stats["failover_count"] += 1
        self.stats["active_interface"] = "wifi"

        # Notify receiver to switch
        payload = Packet.make_json_payload({"interface": "wifi"})
        notify = Packet(PType.SWITCH_NOTIFY, seq_num=self._next_seq(), payload=payload)
        self.net.send_ctrl(notify, interface="wifi")

        self.net.switch_to("wifi")

    def _initiate_switchback(self):
        """Switch data transfer back to LiFi."""
        log.info(">>> SWITCH BACK: WiFi → LiFi <<<")
        self.stats["active_interface"] = "lifi"

        payload = Packet.make_json_payload({"interface": "lifi"})
        notify = Packet(PType.SWITCH_BACK, seq_num=self._next_seq(), payload=payload)
        self.net.send_ctrl(notify, interface="wifi")  # Send over wifi since lifi just recovered

        self.net.switch_to("lifi")

    # ── startup validation ──────────────────────────────────
    @staticmethod
    def _validate_interfaces():
        """Check that we can see both network interfaces."""
        info = NetworkManager.get_interface_info()
        log.info("Detected network interfaces:")
        for name, detail in info.items():
            status = "UP" if detail["is_up"] else "DOWN"
            log.info("  %-20s  %s  [%s]  %s Mbps",
                     name, detail["ip"], status, detail["speed"])

        all_ips = {d["ip"] for d in info.values()}
        lifi_ok = config.SENDER_LIFI_IP in all_ips
        wifi_ok = config.SENDER_WIFI_IP in all_ips

        if not lifi_ok:
            log.warning("⚠ LiFi IP %s not found on any interface!", config.SENDER_LIFI_IP)
        if not wifi_ok:
            log.warning("⚠ WiFi IP %s not found on any interface!", config.SENDER_WIFI_IP)
        if lifi_ok and wifi_ok:
            log.info("✓ Both interfaces detected")

    # ── main ────────────────────────────────────────────────
    def start(self):
        log.info("=" * 50)
        log.info("  LiFi-WiFi SENDER starting")
        log.info("  Shared folder: %s", config.SHARED_FOLDER)
        log.info("  LiFi IP: %s  |  WiFi IP: %s", config.SENDER_LIFI_IP, config.SENDER_WIFI_IP)
        log.info("  Peer  → LiFi: %s  |  WiFi: %s", config.RECEIVER_LIFI_IP, config.RECEIVER_WIFI_IP)
        log.info("  Data port: %d  |  Control port: %d", config.DATA_PORT, config.CONTROL_PORT)
        log.info("=" * 50)

        self._validate_interfaces()

        self.net.start()
        self.last_hb_ack_time = time.time()

        hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb_thread.start()

        files = self._scan_shared_folder()
        log.info("Sharing %d files. Waiting for receiver...", len(files))

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("Shutting down...")
            self.net.stop()


if __name__ == "__main__":
    Sender().start()
