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
from protocol.stream_manager import SenderStreamSession
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
        self._recent_file_request_ids: set[int] = set()
        self._seq = 0
        self._state_lock = threading.Lock()  # protects shared mutable state

        # ── streaming state ─────────────────────────────────
        self.stream_sessions: dict[int, SenderStreamSession] = {}
        self._stream_preparing: set[str] = set()  # filenames currently being prepared
        self._active_seg_transfers: set[tuple] = set()  # (stream_sid, seg_index) in flight
        self._recent_stream_transfer_ids: set[int] = set()

        # ── heartbeat state ─────────────────────────────────
        self.hb_seq = 0
        self.last_hb_ack_time = time.time()
        self.lifi_alive = True
        self.lifi_was_down = False

        # ── switch synchronisation ──────────────────────────
        self._switch_ack_event = threading.Event()
        self._switchback_ack_event = threading.Event()

        # ── stats (for dashboard) ───────────────────────────
        self.stats = {
            "active_interface": self.net.active_interface,
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

        # Streaming handlers
        self.net.on_ctrl(PType.STREAM_REQUEST, self._on_stream_request)
        self.net.on_ctrl(PType.STREAM_SEGMENT_REQUEST, self._on_stream_segment_request)
        self.net.on_ctrl(PType.STREAM_CLOSE, self._on_stream_close)

    # ── handlers ────────────────────────────────────────────
    def _on_ack(self, pkt: Packet, addr, iface):
        sid = pkt.session_id
        cid = pkt.chunk_id
        with self._state_lock:
            if sid in self.acked_chunks:
                self.acked_chunks[sid].add(cid)

    def _on_heartbeat_ack(self, pkt: Packet, addr, iface):
        self.last_hb_ack_time = time.time()
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
        request_id = info.get("request_id")
        if request_id is not None:
            with self._state_lock:
                if request_id in self._recent_file_request_ids:
                    log.debug("Duplicate file request ignored: %s (%s)", filename, request_id)
                    return
                self._recent_file_request_ids.add(request_id)
                if len(self._recent_file_request_ids) > 4096:
                    self._recent_file_request_ids.clear()
        filepath = os.path.join(config.SHARED_FOLDER, filename)

        # Path traversal protection
        root = os.path.abspath(config.SHARED_FOLDER)
        resolved = os.path.abspath(filepath)
        if not resolved.startswith(root + os.sep) and resolved != root:
            log.error("Path traversal rejected: %s", filename)
            return

        if not os.path.isfile(filepath):
            log.error("Requested file not found: %s", filename)
            return

        if iface == "wifi" and (not self.lifi_alive or not self.net.interface_available("lifi")):
            self.net.switch_to("wifi")
            self.stats["active_interface"] = "wifi"

        log.info("File requested: %s (via %s)", filename, iface)
        session_id = random.randint(1, 0xFFFFFFFF)
        t = threading.Thread(target=self._send_file, args=(filepath, session_id), daemon=True)
        t.start()

    def _on_switch_ack(self, pkt: Packet, addr, iface):
        log.info("Receiver acknowledged switch to WiFi")
        self._switch_ack_event.set()

    def _on_switchback_ack(self, pkt: Packet, addr, iface):
        log.info("Receiver acknowledged switch back to LiFi")
        self._switchback_ack_event.set()

    # ── streaming handlers ───────────────────────────────────
    def _on_stream_request(self, pkt: Packet, addr, iface):
        info = pkt.json_payload()
        filename = info.get("filename", "")
        filepath = os.path.join(config.SHARED_FOLDER, filename)

        # Path traversal protection
        root = os.path.abspath(config.SHARED_FOLDER)
        resolved = os.path.abspath(filepath)
        if not resolved.startswith(root + os.sep) and resolved != root:
            log.error("Stream: path traversal rejected: %s", filename)
            return

        if not os.path.isfile(filepath):
            log.error("Stream: file not found: %s", filename)
            return

        ext = os.path.splitext(filename)[1].lower()
        if ext not in config.STREAM_VIDEO_EXTENSIONS:
            log.error("Stream: unsupported extension %s for: %s", ext, filename)
            return

        # Deduplicate: if we're already preparing or streaming this file, skip
        if filename in self._stream_preparing:
            log.debug("Stream: duplicate request for %s ignored (already preparing)", filename)
            return
        for s in self.stream_sessions.values():
            if s.filename == os.path.basename(filename):
                log.debug("Stream: duplicate request for %s ignored (already active)", filename)
                return

        self._stream_preparing.add(filename)
        log.info("Stream request: %s (via %s)", filename, iface)
        session_id = random.randint(1, 0xFFFFFFFF)

        # Run HLS segmentation in a background thread
        def _prepare_and_send_meta():
            try:
                session = SenderStreamSession(
                    session_id=session_id,
                    filepath=filepath,
                    hls_cache_dir=config.HLS_CACHE_DIR,
                    ffmpeg_path=config.FFMPEG_PATH,
                    ffprobe_path=config.FFPROBE_PATH,
                    segment_duration=config.HLS_SEGMENT_DURATION,
                )
                if not session.prepare_hls():
                    log.error("Stream: ffmpeg HLS segmentation failed for %s", filename)
                    log.error("Ensure the video uses H.264/AAC codecs. "
                              "Re-encode with: ffmpeg -i input -c:v libx264 -c:a aac output.mp4")
                    return

                self.stream_sessions[session_id] = session

                # Send STREAM_META to receiver (on both interfaces)
                meta_payload = Packet.make_json_payload(session.get_stream_meta_payload())
                meta_pkt = Packet(
                    PType.STREAM_META,
                    seq_num=self._next_seq(),
                    session_id=session_id,
                    payload=meta_payload,
                )
                self.net.send_ctrl(meta_pkt, interface="lifi")
                self.net.send_ctrl(meta_pkt, interface="wifi")
                log.info("Stream session %d ready: %s (%d segments)",
                         session_id, filename, session.segment_count)
            finally:
                self._stream_preparing.discard(filename)

        t = threading.Thread(target=_prepare_and_send_meta, daemon=True)
        t.start()

    def _on_stream_segment_request(self, pkt: Packet, addr, iface):
        info = pkt.json_payload()
        stream_sid = info.get("stream_session_id", 0)
        seg_index = info.get("segment_index", 0)

        # Deduplicate: skip if this segment is already being transferred
        seg_key = (stream_sid, seg_index)
        with self._state_lock:
            if seg_key in self._active_seg_transfers:
                log.debug("Stream segment %d for session %d already in flight, skipping",
                          seg_index, stream_sid)
                return
            self._active_seg_transfers.add(seg_key)

        session = self.stream_sessions.get(stream_sid)
        if not session:
            log.warning("Stream segment request for unknown session %d", stream_sid)
            with self._state_lock:
                self._active_seg_transfers.discard(seg_key)
            return

        if iface == "wifi" and (not self.lifi_alive or not self.net.interface_available("lifi")):
            self.net.switch_to("wifi")
            self.stats["active_interface"] = "wifi"

        seg_path = session.get_segment_path(seg_index)
        if not seg_path:
            log.warning("Stream segment %d not found for session %d", seg_index, stream_sid)
            with self._state_lock:
                self._active_seg_transfers.discard(seg_key)
            return

        # Use a fresh transfer ID for every request. Reusing the same ID for
        # the same segment lets late ACKs from an older request complete a
        # newer transfer before its data reaches the receiver.
        transfer_sid = self._new_stream_transfer_id()
        log.debug("Sending stream segment %d for session %d (transfer_sid=%d)",
                  seg_index, stream_sid, transfer_sid)

        # Send the segment using the existing _send_file mechanism.
        # We pass extra metadata so the receiver knows this is a streaming segment.
        def _send_and_cleanup():
            try:
                self._send_file(
                    seg_path, transfer_sid,
                    stream_meta={
                        "is_stream_segment": True,
                        "stream_session_id": stream_sid,
                        "segment_index": seg_index,
                    },
                )
            finally:
                with self._state_lock:
                    self._active_seg_transfers.discard(seg_key)

        t = threading.Thread(target=_send_and_cleanup, daemon=True)
        t.start()

    def _new_stream_transfer_id(self) -> int:
        """Return a transfer session ID that is not currently in use."""
        with self._state_lock:
            while True:
                sid = random.randint(1, 0xFFFFFFFF)
                if (
                    sid not in self.active_transfers
                    and sid not in self.acked_chunks
                    and sid not in self._recent_stream_transfer_ids
                    and sid not in self.stream_sessions
                ):
                    self._recent_stream_transfer_ids.add(sid)
                    if len(self._recent_stream_transfer_ids) > 4096:
                        self._recent_stream_transfer_ids.clear()
                    return sid

    def _on_stream_close(self, pkt: Packet, addr, iface):
        info = pkt.json_payload()
        stream_sid = info.get("stream_session_id", 0)

        session = self.stream_sessions.pop(stream_sid, None)
        if not session:
            log.debug("Stream close for unknown session %d", stream_sid)
            return

        log.info("Stream session %d closing: %s", stream_sid, session.filename)

        # Defer cleanup: wait for any in-flight segment transfers to finish
        # before deleting the HLS temp files they're reading from.
        def _deferred_cleanup():
            for _ in range(60):  # poll for up to 30 seconds
                with self._state_lock:
                    active = any(sid == stream_sid for sid, _ in self._active_seg_transfers)
                if not active:
                    break
                time.sleep(0.5)
            session.cleanup()
            log.info("Stream session %d closed: %s", stream_sid, session.filename)

        threading.Thread(target=_deferred_cleanup, daemon=True).start()

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
    def _send_file(self, filepath: str, session_id: int, stream_meta: dict | None = None):
        chunker = FileChunker(filepath, config.CHUNK_SIZE)
        meta = chunker.metadata()
        total = chunker.total_chunks

        with self._state_lock:
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
        meta_for_payload = dict(meta)
        if stream_meta:
            meta_for_payload.update(stream_meta)
        meta_pkt = Packet(
            PType.FILE_META,
            seq_num=self._next_seq(),
            session_id=session_id,
            total_chunks=total,
            payload=Packet.make_json_payload(meta_for_payload),
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

        while True:
            with self._state_lock:
                if len(self.acked_chunks[session_id]) >= total:
                    break
            if not self.net.running:
                return

            # Fill send window
            while (len(unacked) < config.WINDOW_SIZE
                   and next_chunk < total):
                with self._state_lock:
                    already_acked = next_chunk in self.acked_chunks[session_id]
                if not already_acked:
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
            with self._state_lock:
                for cid in list(unacked):
                    if cid in self.acked_chunks[session_id]:
                        del unacked[cid]

            # Retransmit timed-out chunks
            now = time.time()
            for cid, send_time in list(unacked.items()):
                if now - send_time > config.ACK_TIMEOUT:
                    retries[cid] = retries.get(cid, 0) + 1
                    if retries[cid] > config.MAX_RETRIES:
                        # Keep transfer alive by clamping retry attempts and continuing.
                        # This avoids silently dropping a required chunk forever.
                        log.warning("Chunk %d exceeded max retries; continuing retransmit", cid)
                        retries[cid] = config.MAX_RETRIES
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
            with self._state_lock:
                acked_count = len(self.acked_chunks[session_id])
            self.stats["transfers"][session_id]["progress"] = acked_count / total
            self.stats["transfers"][session_id]["interface"] = self.net.active_interface

            time.sleep(0.001)  # yield

        # 3) All done
        done = Packet(PType.TRANSFER_COMPLETE, seq_num=self._next_seq(), session_id=session_id)
        self.net.send_ctrl(done)
        self.net.send_ctrl(done, interface="wifi")  # send on both to be sure
        with self._state_lock:
            transfer_info = self.active_transfers.get(session_id)
            if transfer_info:
                elapsed = time.time() - transfer_info["started"]
                log.info("Transfer complete: %s in %.1fs", meta["filename"], elapsed)
                del self.active_transfers[session_id]
            else:
                log.info("Transfer complete: %s", meta["filename"])
            self.acked_chunks.pop(session_id, None)  # cleanup to prevent memory leak

    # ── heartbeat ───────────────────────────────────────────
    def _heartbeat_loop(self):
        """Time-only heartbeat detection: failover when no ACK received within timeout."""
        hb_timeout = config.HEARTBEAT_INTERVAL * config.MAX_MISSED_HEARTBEATS
        while self.net.running:
            self.hb_seq += 1
            pkt = Packet(PType.HEARTBEAT, seq_num=self.hb_seq)
            self.net.send_ctrl(pkt, interface="lifi")

            time.sleep(config.HEARTBEAT_INTERVAL)

            elapsed = time.time() - self.last_hb_ack_time
            if elapsed > hb_timeout:
                if self.lifi_alive:
                    self.lifi_alive = False
                    self.lifi_was_down = True
                    log.warning("LiFi DOWN — no heartbeat ACK for %.1fs", elapsed)
                    self._initiate_failover()
            else:
                if self.lifi_was_down and self.lifi_alive:
                    self._initiate_switchback()
                    self.lifi_was_down = False

    def _initiate_failover(self):
        """Switch data transfer to WiFi — waits for receiver ACK."""
        log.warning(">>> FAILOVER: LiFi → WiFi <<<")
        self.stats["failover_count"] += 1
        self.stats["active_interface"] = "wifi"

        # Notify receiver and wait for acknowledgment
        self._switch_ack_event.clear()
        payload = Packet.make_json_payload({"interface": "wifi"})
        notify = Packet(PType.SWITCH_NOTIFY, seq_num=self._next_seq(), payload=payload)
        self.net.send_ctrl(notify, interface="wifi")

        if not self._switch_ack_event.wait(timeout=1.0):
            log.warning("No SWITCH_ACK from receiver — switching anyway")

        self.net.switch_to("wifi")

    def _initiate_switchback(self):
        """Switch data transfer back to LiFi — waits for receiver ACK."""
        log.info(">>> SWITCH BACK: WiFi → LiFi <<<")
        self.stats["active_interface"] = "lifi"

        self._switchback_ack_event.clear()
        payload = Packet.make_json_payload({"interface": "lifi"})
        notify = Packet(PType.SWITCH_BACK, seq_num=self._next_seq(), payload=payload)
        self.net.send_ctrl(notify, interface="wifi")  # Send over wifi since lifi just recovered

        if not self._switchback_ack_event.wait(timeout=1.0):
            log.warning("No SWITCH_BACK_ACK from receiver — switching anyway")

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
