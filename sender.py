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
import hashlib
from pathlib import Path

import config
from protocol.packet import Packet, PType
from protocol.chunk_manager import FileChunker
from protocol.hls import HLSManager
from protocol.video_index import VideoStreamIndex, ffprobe_available
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
        self.stream_sources: dict[int, dict] = {}      # session_id -> on-demand stream state
        self.hls = HLSManager(config.HLS_CACHE_FOLDER)
        self._seq = 0
        self._state_lock = threading.Lock()  # protects shared mutable state

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
        self.net.on_ctrl(PType.CANCEL_TRANSFER, self._on_cancel_transfer)
        self.net.on_ctrl(PType.STREAM_CHUNK_REQUEST, self._on_stream_chunk_request)
        self.net.on_ctrl(PType.STREAM_TIME_REQUEST, self._on_stream_time_request)
        self.net.on_ctrl(PType.STREAM_MANIFEST_REQUEST, self._on_stream_manifest_request)
        self.net.on_ctrl(PType.STREAM_SEGMENT_REQUEST, self._on_stream_segment_request)
        self.net.on_ctrl(PType.SWITCH_ACK, self._on_switch_ack)
        self.net.on_ctrl(PType.SWITCH_BACK_ACK, self._on_switchback_ack)

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
        mode = info.get("mode", "download")
        filepath = self._resolve_shared_file(filename)
        if filepath is None:
            return

        log.info("File requested: %s [%s] (via %s)", filename, mode, iface)
        session_id = random.randint(1, 0xFFFFFFFF)
        if mode == "stream":
            threading.Thread(
                target=self._send_hls_manifest,
                args=(filename, filepath, iface),
                daemon=True,
            ).start()
        else:
            t = threading.Thread(target=self._send_file, args=(filepath, session_id, mode), daemon=True)
            t.start()

    def _on_stream_manifest_request(self, pkt: Packet, addr, iface):
        info = pkt.json_payload()
        filename = info.get("filename") or info.get("path", "")
        filepath = self._resolve_shared_file(filename)
        if filepath is None:
            self._send_error(f"Requested video not found: {filename}", iface)
            return
        threading.Thread(
            target=self._send_hls_manifest,
            args=(filename, filepath, iface),
            daemon=True,
        ).start()

    def _on_stream_segment_request(self, pkt: Packet, addr, iface):
        info = pkt.json_payload()
        filename = info.get("filename") or info.get("path", "")
        segment = Path(str(info.get("segment", ""))).name
        filepath = self._resolve_shared_file(filename)
        if filepath is None or not segment:
            self._send_error(f"Invalid HLS segment request: {filename} / {segment}", iface)
            return

        threading.Thread(
            target=self._send_hls_segment,
            args=(filename, filepath, segment, iface),
            daemon=True,
        ).start()

    def _send_hls_segment(self, filename: str, filepath: str, segment: str, iface: str):
        try:
            segment_path = self.hls.segment_path(filepath, segment)
        except Exception as exc:
            log.exception("Could not resolve HLS segment")
            self._send_error(str(exc), iface)
            return

        cache_key = self._hls_cache_key(filename)
        transfer_name = f"hls/{cache_key}/{segment}"
        session_id = random.randint(1, 0xFFFFFFFF)
        log.info("HLS segment requested: %s for %s via %s", segment, filename, iface)
        t = threading.Thread(
            target=self._send_file,
            args=(str(segment_path), session_id, "hls_segment", transfer_name),
            daemon=True,
        )
        t.start()

    def _on_cancel_transfer(self, pkt: Packet, addr, iface):
        with self._state_lock:
            state = self.active_transfers.get(pkt.session_id)
            if state:
                state["cancelled"] = True
                log.info("Transfer cancelled by receiver: %s", state.get("filename", pkt.session_id))
            stream = self.stream_sources.pop(pkt.session_id, None)
            if stream:
                log.info("Stream cancelled by receiver: %s", stream.get("filename", pkt.session_id))

    def _on_stream_chunk_request(self, pkt: Packet, addr, iface):
        info = pkt.json_payload()
        start = max(0, int(info.get("start", 0)))
        end = max(start, int(info.get("end", start)))
        with self._state_lock:
            source = self.stream_sources.get(pkt.session_id)
        if not source:
            log.warning("Stream chunks requested for unknown session %d", pkt.session_id)
            return

        t = threading.Thread(
            target=self._send_stream_chunks,
            args=(pkt.session_id, start, end, iface),
            daemon=True,
        )
        t.start()

    def _on_stream_time_request(self, pkt: Packet, addr, iface):
        info = pkt.json_payload()
        timestamp = max(0.0, float(info.get("time", 0.0)))
        seconds = max(0.5, float(info.get("seconds", config.STREAM_WINDOW_SECONDS)))
        with self._state_lock:
            source = self.stream_sources.get(pkt.session_id)
        if not source:
            log.warning("Stream time requested for unknown session %d", pkt.session_id)
            return

        index: VideoStreamIndex | None = source.get("index")
        if index:
            start, end = index.chunks_for_time_range(timestamp, seconds)
        else:
            start = max(0, int(info.get("start", 0)))
            end = max(start, start + config.STREAM_REQUEST_CHUNKS - 1)

        t = threading.Thread(
            target=self._send_stream_chunks,
            args=(pkt.session_id, start, end, iface),
            daemon=True,
        )
        t.start()

    def _on_switch_ack(self, pkt: Packet, addr, iface):
        log.info("Receiver acknowledged switch to WiFi")
        self._switch_ack_event.set()

    def _on_switchback_ack(self, pkt: Packet, addr, iface):
        log.info("Receiver acknowledged switch back to LiFi")
        self._switchback_ack_event.set()

    def _send_hls_manifest(self, filename: str, filepath: str, iface: str):
        try:
            log.info("Preparing HLS manifest for %s", filename)
            manifest = self.hls.prepare(filepath)
            payload = Packet.make_json_payload({
                "filename": filename,
                "playlist": manifest.playlist_text,
                "segments": [Path(segment).name for segment in manifest.segments],
            })
            if len(payload) > 60000:
                raise ValueError("HLS manifest is too large for one control packet")
            resp = Packet(
                PType.STREAM_MANIFEST_RESPONSE,
                seq_num=self._next_seq(),
                payload=payload,
            )
            self.net.send_ctrl(resp, interface=iface)
            log.info("HLS manifest ready: %s (%d segments)", filename, len(manifest.segments))
        except Exception as exc:
            log.exception("Could not prepare HLS manifest")
            self._send_error(str(exc), iface)

    def _send_error(self, message: str, iface: str | None = None):
        pkt = Packet(
            PType.ERROR,
            seq_num=self._next_seq(),
            payload=Packet.make_json_payload({"error": message}),
        )
        self.net.send_ctrl(pkt, interface=iface)

    def _resolve_shared_file(self, filename: str) -> str | None:
        filepath = os.path.join(config.SHARED_FOLDER, filename)
        root = os.path.abspath(config.SHARED_FOLDER)
        resolved = os.path.abspath(filepath)
        if not resolved.startswith(root + os.sep) and resolved != root:
            log.error("Path traversal rejected: %s", filename)
            return None
        if not os.path.isfile(resolved):
            log.error("Requested file not found: %s", filename)
            return None
        return resolved

    @staticmethod
    def _hls_cache_key(filename: str) -> str:
        return hashlib.sha256(filename.encode("utf-8")).hexdigest()[:16]

    # ── shared folder scanning ──────────────────────────────
    def _scan_shared_folder(self) -> list[dict]:
        files = []
        for root, dirs, filenames in os.walk(config.SHARED_FOLDER):
            dirs[:] = [d for d in dirs if d not in {".hls", ".hls_cache"}]
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
    def _start_stream_session(self, filepath: str, session_id: int):
        chunker = FileChunker(filepath, config.CHUNK_SIZE)
        index = VideoStreamIndex.build(filepath, config.CHUNK_SIZE, config.FFPROBE_BIN)
        meta = chunker.metadata()
        meta["mode"] = "stream"
        meta["stream"] = index.to_json()
        with self._state_lock:
            self.stream_sources[session_id] = {
                "chunker": chunker,
                "index": index,
                "filename": meta["filename"],
                "total_chunks": chunker.total_chunks,
                "started": time.time(),
            }

        meta_pkt = Packet(
            PType.FILE_META,
            seq_num=self._next_seq(),
            session_id=session_id,
            total_chunks=chunker.total_chunks,
            payload=Packet.make_json_payload(meta),
        )
        self.net.send_ctrl(meta_pkt)
        self.net.send_ctrl(meta_pkt, interface="wifi")
        if not ffprobe_available(config.FFPROBE_BIN):
            log.warning("ffprobe not found; using estimated timestamp index")
        log.info(
            "Stream session ready: %s (%d chunks, %.1fs, index=%s)",
            meta["filename"],
            chunker.total_chunks,
            index.duration,
            index.source,
        )

    def _send_stream_chunks(self, session_id: int, start: int, end: int, iface: str):
        with self._state_lock:
            source = self.stream_sources.get(session_id)
        if not source:
            return

        chunker: FileChunker = source["chunker"]
        total = source["total_chunks"]
        end = min(end, total - 1)
        for cid in range(start, end + 1):
            with self._state_lock:
                if session_id not in self.stream_sources:
                    return
            data = chunker.get_chunk(cid)
            pkt = Packet(
                PType.DATA,
                seq_num=self._next_seq(),
                chunk_id=cid,
                total_chunks=total,
                session_id=session_id,
                payload=data,
            )
            self.net.send_data(pkt, interface=iface)
            self.stats["bytes_sent"] += len(data)
            self.stats["chunks_sent"] += 1
        log.debug("Sent stream chunks %d-%d for session %d", start, end, session_id)

    def _send_file(
        self,
        filepath: str,
        session_id: int,
        mode: str = "download",
        transfer_name: str | None = None,
    ):
        chunker = FileChunker(filepath, config.CHUNK_SIZE, transfer_name=transfer_name)
        meta = chunker.metadata()
        meta["mode"] = mode
        total = chunker.total_chunks

        with self._state_lock:
            self.acked_chunks[session_id] = set()
            self.active_transfers[session_id] = {
                "filename": meta["filename"],
                "total_chunks": total,
                "started": time.time(),
                "mode": mode,
                "cancelled": False,
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

        while True:
            with self._state_lock:
                if self.active_transfers.get(session_id, {}).get("cancelled"):
                    log.info("Stopping cancelled transfer: %s", meta["filename"])
                    break
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
        with self._state_lock:
            state = self.active_transfers.get(session_id, {})
            cancelled = state.get("cancelled", False)
            elapsed = time.time() - state.get("started", time.time())
        if not cancelled:
            done = Packet(PType.TRANSFER_COMPLETE, seq_num=self._next_seq(), session_id=session_id)
            self.net.send_ctrl(done)
            self.net.send_ctrl(done, interface="wifi")  # send on both to be sure
            log.info("Transfer complete: %s in %.1fs", meta["filename"], elapsed)
        with self._state_lock:
            self.active_transfers.pop(session_id, None)
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
