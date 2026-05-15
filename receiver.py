"""
Receiver — connects to sender, browses files, receives transfers with failover.

Usage:
    python receiver.py
"""

import os
import sys
import json
import time
import logging
import threading
import hashlib
import shutil
from pathlib import Path
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import quote, unquote, urlparse

import config
from protocol.packet import Packet, PType
from protocol.chunk_manager import ChunkReassembler
from network.manager import NetworkManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("receiver")


class Receiver:
    def __init__(self):
        self.net = NetworkManager(
            my_lifi_ip=config.RECEIVER_LIFI_IP,
            my_wifi_ip=config.RECEIVER_WIFI_IP,
            peer_lifi_ip=config.SENDER_LIFI_IP,
            peer_wifi_ip=config.SENDER_WIFI_IP,
            data_port=config.DATA_PORT,
            control_port=config.CONTROL_PORT,
        )

        # ── state ───────────────────────────────────────────
        self.file_list: list[dict] = []
        self.reassemblers: dict[int, ChunkReassembler] = {}  # session_id -> reassembler
        self.requested_files: set[str] = set()
        self.stream_sessions: dict[str, int] = {}  # filename -> session_id
        self.stream_requested_chunks: dict[int, dict[int, float]] = {}
        self.stream_info: dict[str, dict] = {}
        self.hls_manifests: dict[str, dict] = {}
        self.hls_manifest_events: dict[str, threading.Event] = {}
        self.hls_segment_events: dict[str, threading.Event] = {}
        self.hls_segment_errors: dict[str, str] = {}
        self._hls_lock = threading.Lock()
        self._seq = 0
        self._file_list_event = threading.Event()
        self._state_lock = threading.Lock()

        # ── heartbeat tracking ──────────────────────────────
        self.last_hb_time = time.time()
        self.lifi_alive = True

        # ── stats (pushed to dashboard via SSE) ─────────────
        self.stats = {
            "active_interface": "lifi",
            "bytes_received": 0,
            "chunks_received": 0,
            "failover_count": 0,
            "start_time": time.time(),
            "transfers": {},
            "events": [],
        }
        self._stats_lock = threading.Lock()

        self._clear_stream_cache()
        self._register_handlers()

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _add_event(self, msg: str):
        with self._stats_lock:
            self.stats["events"].append({
                "time": time.strftime("%H:%M:%S"),
                "msg": msg,
            })
            if len(self.stats["events"]) > 100:
                self.stats["events"] = self.stats["events"][-100:]
        log.info("EVENT: %s", msg)

    @staticmethod
    def _clear_stream_cache():
        """Remove leftover temporary stream files from previous runs."""
        root = os.path.abspath(config.STREAM_CACHE_FOLDER)
        os.makedirs(root, exist_ok=True)
        for name in os.listdir(root):
            path = os.path.abspath(os.path.join(root, name))
            if path.startswith(root + os.sep) and os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            elif path.startswith(root + os.sep) and os.path.isfile(path):
                try:
                    os.remove(path)
                except OSError:
                    pass

    # ── handler registration ────────────────────────────────
    def _register_handlers(self):
        # Data port: receive DATA packets
        self.net.on_data(PType.DATA, self._on_data)

        # Control port: heartbeat, file meta, switch signals, etc.
        self.net.on_ctrl(PType.HEARTBEAT, self._on_heartbeat)
        self.net.on_ctrl(PType.FILE_META, self._on_file_meta)
        self.net.on_ctrl(PType.FILE_LIST_RESPONSE, self._on_file_list_response)
        self.net.on_ctrl(PType.TRANSFER_COMPLETE, self._on_transfer_complete)
        self.net.on_ctrl(PType.STREAM_MANIFEST_RESPONSE, self._on_stream_manifest_response)
        self.net.on_ctrl(PType.ERROR, self._on_error)
        self.net.on_ctrl(PType.SWITCH_NOTIFY, self._on_switch_notify)
        self.net.on_ctrl(PType.SWITCH_BACK, self._on_switch_back)

    # ── handlers ────────────────────────────────────────────
    def _on_data(self, pkt: Packet, addr, iface):
        sid = pkt.session_id
        reassembler = self.reassemblers.get(sid)
        if not reassembler:
            log.warning("DATA for unknown session %d", sid)
            return

        is_new = reassembler.add_chunk(pkt.chunk_id, pkt.payload)

        # Send ACK
        ack = Packet(
            PType.ACK,
            seq_num=self._next_seq(),
            chunk_id=pkt.chunk_id,
            session_id=sid,
        )
        self.net.send_data(ack, interface=iface)

        if is_new:
            with self._stats_lock:
                self.stats["bytes_received"] += len(pkt.payload)
                self.stats["chunks_received"] += 1
                if sid in self.stats["transfers"]:
                    self.stats["transfers"][sid]["progress"] = reassembler.progress
                    self.stats["transfers"][sid]["received"] = reassembler.received_count
                    self.stats["transfers"][sid]["bytes"] = reassembler.bytes_written
                    self.stats["transfers"][sid]["interface"] = iface

    def _on_heartbeat(self, pkt: Packet, addr, iface):
        self.last_hb_time = time.time()
        if not self.lifi_alive:
            self.lifi_alive = True
            self._add_event("LiFi heartbeat restored")

        # Reply
        ack = Packet(PType.HEARTBEAT_ACK, seq_num=pkt.seq_num)
        self.net.send_ctrl(ack, interface="lifi")

    def _on_file_meta(self, pkt: Packet, addr, iface):
        meta = pkt.json_payload()
        sid = pkt.session_id
        mode = meta.get("mode", "download")

        # Ignore duplicate FILE_META for an already-active session
        if sid in self.reassemblers:
            log.info("Duplicate FILE_META for session %d ignored", sid)
            return

        log.info("Receiving file: %s (%d chunks, %d bytes)",
                 meta["filename"], meta["total_chunks"], meta["file_size"])

        output_dir = (
            config.STREAM_CACHE_FOLDER
            if mode in {"stream", "hls_segment"}
            else config.RECEIVE_FOLDER
        )
        output_path = None
        if mode == "stream":
            safe_name = meta["filename"].replace("/", "_").replace("\\", "_")
            output_path = os.path.join(output_dir, f"{sid}_{safe_name}")
        elif mode == "hls_segment":
            output_path = self._stream_cache_path(meta["filename"])
            if output_path is None:
                log.error("Rejected unsafe HLS segment path: %s", meta["filename"])
                return

        reassembler = ChunkReassembler(
            filename=meta["filename"],
            file_size=meta["file_size"],
            total_chunks=meta["total_chunks"],
            chunk_size=meta["chunk_size"],
            file_hash=meta["file_hash"],
            output_dir=output_dir,
            output_path=output_path,
            preallocate=(mode != "stream"),
        )
        reassembler.transfer_mode = mode
        self.reassemblers[sid] = reassembler
        if mode == "stream":
            self.stream_sessions[meta["filename"]] = sid
            self.stream_requested_chunks[sid] = {}
            self.stream_info[meta["filename"]] = meta.get("stream", {})
            reassembler.stream_info = meta.get("stream", {})

        with self._stats_lock:
            self.stats["transfers"][sid] = {
                "filename": meta["filename"],
                "mode": mode,
                "stream": meta.get("stream", {}),
                "file_size": meta["file_size"],
                "total_chunks": meta["total_chunks"],
                "progress": 0.0,
                "received": 0,
                "bytes": 0,
                "started": time.time(),
                "completed": False,
                "interface": self.net.active_interface,
            }

        label = "HLS segment" if mode == "hls_segment" else ("Stream" if mode == "stream" else "Transfer")
        self._add_event(f"{label} started: {meta['filename']}")
        if mode == "stream":
            self._request_stream_time(sid, 0.0, config.STREAM_START_SECONDS)

    def _on_file_list_response(self, pkt: Packet, addr, iface):
        data = pkt.json_payload()
        self.file_list = data.get("files", [])
        log.info("Received file list: %d files", len(self.file_list))
        self._file_list_event.set()

    def _on_stream_manifest_response(self, pkt: Packet, addr, iface):
        data = pkt.json_payload()
        filename = data.get("filename") or data.get("path")
        playlist = data.get("playlist", "")
        if not filename or not playlist:
            log.warning("Invalid HLS manifest response")
            return

        rewritten = self._rewrite_hls_playlist(filename, playlist)
        with self._hls_lock:
            self.hls_manifests[filename] = {
                "playlist": rewritten,
                "segments": data.get("segments", []),
                "interface": iface,
            }
            event = self.hls_manifest_events.setdefault(filename, threading.Event())
            event.set()
        self._add_event(f"HLS playlist ready: {filename}")

    def _on_error(self, pkt: Packet, addr, iface):
        data = pkt.json_payload()
        message = data.get("error", "Remote endpoint error")
        log.error("Remote error via %s: %s", iface, message)
        self._add_event(f"Remote error: {message}")

    def _on_transfer_complete(self, pkt: Packet, addr, iface):
        sid = pkt.session_id
        reassembler = self.reassemblers.get(sid)
        if not reassembler:
            return  # already handled or unknown session
        mode = getattr(reassembler, "transfer_mode", "download")
        is_stream = mode == "stream"
        is_hls_segment = mode == "hls_segment"
        if reassembler.is_complete:
            ok = reassembler.verify()
            status = "VERIFIED ✓" if ok else "HASH MISMATCH ✗"
            log.info("Transfer complete: %s — %s", reassembler.filename, status)
            self._add_event(f"Complete: {reassembler.filename} ({status})")
            with self._stats_lock:
                if sid in self.stats["transfers"]:
                    self.stats["transfers"][sid]["completed"] = True
                    self.stats["transfers"][sid]["progress"] = reassembler.progress
            if is_hls_segment:
                self._mark_hls_segment_done(reassembler.filename, ok)
                self.reassemblers.pop(sid, None)
            elif is_stream:
                self._add_event(f"Stream cached temporarily: {reassembler.filename}")
            else:
                self.reassemblers.pop(sid, None)
        else:
            # Grace period: wait for late UDP packets before giving up
            missing = len(reassembler.missing_chunks())
            log.warning("TRANSFER_COMPLETE received but %d chunks missing — waiting for late packets...", missing)
            threading.Thread(
                target=self._grace_period_cleanup,
                args=(sid, missing),
                daemon=True,
            ).start()

    def _grace_period_cleanup(self, sid: int, initial_missing: int):
        """Wait up to 2 seconds for late packets, then finalize the transfer."""
        time.sleep(2.0)
        reassembler = self.reassemblers.get(sid)
        if not reassembler:
            return  # already cleaned up
        mode = getattr(reassembler, "transfer_mode", "download")
        is_stream = mode == "stream"
        is_hls_segment = mode == "hls_segment"

        if reassembler.is_complete:
            ok = reassembler.verify()
            status = "VERIFIED ✓" if ok else "HASH MISMATCH ✗"
            log.info("Transfer completed after grace period: %s — %s", reassembler.filename, status)
            self._add_event(f"Complete (late): {reassembler.filename} ({status})")
        else:
            still_missing = len(reassembler.missing_chunks())
            log.warning("Transfer incomplete after grace period: %s (%d chunks still missing)",
                        reassembler.filename, still_missing)
            self._add_event(f"Incomplete: {reassembler.filename} ({still_missing} missing)")

        with self._stats_lock:
            if sid in self.stats["transfers"]:
                self.stats["transfers"][sid]["completed"] = True
                self.stats["transfers"][sid]["progress"] = reassembler.progress
        if is_hls_segment:
            self._mark_hls_segment_done(reassembler.filename, reassembler.is_complete)
            self.reassemblers.pop(sid, None)
        elif not is_stream or not reassembler.is_complete:
            self._cleanup_reassembler(sid)
        else:
            self._add_event(f"Stream cached temporarily: {reassembler.filename}")

    def _on_switch_notify(self, pkt: Packet, addr, iface):
        info = pkt.json_payload()
        new_iface = info.get("interface", "wifi")
        log.warning(">>> FAILOVER: Sender switching to %s <<<", new_iface)
        self.net.switch_to(new_iface)
        self.stats["active_interface"] = new_iface
        self.stats["failover_count"] = self.stats.get("failover_count", 0) + 1
        self._add_event(f"Failover → {new_iface.upper()}")

        ack = Packet(PType.SWITCH_ACK, seq_num=self._next_seq())
        self.net.send_ctrl(ack, interface=new_iface)

    def _on_switch_back(self, pkt: Packet, addr, iface):
        info = pkt.json_payload()
        new_iface = info.get("interface", "lifi")
        log.info(">>> SWITCH BACK: Sender switching to %s <<<", new_iface)
        self.net.switch_to(new_iface)
        self.stats["active_interface"] = new_iface
        self._add_event(f"Switch back → {new_iface.upper()}")

        ack = Packet(PType.SWITCH_BACK_ACK, seq_num=self._next_seq())
        self.net.send_ctrl(ack, interface=new_iface)

    # ── commands ────────────────────────────────────────────
    def _cleanup_reassembler(self, sid: int, delete_stream_file: bool = True):
        """Remove an active reassembler; stream-mode files are temporary cache."""
        with self._state_lock:
            reassembler = self.reassemblers.pop(sid, None)
            if not reassembler:
                return
            if getattr(reassembler, "transfer_mode", "download") == "stream":
                self.stream_sessions.pop(reassembler.filename, None)
                self.stream_requested_chunks.pop(sid, None)
                self.stream_info.pop(reassembler.filename, None)
                self.requested_files.discard(reassembler.filename)

        if getattr(reassembler, "transfer_mode", "download") == "stream" and delete_stream_file:
            try:
                if os.path.exists(reassembler.output_path):
                    os.remove(reassembler.output_path)
            except OSError as e:
                log.warning("Could not delete stream cache %s: %s", reassembler.output_path, e)

    def request_file_list(self):
        """Ask sender for list of shared files."""
        self._file_list_event.clear()
        pkt = Packet(PType.FILE_LIST_REQUEST, seq_num=self._next_seq())
        # Send on both interfaces in case one is down
        self.net.send_ctrl(pkt)
        self.net.send_ctrl(pkt, interface="wifi")
        log.info("Requesting file list from sender...")
        self._file_list_event.wait(timeout=5)
        return self.file_list

    def _preferred_interface(self) -> str:
        """Use LiFi while it is alive; otherwise route requests over WiFi."""
        if self.lifi_alive and self.net.active_interface == "lifi":
            return "lifi"
        return "wifi"

    def _find_reassembler(self, filename: str) -> ChunkReassembler | None:
        with self._state_lock:
            for reassembler in self.reassemblers.values():
                if reassembler.filename == filename:
                    return reassembler
        return None

    def _received_path(self, filename: str) -> str | None:
        filepath = os.path.join(config.RECEIVE_FOLDER, filename)
        root = os.path.abspath(config.RECEIVE_FOLDER)
        resolved = os.path.abspath(filepath)
        if not resolved.startswith(root + os.sep) and resolved != root:
            return None
        return filepath

    @staticmethod
    def _remote_path_is_safe(filename: str) -> bool:
        normalized = filename.replace("\\", "/")
        if not normalized or normalized.startswith("/") or ":" in normalized:
            return False
        return all(part not in {"", ".."} for part in normalized.split("/"))

    @staticmethod
    def _hls_cache_key(filename: str) -> str:
        return hashlib.sha256(filename.encode("utf-8")).hexdigest()[:16]

    def _stream_cache_path(self, relative_path: str) -> str | None:
        normalized = relative_path.replace("\\", "/")
        if normalized.startswith("/") or ":" in normalized:
            return None
        if any(part in {"", ".."} for part in normalized.split("/")):
            return None

        root = os.path.abspath(config.STREAM_CACHE_FOLDER)
        resolved = os.path.abspath(os.path.join(root, normalized))
        if not resolved.startswith(root + os.sep) and resolved != root:
            return None
        return resolved

    def _hls_transfer_name(self, filename: str, segment: str) -> str:
        safe_segment = Path(segment).name
        return f"hls/{self._hls_cache_key(filename)}/{safe_segment}"

    def _hls_segment_cache_path(self, filename: str, segment: str) -> str | None:
        return self._stream_cache_path(self._hls_transfer_name(filename, segment))

    def _rewrite_hls_playlist(self, filename: str, playlist: str) -> str:
        encoded_filename = quote(filename, safe="")
        lines = []
        for line in playlist.splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                segment = quote(Path(stripped).name, safe="")
                lines.append(f"/api/stream/segment/{encoded_filename}/{segment}")
            else:
                lines.append(line)
        return "\n".join(lines) + "\n"

    def _mark_hls_segment_done(self, transfer_name: str, ok: bool):
        with self._hls_lock:
            if not ok:
                self.hls_segment_errors[transfer_name] = "segment hash verification failed"
            event = self.hls_segment_events.setdefault(transfer_name, threading.Event())
            event.set()

    def _stream_source_path(self, filename: str) -> str | None:
        reassembler = self._find_reassembler(filename)
        if reassembler:
            return reassembler.output_path
        return self._received_path(filename)

    def _request_stream_window(self, sid: int, start_chunk: int, chunk_count: int):
        reassembler = self.reassemblers.get(sid)
        if not reassembler:
            return

        start = max(0, start_chunk)
        end = min(reassembler.total_chunks - 1, start + max(1, chunk_count) - 1)
        now = time.time()
        with self._state_lock:
            requested = self.stream_requested_chunks.setdefault(sid, {})
            missing = [
                cid for cid in range(start, end + 1)
                if cid not in requested or now - requested[cid] > config.ACK_TIMEOUT
            ]
            for cid in missing:
                requested[cid] = now
        if not missing:
            return

        payload = Packet.make_json_payload({"start": missing[0], "end": missing[-1]})
        pkt = Packet(
            PType.STREAM_CHUNK_REQUEST,
            seq_num=self._next_seq(),
            session_id=sid,
            payload=payload,
        )
        self.net.send_ctrl(pkt, interface=self._preferred_interface())

    def _request_stream_time(self, sid: int, timestamp: float, seconds: float):
        reassembler = self.reassemblers.get(sid)
        if not reassembler:
            return

        info = getattr(reassembler, "stream_info", {}) or {}
        duration = float(info.get("duration", 0) or 0)
        timestamp = max(0.0, timestamp)
        if duration > 0:
            timestamp = min(timestamp, duration)

        payload = Packet.make_json_payload({"time": timestamp, "seconds": seconds})
        pkt = Packet(
            PType.STREAM_TIME_REQUEST,
            seq_num=self._next_seq(),
            session_id=sid,
            payload=payload,
        )
        self.net.send_ctrl(pkt, interface=self._preferred_interface())

    def request_stream_time(self, filename: str, timestamp: float, seconds: float | None = None) -> dict:
        return {"status": "ok", "requested": False, "mode": "hls"}

    def _ensure_stream_buffer(self, filename: str, byte_pos: int = 0):
        sid = self.stream_sessions.get(filename)
        if sid is None:
            return
        reassembler = self.reassemblers.get(sid)
        if not reassembler:
            return

        wanted_chunk = max(0, byte_pos // reassembler.chunk_size)
        contiguous = reassembler.contiguous_chunks
        if wanted_chunk > contiguous + config.STREAM_LOW_WATER_CHUNKS:
            self._request_stream_window(sid, wanted_chunk, config.STREAM_REQUEST_CHUNKS)
        elif wanted_chunk >= contiguous - config.STREAM_LOW_WATER_CHUNKS:
            self._request_stream_window(sid, contiguous, config.STREAM_REQUEST_CHUNKS)

    def request_file(self, filename: str, mode: str = "download"):
        """Ask sender to transfer a file over the best currently available link."""
        filepath = self._received_path(filename)
        with self._state_lock:
            already_requested = filename in self.requested_files
            active = any(r.filename == filename for r in self.reassemblers.values())
            complete = bool(filepath and os.path.exists(filepath))
            if already_requested and (active or complete):
                log.info("File already requested: %s", filename)
                return
            if not already_requested:
                self.requested_files.add(filename)

        iface = self._preferred_interface()
        payload = Packet.make_json_payload({"filename": filename, "mode": mode})
        pkt = Packet(PType.FILE_REQUEST, seq_num=self._next_seq(), payload=payload)
        self.net.send_ctrl(pkt, interface=iface)
        log.info("Requested file: %s via %s", filename, iface)

    def request_hls_manifest(self, filename: str, timeout: float | None = None) -> dict | None:
        if not self._remote_path_is_safe(filename):
            return None

        with self._hls_lock:
            cached = self.hls_manifests.get(filename)
            if cached:
                return cached
            event = self.hls_manifest_events.setdefault(filename, threading.Event())
            event.clear()

        deadline = time.time() + (timeout if timeout is not None else config.STREAM_WAIT_TIMEOUT)
        requested_at = 0.0
        while time.time() < deadline:
            with self._hls_lock:
                cached = self.hls_manifests.get(filename)
                if cached:
                    return cached

            now = time.time()
            if now - requested_at >= 1.0:
                payload = Packet.make_json_payload({"filename": filename})
                pkt = Packet(
                    PType.STREAM_MANIFEST_REQUEST,
                    seq_num=self._next_seq(),
                    payload=payload,
                )
                iface = self._preferred_interface()
                self.net.send_ctrl(pkt, interface=iface)
                if iface != "wifi":
                    self.net.send_ctrl(pkt, interface="wifi")
                requested_at = now
                log.info("Requested HLS playlist: %s via %s", filename, iface)

            event.wait(timeout=0.25)

        return None

    def request_hls_segment(
        self,
        filename: str,
        segment: str,
        timeout: float | None = None,
    ) -> str | None:
        if not self._remote_path_is_safe(filename):
            return None

        safe_segment = Path(segment).name
        transfer_name = self._hls_transfer_name(filename, safe_segment)
        cached_path = self._hls_segment_cache_path(filename, safe_segment)
        if cached_path is None:
            return None
        if os.path.exists(cached_path):
            return cached_path

        with self._hls_lock:
            event = self.hls_segment_events.setdefault(transfer_name, threading.Event())
            event.clear()
            self.hls_segment_errors.pop(transfer_name, None)

        deadline = time.time() + (timeout if timeout is not None else config.STREAM_WAIT_TIMEOUT)
        requested_at = 0.0
        while time.time() < deadline:
            if os.path.exists(cached_path):
                return cached_path

            with self._hls_lock:
                error = self.hls_segment_errors.get(transfer_name)
                if error:
                    log.error("HLS segment failed: %s", error)
                    return None

            now = time.time()
            if now - requested_at >= 1.0:
                payload = Packet.make_json_payload({
                    "filename": filename,
                    "segment": safe_segment,
                })
                pkt = Packet(
                    PType.STREAM_SEGMENT_REQUEST,
                    seq_num=self._next_seq(),
                    payload=payload,
                )
                iface = self._preferred_interface()
                self.net.send_ctrl(pkt, interface=iface)
                if iface != "wifi":
                    self.net.send_ctrl(pkt, interface="wifi")
                requested_at = now
                log.info("Requested HLS segment: %s for %s via %s", safe_segment, filename, iface)

            event.wait(timeout=0.25)

        return cached_path if os.path.exists(cached_path) else None

    def start_stream(self, filename: str) -> dict:
        """Prepare an HLS playlist and return the local playlist URL."""
        if not self._remote_path_is_safe(filename):
            return {"error": "Path traversal rejected"}

        manifest = self.request_hls_manifest(filename)
        if manifest is None:
            return {"error": "Timed out preparing HLS playlist"}

        self._add_event(f"HLS stream requested: {filename}")
        return self.stream_status(filename)

    def stop_stream(self, filename: str) -> dict:
        """Delete cached HLS segments for a video player session."""
        if not self._remote_path_is_safe(filename):
            return {"status": "ok", "deleted": False}

        with self._hls_lock:
            self.hls_manifests.pop(filename, None)
            self.hls_manifest_events.pop(filename, None)

        cache_dir = self._stream_cache_path(f"hls/{self._hls_cache_key(filename)}")
        deleted = False
        if cache_dir and os.path.isdir(cache_dir):
            shutil.rmtree(cache_dir, ignore_errors=True)
            deleted = True
        self._add_event(f"HLS stream cache deleted: {filename}")
        return {"status": "ok", "deleted": deleted}

    def stream_status(self, filename: str) -> dict:
        """Return HLS playlist readiness for HTTP playback."""
        if not self._remote_path_is_safe(filename):
            return {"error": "Path traversal rejected", "ready": False}

        with self._hls_lock:
            manifest = self.hls_manifests.get(filename)

        playlist_url = f"/api/stream/playlist/{quote(filename, safe='')}"
        return {
            "filename": filename,
            "ready": manifest is not None,
            "complete": manifest is not None,
            "playlist_url": playlist_url,
            "segment_count": len((manifest or {}).get("segments", [])),
            "mode": "hls",
            "interface": self.net.active_interface,
        }

    def wait_for_stream_bytes(self, filename: str, start_byte: int, end_byte: int, timeout: float) -> bool:
        """Wait until a sparse byte range is available or the stream times out."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            reassembler = self._find_reassembler(filename)
            if not reassembler:
                return os.path.exists(self._received_path(filename) or "")
            if reassembler.has_byte_range(start_byte, end_byte):
                return True
            time.sleep(0.1)
        return False

    # ── heartbeat monitor ───────────────────────────────────
    def _heartbeat_monitor(self):
        while self.net.running:
            elapsed = time.time() - self.last_hb_time
            hb_timeout = config.HEARTBEAT_INTERVAL * (config.MAX_MISSED_HEARTBEATS + 1)
            if elapsed > hb_timeout and self.lifi_alive:
                self.lifi_alive = False
                self.net.switch_to("wifi")
                self.stats["active_interface"] = "wifi"
                self._add_event("LiFi heartbeat lost — expecting failover")
            time.sleep(config.HEARTBEAT_INTERVAL)

    # ── dashboard HTTP server ───────────────────────────────
    def _start_dashboard(self):
        receiver_ref = self

        class DashboardHandler(SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                self.receiver = receiver_ref
                super().__init__(*args, directory=config.DASHBOARD_DIR, **kwargs)

            def log_message(self, format, *args):
                pass  # Suppress HTTP logs

            def do_GET(self):
                path = urlparse(self.path).path
                if path == "/api/status":
                    self._json_response(self.receiver.stats)
                elif path == "/api/files":
                    self._json_response({"files": self.receiver.file_list})
                elif path == "/api/refresh_files":
                    files = self.receiver.request_file_list()
                    self._json_response({"files": files})
                elif path.startswith("/api/stream/playlist/"):
                    filename = unquote(path[len("/api/stream/playlist/"):])
                    self._serve_hls_playlist(filename)
                elif path.startswith("/api/stream/segment/"):
                    rest = path[len("/api/stream/segment/"):]
                    parts = rest.split("/", 1)
                    if len(parts) != 2:
                        self.send_error(404, "segment not found")
                        return
                    filename = unquote(parts[0])
                    segment = unquote(parts[1])
                    self._serve_hls_segment(filename, segment)
                elif path.startswith("/api/stream/status/"):
                    filename = unquote(path[len("/api/stream/status/"):])
                    self._json_response(self.receiver.stream_status(filename))
                elif path.startswith("/api/stream/"):
                    filename = unquote(path[len("/api/stream/"):])
                    self._serve_hls_playlist(filename)
                elif path == "/api/events":
                    self._sse_stream()
                else:
                    super().do_GET()

            def do_POST(self):
                path = urlparse(self.path).path
                if path.startswith("/api/download/"):
                    filename = unquote(path[len("/api/download/"):])
                    self.receiver.request_file(filename)
                    self._json_response({"status": "ok", "filename": filename})
                elif path.startswith("/api/stream/start/"):
                    filename = unquote(path[len("/api/stream/start/"):])
                    status = self.receiver.start_stream(filename)
                    if "error" in status:
                        self._json_response(status, code=400)
                    else:
                        self._json_response({"status": "buffering", **status})
                elif path.startswith("/api/stream/time/"):
                    filename = unquote(path[len("/api/stream/time/"):])
                    self._json_response(self.receiver.request_stream_time(filename, 0.0))
                elif path.startswith("/api/stream/stop/"):
                    filename = unquote(path[len("/api/stream/stop/"):])
                    self._json_response(self.receiver.stop_stream(filename))
                else:
                    self.send_error(404)

            def _read_json_body(self):
                try:
                    length = int(self.headers.get("Content-Length", "0") or 0)
                    if length <= 0:
                        return {}
                    raw = self.rfile.read(length)
                    return json.loads(raw.decode("utf-8"))
                except (ValueError, json.JSONDecodeError):
                    return {}

            def _json_response(self, data, code=200):
                body = json.dumps(data).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", len(body))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)

            def _serve_hls_playlist(self, filename):
                status = self.receiver.start_stream(filename)
                if "error" in status:
                    self.send_error(504, status["error"])
                    return

                with self.receiver._hls_lock:
                    manifest = self.receiver.hls_manifests.get(filename)
                if not manifest:
                    self.send_error(404, "playlist not found")
                    return

                body = manifest["playlist"].encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.apple.mpegurl")
                self.send_header("Content-Length", len(body))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)

            def _serve_hls_segment(self, filename, segment):
                filepath = self.receiver.request_hls_segment(filename, segment)
                if filepath is None:
                    self.send_error(504, "segment transfer timed out")
                    return

                path = Path(filepath)
                if not path.exists():
                    self.send_error(404, "segment not found")
                    return

                data = path.read_bytes()
                if path.suffix == ".m4s":
                    content_type = "video/iso.segment"
                elif path.suffix == ".mp4":
                    content_type = "video/mp4"
                else:
                    content_type = "video/mp2t"

                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", len(data))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(data)

            def _sse_stream(self):
                """Server-Sent Events stream for real-time dashboard updates."""
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()

                try:
                    while self.receiver.net.running:
                        data = json.dumps(self.receiver.stats)
                        self.wfile.write(f"data: {data}\n\n".encode())
                        self.wfile.flush()
                        time.sleep(0.5)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass

        # ThreadingHTTPServer: each request gets its own thread.
        # Critical for SSE — without this, the long-lived SSE connection
        # blocks ALL other HTTP requests (file browser, API, etc.)
        class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
            daemon_threads = True

        server = ThreadedHTTPServer(("0.0.0.0", config.DASHBOARD_PORT), DashboardHandler)
        log.info("Dashboard: http://localhost:%d", config.DASHBOARD_PORT)
        try:
            while self.net.running:
                server.handle_request()
        except Exception:
            pass

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
        lifi_ok = config.RECEIVER_LIFI_IP in all_ips
        wifi_ok = config.RECEIVER_WIFI_IP in all_ips

        if not lifi_ok:
            log.warning("⚠ LiFi IP %s not found on any interface!", config.RECEIVER_LIFI_IP)
        if not wifi_ok:
            log.warning("⚠ WiFi IP %s not found on any interface!", config.RECEIVER_WIFI_IP)
        if lifi_ok and wifi_ok:
            log.info("✓ Both interfaces detected")
        return lifi_ok or wifi_ok  # At least one must be present

    # ── main ────────────────────────────────────────────────
    def start(self):
        log.info("=" * 50)
        log.info("  LiFi-WiFi RECEIVER starting")
        log.info("  Receive folder: %s", config.RECEIVE_FOLDER)
        log.info("  LiFi IP: %s  |  WiFi IP: %s", config.RECEIVER_LIFI_IP, config.RECEIVER_WIFI_IP)
        log.info("  Peer  → LiFi: %s  |  WiFi: %s", config.SENDER_LIFI_IP, config.SENDER_WIFI_IP)
        log.info("  Dashboard: http://localhost:%d", config.DASHBOARD_PORT)
        log.info("=" * 50)

        self._validate_interfaces()

        self.net.start()
        self.last_hb_time = time.time()

        # Start heartbeat monitor
        hb_thread = threading.Thread(target=self._heartbeat_monitor, daemon=True)
        hb_thread.start()

        # Start dashboard server
        dash_thread = threading.Thread(target=self._start_dashboard, daemon=True)
        dash_thread.start()

        # Initial file list request
        time.sleep(0.5)
        self.request_file_list()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("Shutting down...")
            self.net.stop()


if __name__ == "__main__":
    Receiver().start()
