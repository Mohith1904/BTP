"""
Receiver — connects to sender, browses files, receives transfers with failover.

Usage:
    python receiver.py
"""

import os
import sys
import json
import time
import mimetypes
import logging
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import unquote

import config
from protocol.packet import Packet, PType
from protocol.chunk_manager import ChunkReassembler
from protocol.stream_manager import ReceiverStreamSession
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
        self._seq = 0
        self._file_list_event = threading.Event()

        # ── streaming state ─────────────────────────────────
        self.stream_sessions: dict[int, ReceiverStreamSession] = {}
        self._stream_meta_event = threading.Event()  # signaled when STREAM_META arrives
        self._stream_meta_data: dict = {}            # temp storage for stream meta
        # Maps transfer_session_id -> (stream_session_id, segment_index)
        self._segment_map: dict[int, tuple[int, int]] = {}
        # Maps transfer_session_id -> threading.Event (signaled when segment transfer completes)
        self._segment_events: dict[int, threading.Event] = {}
        # Track completed transfer session IDs to prevent late FILE_META from
        # re-creating reassemblers that overwrite completed segment files
        self._completed_transfers: set[int] = set()
        self._closed_stream_sessions: set[int] = set()

        # ── heartbeat tracking ──────────────────────────────
        self.last_hb_time = time.time()
        self.lifi_alive = True

        # ── stats (pushed to dashboard via SSE) ─────────────
        self.stats = {
            "active_interface": self.net.active_interface,
            "bytes_received": 0,
            "chunks_received": 0,
            "failover_count": 0,
            "start_time": time.time(),
            "transfers": {},
            "events": [],
            "stream_sessions": {},
        }
        self._stats_lock = threading.Lock()

        self._register_handlers()

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _send_ctrl_with_fallback(self, pkt: Packet):
        """Send a control packet on the active interface and the standby link."""
        primary = self.net.active_interface
        fallback = "wifi" if primary == "lifi" else "lifi"
        self.net.send_ctrl(pkt, interface=primary)
        self.net.send_ctrl(pkt, interface=fallback)

    def _add_event(self, msg: str):
        with self._stats_lock:
            self.stats["events"].append({
                "time": time.strftime("%H:%M:%S"),
                "msg": msg,
            })
            if len(self.stats["events"]) > 100:
                self.stats["events"] = self.stats["events"][-100:]
        log.info("EVENT: %s", msg)

    # ── handler registration ────────────────────────────────
    def _register_handlers(self):
        # Data port: receive DATA packets
        self.net.on_data(PType.DATA, self._on_data)

        # Control port: heartbeat, file meta, switch signals, etc.
        self.net.on_ctrl(PType.HEARTBEAT, self._on_heartbeat)
        self.net.on_ctrl(PType.FILE_META, self._on_file_meta)
        self.net.on_ctrl(PType.FILE_LIST_RESPONSE, self._on_file_list_response)
        self.net.on_ctrl(PType.TRANSFER_COMPLETE, self._on_transfer_complete)
        self.net.on_ctrl(PType.SWITCH_NOTIFY, self._on_switch_notify)
        self.net.on_ctrl(PType.SWITCH_BACK, self._on_switch_back)

        # Streaming handler
        self.net.on_ctrl(PType.STREAM_META, self._on_stream_meta)

    # ── handlers ────────────────────────────────────────────
    def _on_data(self, pkt: Packet, addr, iface):
        sid = pkt.session_id
        reassembler = self.reassemblers.get(sid)
        if not reassembler:
            if sid in self._completed_transfers:
                ack = Packet(
                    PType.ACK,
                    seq_num=self._next_seq(),
                    chunk_id=pkt.chunk_id,
                    session_id=sid,
                )
                self.net.send_data(ack, interface=iface)
                log.debug("Late DATA for completed session %d ignored", sid)
                return
            log.warning("DATA for unknown session %d", sid)
            return

        # Send ACK
        ack = Packet(
            PType.ACK,
            seq_num=self._next_seq(),
            chunk_id=pkt.chunk_id,
            session_id=sid,
        )

        try:
            is_new = reassembler.add_chunk(pkt.chunk_id, pkt.payload)
        except FileNotFoundError:
            seg_info = self._segment_map.pop(sid, None)
            if seg_info:
                stream_sid, seg_index = seg_info
                stream_session = self.stream_sessions.get(stream_sid)
                if stream_session:
                    stream_session.mark_failed(seg_index)
                self._segment_events.pop(sid, None)
                self._completed_transfers.add(sid)
                self.reassemblers.pop(sid, None)
                self.net.send_data(ack, interface=iface)
                log.debug("Late DATA for closed stream segment %d ignored", seg_index)
                return
            raise

        self.net.send_data(ack, interface=iface)

        if is_new:
            with self._stats_lock:
                self.stats["bytes_received"] += len(pkt.payload)
                self.stats["chunks_received"] += 1
                if sid in self.stats["transfers"]:
                    self.stats["transfers"][sid]["progress"] = reassembler.progress
                    self.stats["transfers"][sid]["received"] = reassembler.received_count
                    self.stats["transfers"][sid]["bytes"] = reassembler.bytes_written

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

        # Ignore duplicate FILE_META for an already-active or already-completed session
        if sid in self.reassemblers or sid in self._completed_transfers:
            log.info("Duplicate FILE_META for session %d ignored", sid)
            return

        # Check if this is a streaming segment transfer
        is_stream_seg = meta.get("is_stream_segment", False)
        stream_sid = meta.get("stream_session_id", 0)
        seg_index = meta.get("segment_index", 0)

        if is_stream_seg and stream_sid in self._closed_stream_sessions:
            self._completed_transfers.add(sid)
            log.debug("Ignoring FILE_META for closed stream session %d", stream_sid)
            return

        if is_stream_seg:
            # Route to streaming cache directory instead of received/
            stream_session = self.stream_sessions.get(stream_sid)
            if not stream_session:
                log.warning("Stream segment for unknown stream session %d", stream_sid)
                return
            output_dir = stream_session.cache_dir
            log.info("Receiving stream segment %d for session %d (transfer_sid=%d)",
                     seg_index, stream_sid, sid)
        else:
            output_dir = config.RECEIVE_FOLDER
            log.info("Receiving file: %s (%d chunks, %d bytes)",
                     meta["filename"], meta["total_chunks"], meta["file_size"])

        reassembler = ChunkReassembler(
            filename=meta["filename"],
            file_size=meta["file_size"],
            total_chunks=meta["total_chunks"],
            chunk_size=meta["chunk_size"],
            file_hash=meta["file_hash"],
            output_dir=output_dir,
        )
        self.reassemblers[sid] = reassembler

        if is_stream_seg:
            # Track mapping so we can signal completion
            self._segment_map[sid] = (stream_sid, seg_index)
            # Don't overwrite existing event — _serve_hls may already be waiting on it
            if sid not in self._segment_events:
                self._segment_events[sid] = threading.Event()
        else:
            with self._stats_lock:
                self.stats["transfers"][sid] = {
                    "filename": meta["filename"],
                    "file_size": meta["file_size"],
                    "total_chunks": meta["total_chunks"],
                    "progress": 0.0,
                    "received": 0,
                    "bytes": 0,
                    "started": time.time(),
                    "completed": False,
                }
            self._add_event(f"Transfer started: {meta['filename']}")

    def _on_file_list_response(self, pkt: Packet, addr, iface):
        data = pkt.json_payload()
        self.file_list = data.get("files", [])
        log.info("Received file list: %d files", len(self.file_list))
        self._file_list_event.set()

    def _on_transfer_complete(self, pkt: Packet, addr, iface):
        sid = pkt.session_id
        reassembler = self.reassemblers.get(sid)
        if not reassembler:
            return  # already handled or unknown session

        # Check if this is a streaming segment completion
        seg_info = self._segment_map.get(sid)
        if seg_info:
            stream_sid, seg_index = seg_info
            stream_session = self.stream_sessions.get(stream_sid)
            if stream_session:
                if reassembler.is_complete:
                    # All chunks received — mark segment as playable
                    stream_session.mark_received(seg_index)
                    log.info("Stream segment %d complete for session %d", seg_index, stream_sid)
                    self._completed_transfers.add(sid)
                    self._segment_map.pop(sid, None)
                    self.reassemblers.pop(sid, None)
                else:
                    # TRANSFER_COMPLETE arrived before all DATA (UDP reordering).
                    # Give late packets a grace period, same as regular files.
                    missing = len(reassembler.missing_chunks())
                    log.info("Segment %d: %d chunks missing, starting grace period",
                             seg_index, missing)
                    threading.Thread(
                        target=self._segment_grace_period,
                        args=(sid, stream_sid, seg_index),
                        daemon=True,
                    ).start()
            else:
                self._segment_map.pop(sid, None)
                self.reassemblers.pop(sid, None)
            return

        # Normal file transfer completion
        if reassembler.is_complete:
            ok = reassembler.verify()
            status = "VERIFIED ✓" if ok else "HASH MISMATCH ✗"
            log.info("Transfer complete: %s — %s", reassembler.filename, status)
            self._add_event(f"Complete: {reassembler.filename} ({status})")
            with self._stats_lock:
                if sid in self.stats["transfers"]:
                    self.stats["transfers"][sid]["completed"] = True
                    self.stats["transfers"][sid]["progress"] = reassembler.progress
            self._completed_transfers.add(sid)
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

    def _segment_grace_period(self, sid: int, stream_sid: int, seg_index: int):
        """Wait for late UDP packets for a streaming segment, then finalize."""
        deadline = time.time() + 2.5
        while time.time() < deadline:
            reassembler = self.reassemblers.get(sid)
            if not reassembler:
                return  # already cleaned up by another path
            if reassembler.is_complete:
                break
            time.sleep(0.1)

        reassembler = self.reassemblers.pop(sid, None)
        self._segment_map.pop(sid, None)
        self._completed_transfers.add(sid)
        if not reassembler:
            return

        stream_session = self.stream_sessions.get(stream_sid)
        if stream_session:
            if reassembler.is_complete:
                stream_session.mark_received(seg_index)
                log.info("Stream segment %d complete (after grace period)", seg_index)
            else:
                still_missing = len(reassembler.missing_chunks())
                log.warning("Stream segment %d incomplete after grace period (%d chunks missing)",
                            seg_index, still_missing)
                stream_session.mark_failed(seg_index)
                try:
                    os.remove(reassembler.output_path)
                except OSError:
                    pass

    def _grace_period_cleanup(self, sid: int, initial_missing: int):
        """Wait up to 2 seconds for late packets, then finalize the transfer."""
        time.sleep(2.0)
        reassembler = self.reassemblers.get(sid)
        if not reassembler:
            return  # already cleaned up

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
        self._completed_transfers.add(sid)
        self.reassemblers.pop(sid, None)

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
    def request_file_list(self):
        """Ask sender for list of shared files."""
        self._file_list_event.clear()
        pkt = Packet(PType.FILE_LIST_REQUEST, seq_num=self._next_seq())
        self._send_ctrl_with_fallback(pkt)
        log.info("Requesting file list from sender...")
        self._file_list_event.wait(timeout=5)
        return self.file_list

    def request_file(self, filename: str):
        """Ask sender to transfer a specific file."""
        request_id = (int(time.time() * 1000) ^ self._next_seq()) & 0xFFFFFFFF
        payload = Packet.make_json_payload({
            "filename": filename,
            "request_id": request_id,
        })
        pkt = Packet(PType.FILE_REQUEST, seq_num=self._next_seq(), payload=payload)
        self._send_ctrl_with_fallback(pkt)
        log.info("Requested file: %s", filename)

    # ── streaming commands ────────────────────────────────
    def request_stream(self, filename: str) -> dict | None:
        """Ask sender to start streaming a video file.
        Returns stream session info dict or None on failure.
        """
        self._stream_meta_event.clear()
        self._stream_meta_data = {}

        payload = Packet.make_json_payload({"filename": filename})
        pkt = Packet(PType.STREAM_REQUEST, seq_num=self._next_seq(), payload=payload)
        self._send_ctrl_with_fallback(pkt)
        log.info("Requesting stream: %s", filename)

        # Wait for STREAM_META from sender. Transcode fallback can take minutes
        # for browser-incompatible videos.
        if not self._stream_meta_event.wait(timeout=config.STREAM_PREPARE_TIMEOUT):
            log.error("Stream request timed out for: %s", filename)
            return None

        return self._stream_meta_data

    def _on_stream_meta(self, pkt: Packet, addr, iface):
        """Handle STREAM_META from sender — create receiver stream session."""
        meta = pkt.json_payload()
        session_id = meta.get("session_id", 0)

        # Ignore duplicate
        if session_id in self.stream_sessions:
            log.info("Duplicate STREAM_META for session %d ignored", session_id)
            return

        session = ReceiverStreamSession(
            session_id=session_id,
            filename=meta.get("filename", ""),
            m3u8_content=meta.get("m3u8", ""),
            metadata=meta,
            cache_base_dir=config.HLS_CACHE_DIR,
            dashboard_port=config.DASHBOARD_PORT,
            buffer_behind=config.HLS_BUFFER_BEHIND,
            buffer_ahead=config.HLS_BUFFER_AHEAD,
        )
        self.stream_sessions[session_id] = session

        with self._stats_lock:
            self.stats["stream_sessions"][session_id] = {
                "filename": meta.get("filename", ""),
                "duration": meta.get("duration", 0),
                "segment_count": meta.get("segment_count", 0),
                "vlc_url": session.vlc_url,
            }

        self._stream_meta_data = {
            "session_id": session_id,
            "filename": meta.get("filename", ""),
            "duration": meta.get("duration", 0),
            "width": meta.get("width", 0),
            "height": meta.get("height", 0),
            "segment_count": meta.get("segment_count", 0),
            "segment_duration": meta.get("segment_duration", 4),
            "vlc_url": session.vlc_url,
        }
        self._stream_meta_event.set()

        self._add_event(f"Stream started: {meta.get('filename', '?')}")
        log.info("Stream session %d: %s (%d segments, VLC: %s)",
                 session_id, meta.get('filename'), meta.get('segment_count', 0), session.vlc_url)

    def _request_segment(self, stream_session_id: int, segment_index: int):
        """Request a specific segment from the sender."""
        session = self.stream_sessions.get(stream_session_id)
        if not session:
            return
        if not session.reserve_segment(segment_index):
            return  # cached, pending, or out of range

        payload = Packet.make_json_payload({
            "stream_session_id": stream_session_id,
            "segment_index": segment_index,
        })
        pkt = Packet(PType.STREAM_SEGMENT_REQUEST, seq_num=self._next_seq(), payload=payload)
        self._send_ctrl_with_fallback(pkt)
        log.debug("Requested segment %d for stream %d", segment_index, stream_session_id)

    def close_stream(self, session_id: int):
        """Close a streaming session."""
        session = self.stream_sessions.pop(session_id, None)
        self._closed_stream_sessions.add(session_id)

        payload = Packet.make_json_payload({"stream_session_id": session_id})
        pkt = Packet(PType.STREAM_CLOSE, seq_num=self._next_seq(), payload=payload)
        self._send_ctrl_with_fallback(pkt)

        cancelled = []
        for transfer_sid, seg_info in list(self._segment_map.items()):
            stream_sid, seg_index = seg_info
            if stream_sid != session_id:
                continue
            cancelled.append((transfer_sid, seg_index))
            self._completed_transfers.add(transfer_sid)
            self._segment_map.pop(transfer_sid, None)
            self.reassemblers.pop(transfer_sid, None)
            event = self._segment_events.pop(transfer_sid, None)
            if event:
                event.set()
            if session:
                session.mark_failed(seg_index)

        if session:
            session.cleanup()
            with self._stats_lock:
                self.stats["stream_sessions"].pop(session_id, None)

        if cancelled:
            log.info("Cancelled %d in-flight segment transfer(s) for stream session %d",
                     len(cancelled), session_id)
        self._add_event(f"Stream closed: session {session_id}")
        log.info("Stream session %d closed", session_id)

    # ── heartbeat monitor ───────────────────────────────────
    def _heartbeat_monitor(self):
        while self.net.running:
            elapsed = time.time() - self.last_hb_time
            hb_timeout = config.HEARTBEAT_INTERVAL * (config.MAX_MISSED_HEARTBEATS + 1)
            if elapsed > hb_timeout and self.lifi_alive:
                self.lifi_alive = False
                self._add_event("LiFi heartbeat lost — expecting failover")
                if self.net.active_interface != "wifi" and self.net.interface_available("wifi"):
                    self.net.switch_to("wifi")
                    self.stats["active_interface"] = "wifi"
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
                if self.path == "/api/status":
                    self._json_response(self.receiver.stats)
                elif self.path == "/api/files":
                    self._json_response({"files": self.receiver.file_list})
                elif self.path == "/api/refresh_files":
                    files = self.receiver.request_file_list()
                    self._json_response({"files": files})
                elif self.path.startswith("/api/stream/"):
                    filename = unquote(self.path[len("/api/stream/"):])
                    self._serve_stream(filename)
                elif self.path.startswith("/api/hls/"):
                    self._serve_hls(self.path)
                elif self.path == "/api/events":
                    self._sse_stream()
                else:
                    super().do_GET()

            def do_POST(self):
                if self.path.startswith("/api/download/"):
                    filename = unquote(self.path[len("/api/download/"):])
                    self.receiver.request_file(filename)
                    self._json_response({"status": "ok", "filename": filename})
                elif self.path.startswith("/api/stream_start/"):
                    filename = unquote(self.path[len("/api/stream_start/"):])
                    self._handle_stream_start(filename)
                elif self.path.startswith("/api/stream_close/"):
                    sid_str = self.path[len("/api/stream_close/"):]
                    try:
                        sid = int(sid_str)
                        self.receiver.close_stream(sid)
                        self._json_response({"status": "ok"})
                    except ValueError:
                        self.send_error(400, "Invalid session ID")
                else:
                    self.send_error(404)

            def _json_response(self, data):
                body = json.dumps(data).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", len(body))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)

            def _handle_stream_start(self, filename):
                """Start a streaming session — runs ffmpeg on sender, waits for metadata."""
                result = self.receiver.request_stream(filename)
                if result:
                    self._json_response(result)
                else:
                    self.send_error(504, "Stream request timed out or failed")

            def _serve_hls(self, path):
                """Serve HLS playlist or segment files for the streaming player.

                Routes:
                    /api/hls/<session_id>/playlist.m3u8
                    /api/hls/<session_id>/seg_NNNNN.ts
                """
                # Parse: /api/hls/<session_id>/<filename>
                parts = path.split("/")
                # parts = ['', 'api', 'hls', '<session_id>', '<filename>']
                if len(parts) < 5:
                    self.send_error(400, "Invalid HLS path")
                    return

                try:
                    session_id = int(parts[3])
                except ValueError:
                    self.send_error(400, "Invalid session ID")
                    return

                filename = unquote(parts[4])
                session = self.receiver.stream_sessions.get(session_id)
                if not session:
                    self.send_error(404, "Stream session not found")
                    return

                if filename == "playlist.m3u8":
                    # Serve the rewritten M3U8 playlist
                    body = session.get_local_playlist().encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/vnd.apple.mpegurl")
                    self.send_header("Content-Length", len(body))
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    self.wfile.write(body)
                    return

                if not filename.endswith(".ts"):
                    self.send_error(400, "Invalid segment filename")
                    return

                # Parse segment index from filename like "seg_00005.ts"
                try:
                    seg_name = filename.replace(".ts", "")
                    seg_index = int(seg_name.split("_")[1])
                except (IndexError, ValueError):
                    self.send_error(400, "Cannot parse segment index")
                    return

                if seg_index < 0 or seg_index >= session.segment_count:
                    self.send_error(404, "Segment out of range")
                    return

                # Check cache — if segment is already downloaded, serve it immediately
                if session.has_segment(seg_index):
                    log.info("HLS: serving cached segment %d", seg_index)
                    self._serve_ts_file(session.get_segment_path(seg_index))
                    # Buffer management: delete old segments
                    session.manage_buffer(seg_index)
                    # Prefetch next segments in background
                    self._prefetch_segments(session_id, seg_index)
                    return

                # Cache miss — request segment from sender and wait
                log.info("HLS: segment %d cache miss, requesting from sender", seg_index)
                self.receiver._request_segment(session_id, seg_index)

                # Also prefetch next segments
                self._prefetch_segments(session_id, seg_index)

                # Poll for segment arrival. We use a polling loop instead of
                # a single event.wait() to avoid a race condition where
                # _on_file_meta overwrites the event object we're waiting on.
                timeout = config.STREAM_SEGMENT_TIMEOUT
                deadline = time.time() + timeout
                served = False
                while time.time() < deadline:
                    # Check if segment has been cached (transfer completed)
                    if session.has_segment(seg_index):
                        log.info("HLS: segment %d ready, serving", seg_index)
                        session.manage_buffer(seg_index)
                        self._serve_ts_file(session.get_segment_path(seg_index))
                        served = True
                        break
                    # Brief sleep to avoid busy-waiting
                    time.sleep(0.3)

                if not served:
                    log.warning("HLS: segment %d timed out (%ds)", seg_index, timeout)
                    self.send_error(504, f"Segment {seg_index} timed out ({timeout}s)")

            def _serve_ts_file(self, filepath):
                """Serve a .ts segment file."""
                if not os.path.exists(filepath):
                    self.send_error(404, "Segment file not found")
                    return

                file_size = os.path.getsize(filepath)
                self.send_response(200)
                self.send_header("Content-Type", "video/mp2t")
                self.send_header("Content-Length", file_size)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "max-age=3600")
                self.end_headers()

                with open(filepath, "rb") as f:
                    remaining = file_size
                    while remaining > 0:
                        chunk = f.read(min(65536, remaining))
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        remaining -= len(chunk)

            def _prefetch_segments(self, session_id, current_segment):
                """Request upcoming segments in background threads."""
                session = self.receiver.stream_sessions.get(session_id)
                if not session:
                    return
                available_slots = config.HLS_MAX_PENDING_SEGMENTS - session.pending_count
                if available_slots <= 0:
                    return
                to_prefetch = session.segments_to_prefetch(current_segment)
                limit = min(config.HLS_PREFETCH_BATCH, available_slots)
                for seg_idx in to_prefetch[:limit]:
                    threading.Thread(
                        target=self.receiver._request_segment,
                        args=(session_id, seg_idx),
                        daemon=True,
                    ).start()

            def _serve_stream(self, filename):
                """Serve a file from received folder (supports Range requests for video)."""
                filepath = os.path.join(config.RECEIVE_FOLDER, filename)

                # Path traversal protection
                root = os.path.abspath(config.RECEIVE_FOLDER)
                resolved = os.path.abspath(filepath)
                if not resolved.startswith(root + os.sep) and resolved != root:
                    self.send_error(403, "Path traversal rejected")
                    return

                if not os.path.exists(filepath):
                    self.send_error(404, "File not found")
                    return

                file_size = os.path.getsize(filepath)
                content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
                available_size = file_size
                is_complete = True
                for reassembler in self.receiver.reassemblers.values():
                    if reassembler.filename == filename:
                        available_size = reassembler.contiguous_bytes
                        is_complete = reassembler.is_complete
                        break
                if not is_complete and available_size <= 0:
                    self.send_error(503, "File transfer in progress; no bytes available yet")
                    return

                range_header = self.headers.get("Range")
                if range_header:
                    # Parse Range: bytes=start-end
                    range_str = range_header.replace("bytes=", "")
                    parts = range_str.split("-")
                    start = int(parts[0]) if parts[0] else 0
                    end = int(parts[1]) if parts[1] else file_size - 1
                    max_end = (file_size - 1) if is_complete else (available_size - 1)
                    if start > max_end:
                        self.send_response(416)
                        self.send_header("Content-Range", f"bytes */{file_size}")
                        self.send_header("Accept-Ranges", "bytes")
                        self.send_header("Access-Control-Allow-Origin", "*")
                        self.end_headers()
                        return
                    end = min(end, max_end)
                    length = end - start + 1

                    self.send_response(206)
                    self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
                    self.send_header("Content-Length", length)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Accept-Ranges", "bytes")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()

                    with open(filepath, "rb") as f:
                        f.seek(start)
                        self.wfile.write(f.read(length))
                else:
                    # For in-progress files, only expose contiguous bytes so players can stream
                    # without reading zero-filled preallocated tail data.
                    if is_complete:
                        self.send_response(200)
                        body_len = file_size
                    else:
                        self.send_response(206)
                        body_len = available_size
                        self.send_header("Content-Range", f"bytes 0-{max(0, body_len - 1)}/{file_size}")
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", body_len)
                    self.send_header("Accept-Ranges", "bytes")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()

                    with open(filepath, "rb") as f:
                        remaining = body_len
                        while remaining > 0:
                            chunk = f.read(min(65536, remaining))
                            if not chunk:
                                break
                            self.wfile.write(chunk)
                            remaining -= len(chunk)

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

            def handle_error(self, request, client_address):
                """Suppress noisy but harmless connection aborts.

                Browsers often open speculative connections and close them
                before sending a request (preflight, favicon, etc.).
                """
                import sys
                exc_type = sys.exc_info()[0]
                if exc_type in (ConnectionAbortedError, ConnectionResetError,
                                BrokenPipeError, OSError):
                    return  # silently ignore
                super().handle_error(request, client_address)

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
