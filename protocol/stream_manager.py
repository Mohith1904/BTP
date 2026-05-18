"""
HLS streaming session management.

SenderStreamSession  — runs ffmpeg to segment video, serves segment files
ReceiverStreamSession — caches segments, rewrites playlist, manages buffer
StreamProbe          — extracts video metadata via ffprobe
"""

import os
import json
import shutil
import subprocess
import logging
import re
import tempfile
import threading

import config

log = logging.getLogger("stream")


class StreamProbe:
    """Extract video metadata using ffprobe."""

    def __init__(self, ffprobe_path: str = "ffprobe"):
        self.ffprobe_path = ffprobe_path

    def probe(self, filepath: str) -> dict:
        """Return video metadata: duration, resolution, video_codec, audio_codec."""
        cmd = [
            self.ffprobe_path,
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            filepath,
        ]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                log.error("ffprobe failed: %s", result.stderr)
                return {}
            data = json.loads(result.stdout)
        except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError) as e:
            log.error("ffprobe error: %s", e)
            return {}

        # Extract info
        info = {
            "duration": 0.0,
            "width": 0,
            "height": 0,
            "video_codec": "",
            "audio_codec": "",
        }

        fmt = data.get("format", {})
        info["duration"] = float(fmt.get("duration", 0))

        for stream in data.get("streams", []):
            if stream.get("codec_type") == "video" and not info["video_codec"]:
                info["video_codec"] = stream.get("codec_name", "")
                info["width"] = int(stream.get("width", 0))
                info["height"] = int(stream.get("height", 0))
            elif stream.get("codec_type") == "audio" and not info["audio_codec"]:
                info["audio_codec"] = stream.get("codec_name", "")

        return info


class SenderStreamSession:
    """Sender-side HLS streaming session.

    Creates HLS segments from a video file using ffmpeg, then serves
    individual segment files on demand.
    """

    def __init__(
        self,
        session_id: int,
        filepath: str,
        hls_cache_dir: str,
        ffmpeg_path: str = "ffmpeg",
        ffprobe_path: str = "ffprobe",
        segment_duration: int = 4,
    ):
        self.session_id = session_id
        self.filepath = filepath
        self.filename = os.path.basename(filepath)
        self.ffmpeg_path = ffmpeg_path
        self.ffprobe_path = ffprobe_path
        self.segment_duration = segment_duration

        # Create a temp directory for this session's HLS files
        self.hls_dir = os.path.join(hls_cache_dir, f"sender_{session_id}")
        os.makedirs(self.hls_dir, exist_ok=True)

        self.segment_count = 0
        self.m3u8_content = ""
        self.metadata: dict = {}
        self._prepared = False

        # Probe video metadata
        self._probe = StreamProbe(ffprobe_path)

    def prepare_hls(self) -> bool:
        """Run ffmpeg to segment the video into HLS format.

        Returns True on success, False on failure.
        Uses -codec copy first for speed, then falls back to H.264/AAC
        if the generated fragments are not browser-playable.
        """
        if not self._run_hls_segmentation(mode="copy"):
            return False

        if not self._hls_is_browser_playable():
            if not config.HLS_TRANSCODE_FALLBACK:
                log.error("HLS fragments are not browser-playable; enable HLS_TRANSCODE_FALLBACK")
                return False
            log.warning("HLS copy output is not browser-playable; transcoding %s to H.264/AAC",
                        self.filename)
            self._clear_hls_outputs()
            if not self._run_hls_segmentation(mode="transcode"):
                return False
            if not self._hls_is_browser_playable():
                log.error("HLS transcode output is still not browser-playable for: %s",
                          self.filename)
                return False

        playlist_path = os.path.join(self.hls_dir, "playlist.m3u8")

        with open(playlist_path, "r", encoding="utf-8") as f:
            self.m3u8_content = f.read()

        # Count segments
        self.segment_count = len([
            f for f in os.listdir(self.hls_dir)
            if f.startswith("seg_") and f.endswith(".ts")
        ])

        # Get video metadata
        self.metadata = self._probe.probe(self.filepath)
        self.metadata["segment_count"] = self.segment_count
        self.metadata["segment_duration"] = self.segment_duration

        self._prepared = True
        log.info("HLS ready: %s → %d segments (%.1fs each)",
                 self.filename, self.segment_count, self.segment_duration)
        return True

    def _run_hls_segmentation(self, mode: str) -> bool:
        """Create HLS playlist/segments using either copy or transcode mode."""
        playlist_path = os.path.join(self.hls_dir, "playlist.m3u8")
        segment_pattern = os.path.join(self.hls_dir, "seg_%05d.ts")

        cmd = [
            self.ffmpeg_path,
            "-hide_banner",
            "-i", self.filepath,
            "-map", "0:v:0",
            "-map", "0:a:0?",
        ]
        if mode == "copy":
            cmd.extend(["-c", "copy"])
        else:
            cmd.extend([
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-crf", "23",
                "-pix_fmt", "yuv420p",
                "-profile:v", "main",
                "-sc_threshold", "0",
                "-force_key_frames", f"expr:gte(t,n_forced*{self.segment_duration})",
                "-c:a", "aac",
                "-b:a", "128k",
                "-ac", "2",
            ])

        cmd.extend([
            "-start_number", "0",
            "-hls_time", str(self.segment_duration),
            "-hls_list_size", "0",
            "-hls_playlist_type", "vod",
            "-hls_flags", "independent_segments",
            "-hls_segment_filename", segment_pattern,
            "-f", "hls",
            "-y",
            playlist_path,
        ])

        log.info("Running ffmpeg HLS segmentation (%s) for: %s", mode, self.filename)
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600,
            )
        except subprocess.TimeoutExpired:
            log.error("ffmpeg timed out after 600s for: %s", self.filename)
            return False
        except FileNotFoundError:
            log.error("ffmpeg not found at: %s", self.ffmpeg_path)
            return False

        if result.returncode != 0:
            log.error("ffmpeg HLS %s failed (returncode=%d): %s",
                      mode, result.returncode,
                      result.stderr[-800:] if result.stderr else "")
            return False

        if not os.path.isfile(playlist_path):
            log.error("Playlist not created: %s", playlist_path)
            return False
        return True

    def _clear_hls_outputs(self):
        """Remove generated playlist and segment files before retrying."""
        for name in os.listdir(self.hls_dir):
            if name == "playlist.m3u8" or (name.startswith("seg_") and name.endswith(".ts")):
                try:
                    os.remove(os.path.join(self.hls_dir, name))
                except OSError:
                    pass

    def _hls_is_browser_playable(self) -> bool:
        """Validate that the first segment contains browser-compatible media."""
        first_segment = self.get_segment_path(0)
        if not first_segment:
            log.error("HLS validation failed: first segment is missing")
            return False

        cmd = [
            self.ffprobe_path,
            "-v", "error",
            "-show_streams",
            "-of", "json",
            first_segment,
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                log.error("ffprobe HLS validation failed: %s", result.stderr[-500:])
                return False
            data = json.loads(result.stdout)
        except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError) as e:
            log.error("ffprobe HLS validation error: %s", e)
            return False

        streams = data.get("streams", [])
        video_codecs = [
            s.get("codec_name", "")
            for s in streams
            if s.get("codec_type") == "video"
        ]
        audio_codecs = [
            s.get("codec_name", "")
            for s in streams
            if s.get("codec_type") == "audio"
        ]
        if not video_codecs:
            log.warning("HLS validation: no video stream found in first segment")
            return False
        if video_codecs[0] != "h264":
            log.warning("HLS validation: video codec %s is not browser-safe H.264",
                        video_codecs[0])
            return False
        unsupported_audio = [c for c in audio_codecs if c not in {"aac", "mp3"}]
        if unsupported_audio:
            log.warning("HLS validation: unsupported audio codec(s): %s",
                        ", ".join(unsupported_audio))
            return False
        return True

    @property
    def is_prepared(self) -> bool:
        return self._prepared

    def get_segment_path(self, index: int) -> str | None:
        """Return the file path for segment N, or None if it doesn't exist."""
        path = os.path.join(self.hls_dir, f"seg_{index:05d}.ts")
        if os.path.isfile(path):
            return path
        return None

    def get_segment_size(self, index: int) -> int:
        """Return the file size of segment N in bytes."""
        path = self.get_segment_path(index)
        if path:
            return os.path.getsize(path)
        return 0

    def get_stream_meta_payload(self) -> dict:
        """Return the metadata dict to send in STREAM_META packet."""
        return {
            "filename": self.filename,
            "session_id": self.session_id,
            "duration": self.metadata.get("duration", 0),
            "width": self.metadata.get("width", 0),
            "height": self.metadata.get("height", 0),
            "video_codec": self.metadata.get("video_codec", ""),
            "audio_codec": self.metadata.get("audio_codec", ""),
            "segment_count": self.segment_count,
            "segment_duration": self.segment_duration,
            "m3u8": self.m3u8_content,
        }

    def cleanup(self):
        """Delete all HLS temp files for this session."""
        try:
            shutil.rmtree(self.hls_dir, ignore_errors=True)
            log.info("Cleaned up HLS session %d (%s)", self.session_id, self.filename)
        except Exception as e:
            log.error("Cleanup error for session %d: %s", self.session_id, e)


class ReceiverStreamSession:
    """Receiver-side HLS streaming session.

    Caches downloaded .ts segments, rewrites the M3U8 playlist to point
    to local HTTP URLs, and manages the buffer window.
    """

    def __init__(
        self,
        session_id: int,
        filename: str,
        m3u8_content: str,
        metadata: dict,
        cache_base_dir: str,
        dashboard_port: int,
        buffer_behind: int = 7,
        buffer_ahead: int = 17,
    ):
        self.session_id = session_id
        self.filename = filename
        self.original_m3u8 = m3u8_content
        self.metadata = metadata
        self.segment_count = metadata.get("segment_count", 0)
        self.segment_duration = metadata.get("segment_duration", 4)
        self.duration = metadata.get("duration", 0)
        self.buffer_behind = buffer_behind
        self.buffer_ahead = buffer_ahead
        self.dashboard_port = dashboard_port

        # Cache directory for this session's segments
        self.cache_dir = os.path.join(cache_base_dir, str(session_id))
        os.makedirs(self.cache_dir, exist_ok=True)

        # Track which segments we have
        self._cached_segments: set[int] = set()
        # Track segments currently being fetched (to avoid duplicate requests)
        self._pending_segments: set[int] = set()
        self._current_segment = 0
        self._lock = threading.Lock()

    def get_local_playlist(self) -> str:
        """Rewrite the M3U8 playlist so segment URLs point to localhost."""
        # Replace segment filenames with local HTTP URLs
        base_url = f"/api/hls/{self.session_id}"
        lines = self.original_m3u8.splitlines()
        rewritten = []
        for line in lines:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                # This is a segment filename like "seg_00000.ts"
                seg_name = stripped
                rewritten.append(f"{base_url}/{seg_name}")
            else:
                rewritten.append(line)
        return "\n".join(rewritten)

    def has_segment(self, index: int) -> bool:
        """Check if a segment is cached locally."""
        with self._lock:
            return index in self._cached_segments

    def is_pending(self, index: int) -> bool:
        """Check if a segment is currently being fetched."""
        with self._lock:
            return index in self._pending_segments

    def mark_pending(self, index: int):
        """Mark a segment as being fetched."""
        with self._lock:
            self._pending_segments.add(index)

    def reserve_segment(self, index: int) -> bool:
        """Atomically reserve a segment for fetching."""
        with self._lock:
            if index < 0 or index >= self.segment_count:
                return False
            if index in self._cached_segments or index in self._pending_segments:
                return False
            self._pending_segments.add(index)
            return True

    def mark_received(self, index: int):
        """Mark a segment as received and cached."""
        with self._lock:
            self._cached_segments.add(index)
            self._pending_segments.discard(index)

    def mark_failed(self, index: int):
        """Clear pending/cache state for a failed segment transfer."""
        with self._lock:
            self._pending_segments.discard(index)
            self._cached_segments.discard(index)

    def get_segment_path(self, index: int) -> str:
        """Return the local cache path for a segment."""
        return os.path.join(self.cache_dir, f"seg_{index:05d}.ts")

    def save_segment(self, index: int, data: bytes):
        """Write segment data to cache."""
        path = self.get_segment_path(index)
        with open(path, "wb") as f:
            f.write(data)
        self.mark_received(index)
        log.debug("Cached segment %d for session %d (%d bytes)",
                  index, self.session_id, len(data))

    def manage_buffer(self, current_segment: int):
        """Delete segments outside the buffer window.

        Keeps segments in range [current - buffer_behind, current + buffer_ahead].
        """
        self._current_segment = current_segment
        keep_start = max(0, current_segment - self.buffer_behind)
        keep_end = min(self.segment_count - 1, current_segment + self.buffer_ahead)

        with self._lock:
            to_delete = []
            for seg_idx in list(self._cached_segments):
                if seg_idx < keep_start or seg_idx > keep_end:
                    to_delete.append(seg_idx)

        for seg_idx in to_delete:
            path = self.get_segment_path(seg_idx)
            try:
                if os.path.exists(path):
                    os.remove(path)
                with self._lock:
                    self._cached_segments.discard(seg_idx)
            except OSError as e:
                log.warning("Failed to delete segment %d: %s", seg_idx, e)

        if to_delete:
            log.debug("Buffer cleanup: deleted %d segments, keeping [%d..%d]",
                      len(to_delete), keep_start, keep_end)

    def segments_to_prefetch(self, current_segment: int) -> list[int]:
        """Return segment indices ahead of the current playback position."""
        result = []
        start = current_segment + 1
        end = min(self.segment_count, current_segment + self.buffer_ahead + 1)
        with self._lock:
            for i in range(start, end):
                if i not in self._cached_segments and i not in self._pending_segments:
                    result.append(i)
        return result

    @property
    def vlc_url(self) -> str:
        """The URL to paste into VLC for playback."""
        return f"http://localhost:{self.dashboard_port}/api/hls/{self.session_id}/playlist.m3u8"

    @property
    def cached_count(self) -> int:
        with self._lock:
            return len(self._cached_segments)

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending_segments)

    def cleanup(self):
        """Delete all cached segments for this session."""
        try:
            shutil.rmtree(self.cache_dir, ignore_errors=True)
            log.info("Cleaned up stream cache for session %d (%s)",
                     self.session_id, self.filename)
        except Exception as e:
            log.error("Stream cache cleanup error for session %d: %s",
                      self.session_id, e)
