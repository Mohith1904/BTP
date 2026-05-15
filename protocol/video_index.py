"""Timestamp to chunk mapping for video streams using ffprobe."""

from __future__ import annotations

import bisect
import json
import logging
import os
import shutil
import subprocess

log = logging.getLogger("video_index")


class VideoStreamIndex:
    """Map playback timestamps to file chunk ranges."""

    def __init__(
        self,
        duration: float,
        points: list[tuple[float, int]],
        file_size: int,
        chunk_size: int,
        source: str,
    ):
        self.duration = duration
        self.points = sorted(points)
        self.times = [p[0] for p in self.points]
        self.file_size = file_size
        self.chunk_size = chunk_size
        self.total_chunks = max(1, (file_size + chunk_size - 1) // chunk_size)
        self.source = source

    @classmethod
    def build(cls, filepath: str, chunk_size: int, ffprobe_bin: str = "ffprobe") -> "VideoStreamIndex":
        file_size = os.path.getsize(filepath)
        duration = _probe_duration(filepath, ffprobe_bin)
        points = _probe_packet_positions(filepath, ffprobe_bin)
        source = "ffprobe"

        if duration <= 0 or not points:
            duration = max(duration, 1.0)
            points = _fallback_points(duration, file_size, chunk_size)
            source = "estimated"

        return cls(duration, points, file_size, chunk_size, source)

    def chunks_for_time_range(self, start_time: float, seconds: float) -> tuple[int, int]:
        """Return inclusive chunk range for [start_time, start_time + seconds]."""
        start_time = max(0.0, min(float(start_time), self.duration))
        end_time = max(start_time, min(start_time + max(0.5, float(seconds)), self.duration))

        start_byte = self._byte_at_time(start_time)
        end_byte = self._byte_at_time(end_time)
        if end_byte <= start_byte:
            end_byte = min(self.file_size - 1, start_byte + self.chunk_size)

        start_chunk = max(0, min(self.total_chunks - 1, start_byte // self.chunk_size))
        end_chunk = max(start_chunk, min(self.total_chunks - 1, end_byte // self.chunk_size))
        return start_chunk, end_chunk

    def _byte_at_time(self, timestamp: float) -> int:
        if not self.points:
            ratio = 0 if self.duration <= 0 else timestamp / self.duration
            return int(max(0, min(self.file_size - 1, ratio * self.file_size)))

        idx = bisect.bisect_right(self.times, timestamp) - 1
        if idx < 0:
            return self.points[0][1]
        return max(0, min(self.file_size - 1, self.points[idx][1]))

    def to_json(self) -> dict:
        return {
            "duration": self.duration,
            "index_source": self.source,
            "total_chunks": self.total_chunks,
        }


def ffprobe_available(ffprobe_bin: str = "ffprobe") -> bool:
    return shutil.which(ffprobe_bin) is not None


def _run_ffprobe(args: list[str]) -> dict:
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            check=True,
        )
        return json.loads(result.stdout or "{}")
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as e:
        log.warning("ffprobe failed: %s", e)
        return {}


def _probe_duration(filepath: str, ffprobe_bin: str) -> float:
    data = _run_ffprobe([
        ffprobe_bin,
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "json",
        filepath,
    ])
    try:
        return float(data.get("format", {}).get("duration", 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def _probe_packet_positions(filepath: str, ffprobe_bin: str) -> list[tuple[float, int]]:
    data = _run_ffprobe([
        ffprobe_bin,
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "packet=pts_time,pos",
        "-of", "json",
        filepath,
    ])
    points: list[tuple[float, int]] = []
    for packet in data.get("packets", []):
        try:
            pts = float(packet.get("pts_time"))
            pos = int(packet.get("pos"))
        except (TypeError, ValueError):
            continue
        if pts >= 0 and pos >= 0:
            points.append((pts, pos))
    return points


def _fallback_points(duration: float, file_size: int, chunk_size: int) -> list[tuple[float, int]]:
    total_chunks = max(1, (file_size + chunk_size - 1) // chunk_size)
    points = []
    for chunk_id in range(total_chunks):
        ratio = chunk_id / total_chunks
        points.append((ratio * duration, chunk_id * chunk_size))
    return points
