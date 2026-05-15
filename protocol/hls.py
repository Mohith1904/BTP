"""Sender-side HLS preparation for on-demand video streaming."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import subprocess

import config


@dataclass(frozen=True)
class HLSManifest:
    playlist_path: Path
    playlist_text: str
    segments: list[str]


class HLSManager:
    def __init__(self, cache_dir: str | Path | None = None):
        self.cache_dir = Path(cache_dir or config.HLS_CACHE_FOLDER)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def prepare(self, video_path: str | Path) -> HLSManifest:
        source = Path(video_path).resolve()
        if not source.exists():
            raise FileNotFoundError(source)

        output_dir = self._output_dir_for(source)
        playlist = output_dir / "playlist.m3u8"
        if not playlist.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
            self._run_ffmpeg(source, output_dir, playlist)

        playlist_text = playlist.read_text(encoding="utf-8")
        segments = [
            line.strip()
            for line in playlist_text.splitlines()
            if line.strip() and not line.startswith("#")
        ]
        return HLSManifest(playlist, playlist_text, segments)

    def segment_path(self, video_path: str | Path, segment_name: str) -> Path:
        manifest = self.prepare(video_path)
        safe_name = Path(segment_name).name
        allowed = {Path(segment).name for segment in manifest.segments}
        if safe_name not in allowed:
            raise FileNotFoundError(f"segment is not in playlist: {segment_name}")

        segment_path = manifest.playlist_path.parent / safe_name
        if not segment_path.exists():
            raise FileNotFoundError(segment_path)
        return segment_path

    def _output_dir_for(self, source: Path) -> Path:
        stat = source.stat()
        identity = f"{source}:{stat.st_size}:{stat.st_mtime_ns}"
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
        return self.cache_dir / digest

    def _run_ffmpeg(self, source: Path, output_dir: Path, playlist: Path) -> None:
        segment_pattern = output_dir / "segment_%05d.ts"
        command = [
            config.FFMPEG_BIN,
            "-y",
            "-i",
            str(source),
            "-c",
            "copy",
            "-hls_time",
            str(config.HLS_TIME_SECONDS),
            "-hls_list_size",
            "0",
            "-hls_segment_filename",
            str(segment_pattern),
            "-f",
            "hls",
            str(playlist),
        ]
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode != 0:
            raise RuntimeError(
                "ffmpeg failed while preparing HLS assets:\n"
                f"{result.stderr.strip()}"
            )

