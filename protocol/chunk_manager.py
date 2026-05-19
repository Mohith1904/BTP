"""
File chunking (sender) and reassembly (receiver).
Supports progressive writes so video can stream while still transferring.
"""

import os
import hashlib
import threading


class FileChunker:
    """Splits a file into numbered chunks for transmission."""

    def __init__(self, filepath: str, chunk_size: int = 60 * 1024):
        self.filepath = filepath
        self.chunk_size = chunk_size
        self.file_size = os.path.getsize(filepath)
        self.total_chunks = max(1, (self.file_size + chunk_size - 1) // chunk_size)
        self._hash: str | None = None

    @property
    def filename(self) -> str:
        return os.path.basename(self.filepath)

    def file_hash(self) -> str:
        """SHA-256 of the whole file (cached)."""
        if self._hash is None:
            h = hashlib.sha256()
            with open(self.filepath, "rb") as f:
                for block in iter(lambda: f.read(65536), b""):
                    h.update(block)
            self._hash = h.hexdigest()
        return self._hash

    def get_chunk(self, chunk_id: int) -> bytes:
        with open(self.filepath, "rb") as f:
            f.seek(chunk_id * self.chunk_size)
            return f.read(self.chunk_size)

    def get_range(self, offset: int, length: int) -> bytes:
        with open(self.filepath, "rb") as f:
            f.seek(offset)
            return f.read(length)

    def metadata(self) -> dict:
        return {
            "filename": self.filename,
            "file_size": self.file_size,
            "total_chunks": self.total_chunks,
            "chunk_size": self.chunk_size,
            "file_hash": self.file_hash(),
        }


class ChunkReassembler:
    """Receives chunks (possibly out of order) and writes them to disk."""

    def __init__(
        self,
        filename: str,
        file_size: int,
        total_chunks: int,
        chunk_size: int,
        file_hash: str,
        output_dir: str = "./received",
        byte_range_mode: bool = False,
    ):
        self.filename = filename
        self.file_size = file_size
        self.total_chunks = total_chunks
        self.chunk_size = chunk_size
        self.expected_hash = file_hash
        self.output_dir = output_dir
        self.output_path = os.path.join(output_dir, filename)
        self.byte_range_mode = byte_range_mode

        self._received: set[int] = set()
        self._ranges: list[tuple[int, int]] = []
        self._lock = threading.Lock()
        self._bytes_written = 0
        self._contiguous_next = 0
        self._packet_count = 0

        # Create parent directories (handles nested filenames like "subdir/video.mp4")
        parent_dir = os.path.dirname(self.output_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        else:
            os.makedirs(output_dir, exist_ok=True)
        # Pre-allocate the file
        with open(self.output_path, "wb") as f:
            f.truncate(file_size)

    def add_chunk(self, chunk_id: int, data: bytes) -> bool:
        """Write chunk to correct offset. Returns True if new, False if dup."""
        with self._lock:
            if chunk_id in self._received:
                return False
            with open(self.output_path, "r+b") as f:
                f.seek(chunk_id * self.chunk_size)
                f.write(data)
            self._received.add(chunk_id)
            self._bytes_written += len(data)
            while self._contiguous_next in self._received:
                self._contiguous_next += 1
            self._packet_count += 1
            return True

    def add_range(self, offset: int, data: bytes) -> bool:
        """Write a byte range. Returns True when new bytes were added."""
        if not data:
            return False

        end = min(self.file_size, offset + len(data))
        if offset < 0 or offset >= self.file_size or end <= offset:
            return False

        with self._lock:
            before = self._covered_bytes_unlocked()
            with open(self.output_path, "r+b") as f:
                f.seek(offset)
                f.write(data[: end - offset])

            self._add_range_unlocked(offset, end)
            after = self._covered_bytes_unlocked()
            added = after - before
            if added > 0:
                self._bytes_written += added
                self._packet_count += 1
                return True
            return False

    def _add_range_unlocked(self, start: int, end: int):
        self._ranges.append((start, end))
        self._ranges.sort()
        merged: list[tuple[int, int]] = []
        for s, e in self._ranges:
            if not merged or s > merged[-1][1]:
                merged.append((s, e))
            else:
                merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        self._ranges = merged

    def _covered_bytes_unlocked(self) -> int:
        return sum(end - start for start, end in self._ranges)

    def _range_covered_unlocked(self, start: int, end: int) -> bool:
        if start >= end:
            return True
        return any(s <= start and e >= end for s, e in self._ranges)

    @property
    def received_count(self) -> int:
        if self.byte_range_mode:
            return self._packet_count
        return len(self._received)

    @property
    def progress(self) -> float:
        if self.file_size == 0:
            return 1.0
        if self.byte_range_mode:
            return min(1.0, self._bytes_written / self.file_size)
        if self.total_chunks == 0:
            return 1.0
        return len(self._received) / self.total_chunks

    @property
    def is_complete(self) -> bool:
        if self.byte_range_mode:
            with self._lock:
                return self._range_covered_unlocked(0, self.file_size)
        return len(self._received) >= self.total_chunks

    @property
    def bytes_written(self) -> int:
        return self._bytes_written

    @property
    def contiguous_chunks(self) -> int:
        return self._contiguous_next

    @property
    def contiguous_bytes(self) -> int:
        if self.byte_range_mode:
            with self._lock:
                if not self._ranges or self._ranges[0][0] > 0:
                    return 0
                return min(self.file_size, self._ranges[0][1])
        if self._contiguous_next >= self.total_chunks:
            return self.file_size
        return min(self.file_size, self._contiguous_next * self.chunk_size)

    def missing_chunks(self) -> list[int]:
        """Return list of chunk IDs not yet received."""
        if self.byte_range_mode:
            missing = []
            with self._lock:
                for i in range(self.total_chunks):
                    start = i * self.chunk_size
                    end = min(self.file_size, start + self.chunk_size)
                    if not self._range_covered_unlocked(start, end):
                        missing.append(i)
            return missing
        return [i for i in range(self.total_chunks) if i not in self._received]

    def verify(self) -> bool:
        """Verify SHA-256 of completed file."""
        if not self.is_complete:
            return False
        h = hashlib.sha256()
        with open(self.output_path, "rb") as f:
            for block in iter(lambda: f.read(65536), b""):
                h.update(block)
        return h.hexdigest() == self.expected_hash
