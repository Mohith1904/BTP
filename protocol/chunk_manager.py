"""
File chunking (sender) and reassembly (receiver).
Supports progressive writes so video can stream while still transferring.
"""

import os
import hashlib
import threading


class FileChunker:
    """Splits a file into numbered chunks for transmission."""

    def __init__(
        self,
        filepath: str,
        chunk_size: int = 60 * 1024,
        transfer_name: str | None = None,
    ):
        self.filepath = filepath
        self.chunk_size = chunk_size
        self.file_size = os.path.getsize(filepath)
        self.total_chunks = max(1, (self.file_size + chunk_size - 1) // chunk_size)
        self.transfer_name = transfer_name
        self._hash: str | None = None

    @property
    def filename(self) -> str:
        if self.transfer_name:
            return self.transfer_name
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
        output_path: str | None = None,
        preallocate: bool = True,
    ):
        self.filename = filename
        self.file_size = file_size
        self.total_chunks = total_chunks
        self.chunk_size = chunk_size
        self.expected_hash = file_hash
        self.output_dir = output_dir
        self.output_path = output_path or os.path.join(output_dir, filename)

        self._received: set[int] = set()
        self._lock = threading.Lock()
        self._bytes_written = 0
        self._contiguous_next = 0

        # Create parent directories (handles nested filenames like "subdir/video.mp4")
        parent_dir = os.path.dirname(self.output_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        else:
            os.makedirs(output_dir, exist_ok=True)
        with open(self.output_path, "wb") as f:
            if preallocate:
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
            return True

    @property
    def received_count(self) -> int:
        return len(self._received)

    @property
    def progress(self) -> float:
        if self.total_chunks == 0:
            return 1.0
        return len(self._received) / self.total_chunks

    @property
    def is_complete(self) -> bool:
        return len(self._received) >= self.total_chunks

    @property
    def bytes_written(self) -> int:
        return self._bytes_written

    @property
    def contiguous_chunks(self) -> int:
        return self._contiguous_next

    @property
    def contiguous_bytes(self) -> int:
        if self._contiguous_next >= self.total_chunks:
            return self.file_size
        return min(self.file_size, self._contiguous_next * self.chunk_size)

    def has_chunk_range(self, start_chunk: int, end_chunk: int) -> bool:
        """Return True if every chunk in the inclusive range is available."""
        with self._lock:
            return all(i in self._received for i in range(start_chunk, end_chunk + 1))

    def has_byte_range(self, start: int, end: int) -> bool:
        """Return True if every chunk needed for byte range [start, end] exists."""
        if start < 0 or end < start:
            return False
        start_chunk = start // self.chunk_size
        end_chunk = min(self.total_chunks - 1, end // self.chunk_size)
        return self.has_chunk_range(start_chunk, end_chunk)

    def available_bytes_from(self, start: int, requested_end: int) -> int:
        """Return readable bytes from start before the first missing chunk."""
        if start < 0 or start >= self.file_size:
            return 0

        start_chunk = start // self.chunk_size
        requested_end = min(self.file_size - 1, requested_end)
        end_chunk = min(self.total_chunks - 1, requested_end // self.chunk_size)
        with self._lock:
            chunk = start_chunk
            while chunk <= end_chunk and chunk in self._received:
                chunk += 1

        if chunk == start_chunk:
            return 0
        available_end = min(self.file_size - 1, chunk * self.chunk_size - 1, requested_end)
        return max(0, available_end - start + 1)

    def missing_chunks(self) -> list[int]:
        """Return list of chunk IDs not yet received."""
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
