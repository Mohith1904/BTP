import threading


class StreamBuffer:
    def __init__(
        self,
        filename: str,
        file_size: int,
        total_chunks: int,
        chunk_size: int,
        output_dir: str,
        session_id: int,
    ):
        self.filename = filename
        self.file_size = file_size
        self.total_chunks = total_chunks
        self.chunk_size = chunk_size
        self.session_id = session_id

        self._received = set()
        self._chunk_cache = {}
        self._bytes_written = 0

        self._lock = threading.Lock()

    def add_chunk(self, chunk_id: int, data: bytes) -> bool:
        if chunk_id < 0 or chunk_id >= self.total_chunks:
            return False

        with self._lock:
            if chunk_id in self._received:
                return False

            self._chunk_cache[chunk_id] = data
            self._received.add(chunk_id)
            self._bytes_written += len(data)

            return True

    def has_chunk_range(self, start_chunk: int, end_chunk: int) -> bool:
        start_chunk = max(0, start_chunk)
        end_chunk = min(self.total_chunks - 1, end_chunk)

        with self._lock:
            for i in range(start_chunk, end_chunk + 1):
                if i not in self._received:
                    return False

        return True

    def missing_chunks(self, start_chunk: int, end_chunk: int):
        start_chunk = max(0, start_chunk)
        end_chunk = min(self.total_chunks - 1, end_chunk)

        with self._lock:
            return [
                i for i in range(start_chunk, end_chunk + 1)
                if i not in self._received
            ]

    def has_byte_range(self, start: int, end: int) -> bool:
        if start < 0 or end < start or start >= self.file_size:
            return False

        end = min(end, self.file_size - 1)

        start_chunk = start // self.chunk_size
        end_chunk = end // self.chunk_size

        return self.has_chunk_range(start_chunk, end_chunk)

    def missing_chunks_for_byte_range(self, start: int, end: int):
        if start < 0 or end < start or start >= self.file_size:
            return []

        end = min(end, self.file_size - 1)

        start_chunk = start // self.chunk_size
        end_chunk = end // self.chunk_size

        return self.missing_chunks(start_chunk, end_chunk)

    def read_range(self, start: int, end: int) -> bytes:
        if start < 0 or end < start or start >= self.file_size:
            return b""

        end = min(end, self.file_size - 1)

        start_chunk = start // self.chunk_size
        end_chunk = end // self.chunk_size

        parts = []

        with self._lock:
            for chunk_id in range(start_chunk, end_chunk + 1):

                if chunk_id not in self._chunk_cache:
                    break

                data = self._chunk_cache[chunk_id]

                chunk_start = chunk_id * self.chunk_size

                slice_start = max(0, start - chunk_start)
                slice_end = min(len(data), end - chunk_start + 1)

                parts.append(data[slice_start:slice_end])

        if not parts:
            return b""

        return b"".join(parts)

    @property
    def received_count(self):
        return len(self._received)

    @property
    def contiguous_chunks(self):
        with self._lock:
            count = 0

            while count < self.total_chunks:
                if count not in self._received:
                    break

                count += 1

            return count

    @property
    def bytes_written(self):
        return self._bytes_written

    @property
    def progress(self):
        if self.total_chunks == 0:
            return 1.0

        return len(self._received) / self.total_chunks

    def cleanup(self):
        with self._lock:
            self._chunk_cache.clear()
            self._received.clear()