"""
Binary packet format for the LiFi-WiFi failover protocol.

Header (28 bytes):
  magic       : 2 bytes  (0x4C46 = "LF")
  version     : 1 byte   (0x01)
  packet_type : 1 byte
  seq_num     : 4 bytes   (unsigned, monotonic per sender)
  chunk_id    : 4 bytes   (unsigned, index within file)
  total_chunks: 4 bytes   (unsigned)
  session_id  : 4 bytes   (unsigned, identifies one transfer)
  payload_len : 4 bytes   (unsigned)
  flags       : 1 byte
  reserved    : 3 bytes

Payload: 0 .. CHUNK_SIZE bytes
Checksum: 4 bytes (CRC32 of header + payload)
"""

import struct
import zlib
import json

# ── Magic & format ──────────────────────────────────────────
MAGIC = b'\x4C\x46'
VERSION = 0x01
HEADER_FMT = '!2sBBIIIIIB3s'          # network byte-order
HEADER_SIZE = struct.calcsize(HEADER_FMT)  # 28
CHECKSUM_SIZE = 4


# ── Packet types ────────────────────────────────────────────
class PType:
    DATA              = 0x01
    ACK               = 0x02
    HEARTBEAT         = 0x03
    HEARTBEAT_ACK     = 0x04
    SWITCH_NOTIFY     = 0x05
    SWITCH_ACK        = 0x06
    SWITCH_BACK       = 0x07
    SWITCH_BACK_ACK   = 0x08
    FILE_META         = 0x09
    TRANSFER_COMPLETE = 0x0A
    FILE_LIST_REQUEST = 0x0B
    FILE_LIST_RESPONSE= 0x0C
    FILE_REQUEST      = 0x0D

    _NAMES = {
        0x01: "DATA",       0x02: "ACK",
        0x03: "HEARTBEAT",  0x04: "HEARTBEAT_ACK",
        0x05: "SWITCH_NOTIFY",  0x06: "SWITCH_ACK",
        0x07: "SWITCH_BACK",    0x08: "SWITCH_BACK_ACK",
        0x09: "FILE_META",      0x0A: "TRANSFER_COMPLETE",
        0x0B: "FILE_LIST_REQ",  0x0C: "FILE_LIST_RESP",
        0x0D: "FILE_REQUEST",
    }

    @classmethod
    def name(cls, ptype: int) -> str:
        return cls._NAMES.get(ptype, f"UNKNOWN(0x{ptype:02X})")


# ── Packet class ────────────────────────────────────────────
class Packet:
    __slots__ = (
        "packet_type", "seq_num", "chunk_id",
        "total_chunks", "session_id", "payload", "flags",
    )

    def __init__(
        self,
        packet_type: int,
        seq_num: int = 0,
        chunk_id: int = 0,
        total_chunks: int = 0,
        session_id: int = 0,
        payload: bytes = b"",
        flags: int = 0,
    ):
        self.packet_type = packet_type
        self.seq_num = seq_num
        self.chunk_id = chunk_id
        self.total_chunks = total_chunks
        self.session_id = session_id
        self.payload = payload if isinstance(payload, bytes) else payload.encode("utf-8")
        self.flags = flags

    # ── serialise ───────────────────────────────────────────
    def pack(self) -> bytes:
        header = struct.pack(
            HEADER_FMT,
            MAGIC,
            VERSION,
            self.packet_type,
            self.seq_num,
            self.chunk_id,
            self.total_chunks,
            self.session_id,
            len(self.payload),
            self.flags,
            b'\x00\x00\x00',
        )
        body = header + self.payload
        crc = struct.pack('!I', zlib.crc32(body) & 0xFFFFFFFF)
        return body + crc

    # ── deserialise ─────────────────────────────────────────
    @classmethod
    def unpack(cls, data: bytes) -> "Packet":
        min_len = HEADER_SIZE + CHECKSUM_SIZE
        if len(data) < min_len:
            raise ValueError(f"Packet too short: {len(data)} < {min_len}")

        body = data[:-CHECKSUM_SIZE]
        crc_bytes = data[-CHECKSUM_SIZE:]

        expected = struct.unpack('!I', crc_bytes)[0]
        actual = zlib.crc32(body) & 0xFFFFFFFF
        if expected != actual:
            raise ValueError("CRC32 checksum mismatch")

        (magic, _ver, ptype, seq, cid, total,
         sid, plen, flags, _reserved) = struct.unpack(HEADER_FMT, body[:HEADER_SIZE])

        if magic != MAGIC:
            raise ValueError("Invalid magic bytes")
        if _ver != VERSION:
            raise ValueError(f"Unsupported protocol version: {_ver}")
        if len(body) < HEADER_SIZE + plen:
            raise ValueError(f"Payload length mismatch: header says {plen}, but only {len(body) - HEADER_SIZE} bytes available")

        payload = body[HEADER_SIZE: HEADER_SIZE + plen]
        return cls(
            packet_type=ptype,
            seq_num=seq,
            chunk_id=cid,
            total_chunks=total,
            session_id=sid,
            payload=payload,
            flags=flags,
        )

    # ── helpers ─────────────────────────────────────────────
    def json_payload(self) -> dict:
        """Parse payload as JSON dict."""
        return json.loads(self.payload.decode("utf-8"))

    @staticmethod
    def make_json_payload(obj: dict) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

    def __repr__(self):
        return (
            f"Packet({PType.name(self.packet_type)}, seq={self.seq_num}, "
            f"chunk={self.chunk_id}/{self.total_chunks}, "
            f"session={self.session_id}, payload={len(self.payload)}B)"
        )
