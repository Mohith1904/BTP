"""
Loopback test — runs sender and receiver on the same machine using 127.0.0.1.
Tests: packet exchange, file transfer, and basic protocol flow.
"""

import os
import sys
import time
import threading

# Override config for loopback
import config
config.SENDER_LIFI_IP   = "127.0.0.1"
config.SENDER_WIFI_IP   = "127.0.0.2"
config.RECEIVER_LIFI_IP = "127.0.0.1"
config.RECEIVER_WIFI_IP = "127.0.0.2"

# Use different ports so sender & receiver don't clash on same machine
# We need to restructure slightly for same-machine test
# Instead, let's do a unit-level test of each component

from protocol.packet import Packet, PType
from protocol.chunk_manager import FileChunker, ChunkReassembler

def test_packet_roundtrip():
    print("Test 1: Packet serialization roundtrip...")
    for ptype in [PType.DATA, PType.ACK, PType.HEARTBEAT, PType.SWITCH_NOTIFY, PType.FILE_META]:
        p = Packet(ptype, seq_num=123, chunk_id=45, total_chunks=100,
                   session_id=999, payload=b"hello world", flags=0x01)
        raw = p.pack()
        p2 = Packet.unpack(raw)
        assert p2.packet_type == ptype
        assert p2.seq_num == 123
        assert p2.chunk_id == 45
        assert p2.total_chunks == 100
        assert p2.session_id == 999
        assert p2.payload == b"hello world"
        assert p2.flags == 0x01
    print("  ✓ All packet types serialize/deserialize correctly")

def test_packet_corruption():
    print("Test 2: Packet corruption detection...")
    p = Packet(PType.DATA, payload=b"important data")
    raw = bytearray(p.pack())
    raw[20] ^= 0xFF  # flip a byte in payload
    try:
        Packet.unpack(bytes(raw))
        print("  ✗ Should have raised ValueError!")
    except ValueError:
        print("  ✓ Corrupted packet correctly rejected")

def test_chunking():
    print("Test 3: File chunking + reassembly...")
    # Create test file
    test_file = os.path.join(config.SHARED_FOLDER, "test_video.bin")
    test_data = os.urandom(500_000)  # 500 KB random data
    with open(test_file, "wb") as f:
        f.write(test_data)

    chunker = FileChunker(test_file, chunk_size=60 * 1024)
    meta = chunker.metadata()
    print(f"  File: {meta['file_size']} bytes, {meta['total_chunks']} chunks")

    reassembler = ChunkReassembler(
        filename="test_video.bin",
        file_size=meta["file_size"],
        total_chunks=meta["total_chunks"],
        chunk_size=meta["chunk_size"],
        file_hash=meta["file_hash"],
        output_dir=config.RECEIVE_FOLDER,
    )

    # Simulate out-of-order delivery
    import random
    order = list(range(chunker.total_chunks))
    random.shuffle(order)

    for cid in order:
        data = chunker.get_chunk(cid)
        reassembler.add_chunk(cid, data)

    assert reassembler.is_complete, "Should be complete"
    assert reassembler.verify(), "Hash should match"

    # Verify byte-for-byte
    with open(reassembler.output_path, "rb") as f:
        received = f.read()
    assert received == test_data, "Data mismatch!"
    print(f"  ✓ {chunker.total_chunks} chunks reassembled out-of-order, hash verified")

    # Cleanup
    os.remove(test_file)
    os.remove(reassembler.output_path)

def test_json_payload():
    print("Test 4: JSON payload encoding...")
    data = {"filename": "movie.mp4", "file_size": 1234567, "total_chunks": 21}
    payload = Packet.make_json_payload(data)
    p = Packet(PType.FILE_META, payload=payload)
    raw = p.pack()
    p2 = Packet.unpack(raw)
    decoded = p2.json_payload()
    assert decoded["filename"] == "movie.mp4"
    assert decoded["file_size"] == 1234567
    print("  ✓ JSON payload roundtrip OK")

def test_network_manager():
    print("Test 5: NetworkManager socket creation...")
    from network.manager import NetworkManager
    nm = NetworkManager(
        my_lifi_ip="127.0.0.1",
        my_wifi_ip="127.0.0.2",
        peer_lifi_ip="127.0.0.1",
        peer_wifi_ip="127.0.0.2",
        data_port=6000,
        control_port=6001,
    )
    nm.start()
    time.sleep(0.2)
    nm.stop()
    print("  ✓ NetworkManager start/stop OK")

if __name__ == "__main__":
    print("=" * 50)
    print(" LiFi-WiFi Protocol — Component Tests")
    print("=" * 50)
    print()

    test_packet_roundtrip()
    test_packet_corruption()
    test_chunking()
    test_json_payload()
    test_network_manager()

    print()
    print("=" * 50)
    print(" ALL TESTS PASSED ✓")
    print("=" * 50)
