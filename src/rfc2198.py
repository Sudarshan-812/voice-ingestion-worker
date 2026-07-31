"""Byte-packer for RFC 2198 (RTP Payload for Redundant Audio Data).

RFC 2198 wraps a primary payload plus zero or more redundant (older)
payloads behind a single RTP payload type. Each redundant block is
preceded by a 4-byte header; the primary (most recent) block has no
header of its own, only the preceding redundant headers.

Redundant header (4 bytes), one per redundant block:
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |F|   block PT  |  timestamp offset         |   block length   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     F = 1 for a redundant block header, 0 for the primary block header.

Primary header (1 byte): F=0, payload type, no offset/length fields.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

PRIMARY_HEADER_LENGTH = 1
REDUNDANT_HEADER_LENGTH = 4


@dataclass(frozen=True)
class RedundantBlock:
    payload_type: int
    timestamp_offset: int
    payload: bytes


def pack_redundant_header(payload_type: int, timestamp_offset: int, block_length: int) -> bytes:
    """Pack a single 4-byte RFC 2198 redundant block header."""
    if not (0 <= payload_type <= 0x7F):
        raise ValueError("payload_type must fit in 7 bits")
    if not (0 <= timestamp_offset <= 0x3FFF):
        raise ValueError("timestamp_offset must fit in 14 bits")
    if not (0 <= block_length <= 0x3FF):
        raise ValueError("block_length must fit in 10 bits")

    first_byte = 0x80 | (payload_type & 0x7F)
    remaining = (timestamp_offset << 10) | block_length
    return struct.pack(">B", first_byte) + struct.pack(">I", remaining)[1:]


def pack_primary_header(payload_type: int) -> bytes:
    """Pack the 1-byte primary block header (F=0)."""
    if not (0 <= payload_type <= 0x7F):
        raise ValueError("payload_type must fit in 7 bits")
    return struct.pack(">B", payload_type & 0x7F)


def build_packet(redundant_blocks: list[RedundantBlock], primary_payload_type: int, primary_payload: bytes) -> bytes:
    """Assemble a full RFC 2198 payload: redundant headers + primary header + all payloads."""
    headers = b"".join(
        pack_redundant_header(block.payload_type, block.timestamp_offset, len(block.payload))
        for block in redundant_blocks
    )
    headers += pack_primary_header(primary_payload_type)

    payloads = b"".join(block.payload for block in redundant_blocks) + primary_payload
    return headers + payloads
