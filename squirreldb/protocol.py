"""Wire protocol types and constants for SquirrelDB TCP connections."""

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Any
import msgpack
import json


# Protocol constants
MAGIC = b"SQRL"
PROTOCOL_VERSION = 0x01
MAX_MESSAGE_SIZE = 16 * 1024 * 1024  # 16MB


class HandshakeStatus(IntEnum):
    """Handshake response status codes."""
    SUCCESS = 0x00
    VERSION_MISMATCH = 0x01
    AUTH_FAILED = 0x02


class MessageType(IntEnum):
    """Message type codes."""
    REQUEST = 0x01
    RESPONSE = 0x02
    NOTIFICATION = 0x03


class Encoding(IntEnum):
    """Serialization encoding codes."""
    MESSAGEPACK = 0x01
    JSON = 0x02


@dataclass
class ProtocolFlags:
    """Handshake protocol flags."""
    messagepack: bool = True
    json_fallback: bool = True

    def to_byte(self) -> int:
        """Convert flags to byte."""
        byte = 0
        if self.messagepack:
            byte |= 0x01
        if self.json_fallback:
            byte |= 0x02
        return byte

    @classmethod
    def from_byte(cls, byte: int) -> "ProtocolFlags":
        """Create flags from byte."""
        return cls(
            messagepack=bool(byte & 0x01),
            json_fallback=bool(byte & 0x02),
        )


def build_handshake(auth_token: str = "", flags: ProtocolFlags | None = None) -> bytes:
    """Build handshake packet to send to server."""
    if flags is None:
        flags = ProtocolFlags()

    token_bytes = auth_token.encode("utf-8")

    return (
        MAGIC +
        struct.pack(">B", PROTOCOL_VERSION) +
        struct.pack(">B", flags.to_byte()) +
        struct.pack(">H", len(token_bytes)) +
        token_bytes
    )


def parse_handshake_response(data: bytes) -> tuple[HandshakeStatus, int, ProtocolFlags, bytes]:
    """
    Parse handshake response from server.

    Returns: (status, version, flags, session_id)
    """
    if len(data) < 19:
        raise ValueError(f"Handshake response too short: {len(data)} bytes")

    status = HandshakeStatus(data[0])
    version = data[1]
    flags = ProtocolFlags.from_byte(data[2])
    session_id = data[3:19]

    return status, version, flags, session_id


def encode_message(msg: dict[str, Any], encoding: Encoding) -> bytes:
    """Encode a message using the specified encoding."""
    if encoding == Encoding.MESSAGEPACK:
        return msgpack.packb(msg, use_bin_type=True)
    else:
        return json.dumps(msg).encode("utf-8")


def decode_message(data: bytes, encoding: Encoding) -> dict[str, Any]:
    """Decode a message using the specified encoding."""
    if encoding == Encoding.MESSAGEPACK:
        return msgpack.unpackb(data, raw=False)
    else:
        return json.loads(data.decode("utf-8"))


def build_frame(msg_type: MessageType, encoding: Encoding, payload: bytes) -> bytes:
    """Build a framed message."""
    length = len(payload) + 2  # +2 for type and encoding bytes
    return (
        struct.pack(">I", length) +
        struct.pack(">B", msg_type) +
        struct.pack(">B", encoding) +
        payload
    )


def parse_frame_header(data: bytes) -> tuple[int, MessageType, Encoding]:
    """
    Parse frame header.

    Returns: (payload_length, message_type, encoding)
    """
    if len(data) < 6:
        raise ValueError(f"Frame header too short: {len(data)} bytes")

    length = struct.unpack(">I", data[0:4])[0]
    msg_type = MessageType(data[4])
    encoding = Encoding(data[5])

    # length includes type and encoding bytes
    payload_length = length - 2

    return payload_length, msg_type, encoding
