"""Tests for SquirrelDB Python SDK TCP protocol module."""

import pytest
from squirreldb.protocol import (
    MAGIC,
    PROTOCOL_VERSION,
    MAX_MESSAGE_SIZE,
    HandshakeStatus,
    MessageType,
    Encoding,
    ProtocolFlags,
    build_handshake,
    parse_handshake_response,
    encode_message,
    decode_message,
    build_frame,
    parse_frame_header,
)


class TestProtocolConstants:
    """Test protocol constants."""

    def test_magic_bytes(self):
        assert MAGIC == b"SQRL"

    def test_protocol_version(self):
        assert PROTOCOL_VERSION == 0x01

    def test_max_message_size(self):
        assert MAX_MESSAGE_SIZE == 16 * 1024 * 1024


class TestHandshakeStatus:
    """Test HandshakeStatus enum."""

    def test_success_value(self):
        assert HandshakeStatus.SUCCESS == 0x00

    def test_version_mismatch_value(self):
        assert HandshakeStatus.VERSION_MISMATCH == 0x01

    def test_auth_failed_value(self):
        assert HandshakeStatus.AUTH_FAILED == 0x02

    def test_from_int(self):
        assert HandshakeStatus(0x00) == HandshakeStatus.SUCCESS
        assert HandshakeStatus(0x01) == HandshakeStatus.VERSION_MISMATCH
        assert HandshakeStatus(0x02) == HandshakeStatus.AUTH_FAILED


class TestMessageType:
    """Test MessageType enum."""

    def test_request_value(self):
        assert MessageType.REQUEST == 0x01

    def test_response_value(self):
        assert MessageType.RESPONSE == 0x02

    def test_notification_value(self):
        assert MessageType.NOTIFICATION == 0x03

    def test_from_int(self):
        assert MessageType(0x01) == MessageType.REQUEST
        assert MessageType(0x02) == MessageType.RESPONSE
        assert MessageType(0x03) == MessageType.NOTIFICATION


class TestEncoding:
    """Test Encoding enum."""

    def test_messagepack_value(self):
        assert Encoding.MESSAGEPACK == 0x01

    def test_json_value(self):
        assert Encoding.JSON == 0x02

    def test_from_int(self):
        assert Encoding(0x01) == Encoding.MESSAGEPACK
        assert Encoding(0x02) == Encoding.JSON


class TestProtocolFlags:
    """Test ProtocolFlags dataclass."""

    def test_default_flags(self):
        flags = ProtocolFlags()
        assert flags.messagepack is True
        assert flags.json_fallback is True

    def test_custom_flags(self):
        flags = ProtocolFlags(messagepack=False, json_fallback=False)
        assert flags.messagepack is False
        assert flags.json_fallback is False

    def test_to_byte_both_false(self):
        flags = ProtocolFlags(messagepack=False, json_fallback=False)
        assert flags.to_byte() == 0x00

    def test_to_byte_messagepack_only(self):
        flags = ProtocolFlags(messagepack=True, json_fallback=False)
        assert flags.to_byte() == 0x01

    def test_to_byte_json_fallback_only(self):
        flags = ProtocolFlags(messagepack=False, json_fallback=True)
        assert flags.to_byte() == 0x02

    def test_to_byte_both_true(self):
        flags = ProtocolFlags(messagepack=True, json_fallback=True)
        assert flags.to_byte() == 0x03

    def test_from_byte_zero(self):
        flags = ProtocolFlags.from_byte(0x00)
        assert flags.messagepack is False
        assert flags.json_fallback is False

    def test_from_byte_messagepack(self):
        flags = ProtocolFlags.from_byte(0x01)
        assert flags.messagepack is True
        assert flags.json_fallback is False

    def test_from_byte_json_fallback(self):
        flags = ProtocolFlags.from_byte(0x02)
        assert flags.messagepack is False
        assert flags.json_fallback is True

    def test_from_byte_both(self):
        flags = ProtocolFlags.from_byte(0x03)
        assert flags.messagepack is True
        assert flags.json_fallback is True

    def test_roundtrip(self):
        """Test converting to byte and back preserves values."""
        for msgpack in [True, False]:
            for json_fb in [True, False]:
                flags = ProtocolFlags(messagepack=msgpack, json_fallback=json_fb)
                byte = flags.to_byte()
                restored = ProtocolFlags.from_byte(byte)
                assert restored.messagepack == msgpack
                assert restored.json_fallback == json_fb


class TestBuildHandshake:
    """Test build_handshake function."""

    def test_handshake_without_auth(self):
        data = build_handshake()
        assert data[:4] == MAGIC
        assert data[4] == PROTOCOL_VERSION
        assert data[5] == 0x03  # Default flags (both true)
        assert data[6:8] == b"\x00\x00"  # Token length 0

    def test_handshake_with_auth(self):
        data = build_handshake(auth_token="my-secret-token")
        assert data[:4] == MAGIC
        assert data[4] == PROTOCOL_VERSION
        token = "my-secret-token"
        token_len = len(token.encode("utf-8"))
        assert int.from_bytes(data[6:8], "big") == token_len
        assert data[8:].decode("utf-8") == token

    def test_handshake_with_custom_flags(self):
        flags = ProtocolFlags(messagepack=True, json_fallback=False)
        data = build_handshake(flags=flags)
        assert data[5] == 0x01

    def test_handshake_with_unicode_token(self):
        data = build_handshake(auth_token="token-\u00e9\u00e8")  # token-ee
        assert MAGIC in data


class TestParseHandshakeResponse:
    """Test parse_handshake_response function."""

    def test_parse_success_response(self):
        # Build a successful response: status=0, version=1, flags=3, session_id=16 bytes
        session_id = b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
        response = bytes([0x00, 0x01, 0x03]) + session_id

        status, version, flags, sid = parse_handshake_response(response)

        assert status == HandshakeStatus.SUCCESS
        assert version == 0x01
        assert flags.messagepack is True
        assert flags.json_fallback is True
        assert sid == session_id

    def test_parse_version_mismatch_response(self):
        session_id = b"\x00" * 16
        response = bytes([0x01, 0x02, 0x01]) + session_id

        status, version, flags, _ = parse_handshake_response(response)

        assert status == HandshakeStatus.VERSION_MISMATCH
        assert version == 0x02

    def test_parse_auth_failed_response(self):
        session_id = b"\x00" * 16
        response = bytes([0x02, 0x01, 0x01]) + session_id

        status, _, _, _ = parse_handshake_response(response)

        assert status == HandshakeStatus.AUTH_FAILED

    def test_parse_too_short_raises(self):
        with pytest.raises(ValueError, match="too short"):
            parse_handshake_response(b"\x00\x01")


class TestEncodeDecodeMessage:
    """Test message encoding and decoding."""

    def test_encode_messagepack(self):
        msg = {"type": "query", "id": "123", "query": "test"}
        data = encode_message(msg, Encoding.MESSAGEPACK)
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_encode_json(self):
        msg = {"type": "query", "id": "123", "query": "test"}
        data = encode_message(msg, Encoding.JSON)
        assert isinstance(data, bytes)
        assert b'"type"' in data
        assert b'"query"' in data

    def test_decode_messagepack(self):
        import msgpack
        msg = {"type": "result", "id": "456", "data": [1, 2, 3]}
        data = msgpack.packb(msg, use_bin_type=True)
        decoded = decode_message(data, Encoding.MESSAGEPACK)
        assert decoded == msg

    def test_decode_json(self):
        msg = {"type": "result", "id": "456", "data": [1, 2, 3]}
        data = b'{"type": "result", "id": "456", "data": [1, 2, 3]}'
        decoded = decode_message(data, Encoding.JSON)
        assert decoded == msg

    def test_roundtrip_messagepack(self):
        msg = {
            "type": "insert",
            "id": "req-1",
            "collection": "users",
            "data": {"name": "Alice", "age": 30, "active": True},
        }
        encoded = encode_message(msg, Encoding.MESSAGEPACK)
        decoded = decode_message(encoded, Encoding.MESSAGEPACK)
        assert decoded == msg

    def test_roundtrip_json(self):
        msg = {
            "type": "insert",
            "id": "req-1",
            "collection": "users",
            "data": {"name": "Alice", "age": 30, "active": True},
        }
        encoded = encode_message(msg, Encoding.JSON)
        decoded = decode_message(encoded, Encoding.JSON)
        assert decoded == msg


class TestBuildFrame:
    """Test build_frame function."""

    def test_frame_structure(self):
        payload = b"test payload"
        frame = build_frame(MessageType.REQUEST, Encoding.MESSAGEPACK, payload)

        # Length should be payload + 2 (type + encoding)
        length = int.from_bytes(frame[0:4], "big")
        assert length == len(payload) + 2

        # Message type
        assert frame[4] == MessageType.REQUEST

        # Encoding
        assert frame[5] == Encoding.MESSAGEPACK

        # Payload
        assert frame[6:] == payload

    def test_frame_with_response_type(self):
        payload = b"response data"
        frame = build_frame(MessageType.RESPONSE, Encoding.JSON, payload)

        assert frame[4] == MessageType.RESPONSE
        assert frame[5] == Encoding.JSON

    def test_frame_with_notification_type(self):
        payload = b"notification"
        frame = build_frame(MessageType.NOTIFICATION, Encoding.MESSAGEPACK, payload)

        assert frame[4] == MessageType.NOTIFICATION


class TestParseFrameHeader:
    """Test parse_frame_header function."""

    def test_parse_request_header(self):
        # Length=14 (12 payload + 2), type=REQUEST, encoding=MESSAGEPACK
        header = b"\x00\x00\x00\x0e\x01\x01"
        payload_len, msg_type, encoding = parse_frame_header(header)

        assert payload_len == 12
        assert msg_type == MessageType.REQUEST
        assert encoding == Encoding.MESSAGEPACK

    def test_parse_response_header(self):
        header = b"\x00\x00\x00\x22\x02\x02"  # Length=34, type=RESPONSE, encoding=JSON
        payload_len, msg_type, encoding = parse_frame_header(header)

        assert payload_len == 32
        assert msg_type == MessageType.RESPONSE
        assert encoding == Encoding.JSON

    def test_parse_notification_header(self):
        header = b"\x00\x00\x01\x02\x03\x01"  # Length=258, type=NOTIFICATION, encoding=MSGPACK
        payload_len, msg_type, encoding = parse_frame_header(header)

        assert payload_len == 256
        assert msg_type == MessageType.NOTIFICATION

    def test_parse_too_short_raises(self):
        with pytest.raises(ValueError, match="too short"):
            parse_frame_header(b"\x00\x00\x00")


class TestFullFrameRoundtrip:
    """Test building and parsing frames together."""

    def test_frame_roundtrip(self):
        """Test that we can build a frame and parse it back."""
        msg = {"type": "query", "id": "test-123", "query": 'db.table("users").run()'}

        # Encode message
        payload = encode_message(msg, Encoding.MESSAGEPACK)

        # Build frame
        frame = build_frame(MessageType.REQUEST, Encoding.MESSAGEPACK, payload)

        # Parse header
        payload_len, msg_type, encoding = parse_frame_header(frame[:6])

        # Extract and decode payload
        extracted_payload = frame[6 : 6 + payload_len]
        decoded = decode_message(extracted_payload, encoding)

        assert msg_type == MessageType.REQUEST
        assert encoding == Encoding.MESSAGEPACK
        assert decoded == msg

    def test_frame_roundtrip_json(self):
        """Test JSON frame roundtrip."""
        msg = {"type": "result", "id": "resp-456", "data": {"count": 42}}

        payload = encode_message(msg, Encoding.JSON)
        frame = build_frame(MessageType.RESPONSE, Encoding.JSON, payload)
        payload_len, msg_type, encoding = parse_frame_header(frame[:6])
        extracted_payload = frame[6 : 6 + payload_len]
        decoded = decode_message(extracted_payload, encoding)

        assert msg_type == MessageType.RESPONSE
        assert encoding == Encoding.JSON
        assert decoded == msg
