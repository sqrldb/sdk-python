"""Tests for SquirrelDB Python SDK TCP client module."""

import pytest
import asyncio

from squirreldb.tcp import (
    SquirrelDBError,
    ConnectionError,
    HandshakeError,
    VersionMismatchError,
    AuthenticationError,
    SquirrelDBTcp,
    Subscription,
)


class TestErrorClasses:
    """Test error class hierarchy and messages."""

    def test_squirreldb_error_is_exception(self):
        assert issubclass(SquirrelDBError, Exception)

    def test_connection_error_inherits(self):
        assert issubclass(ConnectionError, SquirrelDBError)

    def test_handshake_error_inherits(self):
        assert issubclass(HandshakeError, SquirrelDBError)

    def test_version_mismatch_error_inherits(self):
        assert issubclass(VersionMismatchError, HandshakeError)

    def test_authentication_error_inherits(self):
        assert issubclass(AuthenticationError, HandshakeError)

    def test_squirreldb_error_message(self):
        err = SquirrelDBError("test message")
        assert str(err) == "test message"

    def test_connection_error_message(self):
        err = ConnectionError("connection refused")
        assert str(err) == "connection refused"

    def test_version_mismatch_error_properties(self):
        err = VersionMismatchError(server_version=2, client_version=1)
        assert err.server_version == 2
        assert err.client_version == 1
        assert "server=2" in str(err)
        assert "client=1" in str(err)


class TestSquirrelDBTcpInit:
    """Test SquirrelDBTcp initialization."""

    def test_default_init(self):
        client = SquirrelDBTcp()
        assert client._host == "localhost"
        assert client._port == 8082
        assert client._auth_token == ""
        assert client._flags.messagepack is True
        assert client._flags.json_fallback is True

    def test_custom_init(self):
        client = SquirrelDBTcp(
            host="db.example.com",
            port=9000,
            auth_token="my-token",
            use_messagepack=False,
            json_fallback=False,
        )
        assert client._host == "db.example.com"
        assert client._port == 9000
        assert client._auth_token == "my-token"
        assert client._flags.messagepack is False
        assert client._flags.json_fallback is False

    def test_initial_state(self):
        client = SquirrelDBTcp()
        assert client._reader is None
        assert client._writer is None
        assert client._session_id is None
        assert client._request_id == 0
        assert client._pending == {}
        assert client._subscriptions == {}
        assert client._recv_task is None
        assert client._closed is False


class TestSquirrelDBTcpNextId:
    """Test request ID generation."""

    def test_next_id_increments(self):
        client = SquirrelDBTcp()
        id1 = client._next_id()
        id2 = client._next_id()
        id3 = client._next_id()

        assert id1 == "1"
        assert id2 == "2"
        assert id3 == "3"

    def test_next_id_returns_string(self):
        client = SquirrelDBTcp()
        assert isinstance(client._next_id(), str)


class TestSquirrelDBTcpSessionId:
    """Test session ID property."""

    def test_session_id_none_when_not_connected(self):
        client = SquirrelDBTcp()
        assert client.session_id is None

    def test_session_id_format(self):
        client = SquirrelDBTcp()
        # Simulate setting session_id
        client._session_id = b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
        session_id = client.session_id
        assert isinstance(session_id, str)
        # UUID format: 8-4-4-4-12 hex characters
        assert len(session_id) == 36
        assert session_id.count("-") == 4


class TestSubscription:
    """Test Subscription class."""

    @pytest.mark.asyncio
    async def test_subscription_id_property(self):
        client = SquirrelDBTcp()
        queue = asyncio.Queue()
        sub = Subscription("sub-123", queue, client)
        assert sub.id == "sub-123"

    @pytest.mark.asyncio
    async def test_subscription_is_async_iterable(self):
        client = SquirrelDBTcp()
        queue = asyncio.Queue()
        sub = Subscription("sub-123", queue, client)
        assert hasattr(sub, "__aiter__")
        assert hasattr(sub, "__anext__")

    @pytest.mark.asyncio
    async def test_subscription_iter_returns_self(self):
        client = SquirrelDBTcp()
        queue = asyncio.Queue()
        sub = Subscription("sub-123", queue, client)
        assert sub.__aiter__() is sub


class TestConnectionNotConnected:
    """Test operations when not connected."""

    @pytest.mark.asyncio
    async def test_send_when_not_connected_raises(self):
        client = SquirrelDBTcp()
        with pytest.raises(ConnectionError, match="Not connected"):
            await client._send({"type": "ping", "id": "1"})


class TestConnectionRefused:
    """Test connection to non-existent server."""

    @pytest.mark.asyncio
    async def test_connect_refused(self):
        """Connecting to a port with nothing listening should fail."""
        with pytest.raises(Exception):
            # Use a port that's unlikely to be listening
            await SquirrelDBTcp.connect(host="127.0.0.1", port=59999)

    @pytest.mark.asyncio
    async def test_connect_invalid_host(self):
        """Connecting to an invalid host should fail."""
        with pytest.raises(Exception):
            await SquirrelDBTcp.connect(host="invalid.host.that.does.not.exist", port=8082)
