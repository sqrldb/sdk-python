"""SquirrelDB TCP wire protocol client implementation."""

import asyncio
import struct
import uuid as uuid_module
from typing import Any, AsyncIterator, Callable, Optional

from .types import Document, ChangeEvent
from .protocol import (
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
)


class SquirrelDBError(Exception):
    """Base exception for SquirrelDB errors."""
    pass


class ConnectionError(SquirrelDBError):
    """Connection-related errors."""
    pass


class HandshakeError(SquirrelDBError):
    """Handshake-related errors."""
    pass


class VersionMismatchError(HandshakeError):
    """Protocol version mismatch."""
    def __init__(self, server_version: int, client_version: int):
        self.server_version = server_version
        self.client_version = client_version
        super().__init__(f"Version mismatch: server={server_version}, client={client_version}")


class AuthenticationError(HandshakeError):
    """Authentication failed."""
    pass


class SquirrelDBTcp:
    """TCP wire protocol client for SquirrelDB."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8082,
        auth_token: str = "",
        use_messagepack: bool = True,
        json_fallback: bool = True,
    ):
        self._host = host
        self._port = port
        self._auth_token = auth_token
        self._flags = ProtocolFlags(messagepack=use_messagepack, json_fallback=json_fallback)
        self._encoding = Encoding.MESSAGEPACK if use_messagepack else Encoding.JSON

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._session_id: Optional[bytes] = None
        self._request_id = 0
        self._pending: dict[str, asyncio.Future] = {}
        self._subscriptions: dict[str, asyncio.Queue] = {}
        self._recv_task: Optional[asyncio.Task] = None
        self._closed = False
        self._write_lock = asyncio.Lock()

    @classmethod
    async def connect(
        cls,
        host: str = "localhost",
        port: int = 8082,
        auth_token: str = "",
        use_messagepack: bool = True,
    ) -> "SquirrelDBTcp":
        """Connect to a SquirrelDB server via TCP wire protocol."""
        client = cls(host, port, auth_token, use_messagepack)
        await client._connect()
        return client

    @property
    def session_id(self) -> Optional[str]:
        """Get the session ID as a UUID string."""
        if self._session_id:
            return str(uuid_module.UUID(bytes=self._session_id))
        return None

    async def _connect(self) -> None:
        """Establish connection and perform handshake."""
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port
        )

        # Send handshake
        handshake = build_handshake(self._auth_token, self._flags)
        self._writer.write(handshake)
        await self._writer.drain()

        # Read handshake response (19 bytes: 1 status + 1 version + 1 flags + 16 session_id)
        response = await self._reader.readexactly(19)
        status, version, flags, session_id = parse_handshake_response(response)

        if status == HandshakeStatus.VERSION_MISMATCH:
            raise VersionMismatchError(version, PROTOCOL_VERSION)
        elif status == HandshakeStatus.AUTH_FAILED:
            raise AuthenticationError("Authentication failed")
        elif status != HandshakeStatus.SUCCESS:
            raise HandshakeError(f"Unexpected status: {status}")

        self._session_id = session_id
        self._encoding = Encoding.MESSAGEPACK if flags.messagepack else Encoding.JSON

        # Start receive loop
        self._recv_task = asyncio.create_task(self._receive_loop())

    async def _receive_loop(self) -> None:
        """Background task to receive and dispatch messages."""
        try:
            while not self._closed and self._reader:
                # Read frame header (6 bytes)
                header = await self._reader.readexactly(6)
                length = struct.unpack(">I", header[0:4])[0]
                msg_type = MessageType(header[4])
                encoding = Encoding(header[5])

                if length > MAX_MESSAGE_SIZE:
                    raise SquirrelDBError(f"Message too large: {length}")

                # Read payload
                payload_len = length - 2
                payload = await self._reader.readexactly(payload_len)
                msg = decode_message(payload, encoding)

                await self._dispatch_message(msg, msg_type)
        except asyncio.IncompleteReadError:
            pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if not self._closed:
                # Reject all pending requests
                for future in self._pending.values():
                    if not future.done():
                        future.set_exception(ConnectionError(str(e)))
                self._pending.clear()

    async def _dispatch_message(self, msg: dict, msg_type: MessageType) -> None:
        """Dispatch a received message to the appropriate handler."""
        msg_id = msg.get("id")
        type_name = msg.get("type")

        if type_name == "change" and msg_id in self._subscriptions:
            # Subscription notification
            change = ChangeEvent.from_dict(msg["change"])
            await self._subscriptions[msg_id].put(change)
        elif msg_id in self._pending:
            # Response to a pending request
            future = self._pending.pop(msg_id)
            if not future.done():
                future.set_result(msg)

    def _next_id(self) -> str:
        """Generate a unique request ID."""
        self._request_id += 1
        return str(self._request_id)

    async def _send(self, msg: dict) -> dict:
        """Send a request and wait for response."""
        if not self._writer:
            raise ConnectionError("Not connected")

        msg_id = msg["id"]
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[msg_id] = future

        payload = encode_message(msg, self._encoding)
        frame = build_frame(MessageType.REQUEST, self._encoding, payload)

        async with self._write_lock:
            self._writer.write(frame)
            await self._writer.drain()

        return await future

    async def query(self, q: str) -> list[Document]:
        """Execute a query and return documents."""
        resp = await self._send({
            "type": "query",
            "id": self._next_id(),
            "query": q,
        })
        if resp["type"] == "error":
            raise SquirrelDBError(resp["error"])
        return [Document.from_dict(d) for d in resp["data"]]

    async def query_raw(self, q: str) -> Any:
        """Execute a query and return raw JSON data."""
        resp = await self._send({
            "type": "query",
            "id": self._next_id(),
            "query": q,
        })
        if resp["type"] == "error":
            raise SquirrelDBError(resp["error"])
        return resp["data"]

    async def insert(self, collection: str, data: dict[str, Any]) -> Document:
        """Insert a document into a collection."""
        resp = await self._send({
            "type": "insert",
            "id": self._next_id(),
            "collection": collection,
            "data": data,
        })
        if resp["type"] == "error":
            raise SquirrelDBError(resp["error"])
        return Document.from_dict(resp["data"])

    async def update(
        self, collection: str, document_id: str, data: dict[str, Any]
    ) -> Document:
        """Update a document."""
        resp = await self._send({
            "type": "update",
            "id": self._next_id(),
            "collection": collection,
            "document_id": document_id,
            "data": data,
        })
        if resp["type"] == "error":
            raise SquirrelDBError(resp["error"])
        return Document.from_dict(resp["data"])

    async def delete(self, collection: str, document_id: str) -> Document:
        """Delete a document."""
        resp = await self._send({
            "type": "delete",
            "id": self._next_id(),
            "collection": collection,
            "document_id": document_id,
        })
        if resp["type"] == "error":
            raise SquirrelDBError(resp["error"])
        return Document.from_dict(resp["data"])

    async def list_collections(self) -> list[str]:
        """List all collections."""
        resp = await self._send({
            "type": "listcollections",
            "id": self._next_id(),
        })
        if resp["type"] == "error":
            raise SquirrelDBError(resp["error"])
        return resp["data"]

    async def subscribe(self, q: str) -> "Subscription":
        """Subscribe to changes and return an async iterator."""
        sub_id = self._next_id()
        resp = await self._send({
            "type": "subscribe",
            "id": sub_id,
            "query": q,
        })
        if resp["type"] == "error":
            raise SquirrelDBError(resp["error"])

        queue: asyncio.Queue = asyncio.Queue()
        self._subscriptions[sub_id] = queue

        return Subscription(sub_id, queue, self)

    async def unsubscribe(self, subscription_id: str) -> None:
        """Unsubscribe from changes."""
        self._subscriptions.pop(subscription_id, None)

        if self._writer:
            msg = {"type": "unsubscribe", "id": subscription_id}
            payload = encode_message(msg, self._encoding)
            frame = build_frame(MessageType.REQUEST, self._encoding, payload)
            async with self._write_lock:
                self._writer.write(frame)
                await self._writer.drain()

    async def ping(self) -> None:
        """Ping the server."""
        resp = await self._send({
            "type": "ping",
            "id": self._next_id(),
        })
        if resp["type"] != "pong":
            raise SquirrelDBError(f"Unexpected response: {resp['type']}")

    async def close(self) -> None:
        """Close the connection."""
        self._closed = True
        self._subscriptions.clear()

        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()

        if self._recv_task:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass

        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()


class Subscription:
    """Async iterator for subscription changes."""

    def __init__(
        self,
        subscription_id: str,
        queue: asyncio.Queue,
        client: SquirrelDBTcp,
    ):
        self._id = subscription_id
        self._queue = queue
        self._client = client
        self._closed = False

    @property
    def id(self) -> str:
        """Get the subscription ID."""
        return self._id

    def __aiter__(self) -> "Subscription":
        return self

    async def __anext__(self) -> ChangeEvent:
        if self._closed:
            raise StopAsyncIteration

        try:
            return await self._queue.get()
        except asyncio.CancelledError:
            raise StopAsyncIteration

    async def unsubscribe(self) -> None:
        """Unsubscribe from changes."""
        self._closed = True
        await self._client.unsubscribe(self._id)


# Synchronous wrapper for convenience
class SquirrelDBTcpSync:
    """Synchronous wrapper for SquirrelDBTcp."""

    def __init__(self, client: SquirrelDBTcp):
        self._client = client
        self._loop = asyncio.new_event_loop()

    @classmethod
    def connect(
        cls,
        host: str = "localhost",
        port: int = 8082,
        auth_token: str = "",
        use_messagepack: bool = True,
    ) -> "SquirrelDBTcpSync":
        """Connect to a SquirrelDB server via TCP wire protocol."""
        loop = asyncio.new_event_loop()
        client = loop.run_until_complete(
            SquirrelDBTcp.connect(host, port, auth_token, use_messagepack)
        )
        wrapper = cls.__new__(cls)
        wrapper._client = client
        wrapper._loop = loop
        return wrapper

    def query(self, q: str) -> list[Document]:
        return self._loop.run_until_complete(self._client.query(q))

    def query_raw(self, q: str) -> Any:
        return self._loop.run_until_complete(self._client.query_raw(q))

    def insert(self, collection: str, data: dict[str, Any]) -> Document:
        return self._loop.run_until_complete(self._client.insert(collection, data))

    def update(self, collection: str, document_id: str, data: dict[str, Any]) -> Document:
        return self._loop.run_until_complete(self._client.update(collection, document_id, data))

    def delete(self, collection: str, document_id: str) -> Document:
        return self._loop.run_until_complete(self._client.delete(collection, document_id))

    def list_collections(self) -> list[str]:
        return self._loop.run_until_complete(self._client.list_collections())

    def ping(self) -> None:
        return self._loop.run_until_complete(self._client.ping())

    def close(self) -> None:
        self._loop.run_until_complete(self._client.close())
        self._loop.close()
