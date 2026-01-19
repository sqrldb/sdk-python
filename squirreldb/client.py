"""SquirrelDB client implementation"""

import asyncio
import json
import uuid
from typing import Any, Callable, Optional
from websockets.asyncio.client import connect as ws_connect, ClientConnection

from .types import Document, ChangeEvent


class SquirrelDB:
    """Client for connecting to SquirrelDB"""

    def __init__(
        self,
        url: str,
        reconnect: bool = True,
        max_reconnect_attempts: int = 10,
        reconnect_delay: float = 1.0,
    ):
        self._url = url if url.startswith("ws://") or url.startswith("wss://") else f"ws://{url}"
        self._reconnect = reconnect
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_delay = reconnect_delay
        self._ws: Optional[ClientConnection] = None
        self._pending: dict[str, asyncio.Future] = {}
        self._subscriptions: dict[str, Callable[[ChangeEvent], None]] = {}
        self._recv_task: Optional[asyncio.Task] = None
        self._closed = False
        self._reconnect_attempts = 0

    @classmethod
    async def connect(
        cls,
        url: str,
        reconnect: bool = True,
        max_reconnect_attempts: int = 10,
        reconnect_delay: float = 1.0,
    ) -> "SquirrelDB":
        """Connect to a SquirrelDB server"""
        client = cls(url, reconnect, max_reconnect_attempts, reconnect_delay)
        await client._connect()
        return client

    async def _connect(self) -> None:
        self._ws = await ws_connect(self._url)
        self._reconnect_attempts = 0
        self._recv_task = asyncio.create_task(self._receive_loop())

    async def _receive_loop(self) -> None:
        try:
            async for message in self._ws:
                await self._handle_message(message)
        except Exception:
            pass
        finally:
            await self._handle_disconnect()

    async def _handle_message(self, data: str) -> None:
        try:
            msg = json.loads(data)
            msg_type = msg.get("type")
            msg_id = msg.get("id")

            if msg_type == "change":
                callback = self._subscriptions.get(msg_id)
                if callback:
                    change = ChangeEvent.from_dict(msg["change"])
                    callback(change)
                return

            if msg_id and msg_id in self._pending:
                future = self._pending.pop(msg_id)
                if not future.done():
                    future.set_result(msg)
        except json.JSONDecodeError:
            pass

    async def _handle_disconnect(self) -> None:
        if self._closed:
            return

        # Reject pending requests
        for future in self._pending.values():
            if not future.done():
                future.set_exception(ConnectionError("Connection closed"))
        self._pending.clear()

        # Attempt reconnection
        if self._reconnect and self._reconnect_attempts < self._max_reconnect_attempts:
            self._reconnect_attempts += 1
            delay = self._reconnect_delay * (2 ** (self._reconnect_attempts - 1))
            await asyncio.sleep(delay)
            try:
                await self._connect()
            except Exception:
                pass

    async def _send(self, msg: dict) -> dict:
        if not self._ws:
            raise ConnectionError("Not connected")

        msg_id = msg["id"]
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[msg_id] = future

        await self._ws.send(json.dumps(msg))
        return await future

    def _generate_id(self) -> str:
        return str(uuid.uuid4())

    async def query(self, q: str) -> list[Document]:
        """Execute a query"""
        resp = await self._send({"type": "query", "id": self._generate_id(), "query": q})
        if resp["type"] == "error":
            raise Exception(resp["error"])
        return [Document.from_dict(d) for d in resp["data"]]

    async def subscribe(
        self, q: str, callback: Callable[[ChangeEvent], None]
    ) -> str:
        """Subscribe to changes"""
        sub_id = self._generate_id()
        resp = await self._send({"type": "subscribe", "id": sub_id, "query": q})
        if resp["type"] == "error":
            raise Exception(resp["error"])
        self._subscriptions[sub_id] = callback
        return sub_id

    async def unsubscribe(self, subscription_id: str) -> None:
        """Unsubscribe from changes"""
        await self._send({"type": "unsubscribe", "id": subscription_id})
        self._subscriptions.pop(subscription_id, None)

    async def insert(self, collection: str, data: dict[str, Any]) -> Document:
        """Insert a document"""
        resp = await self._send({
            "type": "insert",
            "id": self._generate_id(),
            "collection": collection,
            "data": data,
        })
        if resp["type"] == "error":
            raise Exception(resp["error"])
        return Document.from_dict(resp["data"])

    async def update(
        self, collection: str, document_id: str, data: dict[str, Any]
    ) -> Document:
        """Update a document"""
        resp = await self._send({
            "type": "update",
            "id": self._generate_id(),
            "collection": collection,
            "document_id": document_id,
            "data": data,
        })
        if resp["type"] == "error":
            raise Exception(resp["error"])
        return Document.from_dict(resp["data"])

    async def delete(self, collection: str, document_id: str) -> Document:
        """Delete a document"""
        resp = await self._send({
            "type": "delete",
            "id": self._generate_id(),
            "collection": collection,
            "document_id": document_id,
        })
        if resp["type"] == "error":
            raise Exception(resp["error"])
        return Document.from_dict(resp["data"])

    async def list_collections(self) -> list[str]:
        """List all collections"""
        resp = await self._send({"type": "listcollections", "id": self._generate_id()})
        if resp["type"] == "error":
            raise Exception(resp["error"])
        return resp["data"]

    async def ping(self) -> None:
        """Ping the server"""
        resp = await self._send({"type": "ping", "id": self._generate_id()})
        if resp["type"] != "pong":
            raise Exception("Unexpected response")

    async def close(self) -> None:
        """Close the connection"""
        self._closed = True
        self._subscriptions.clear()
        if self._recv_task:
            self._recv_task.cancel()
        if self._ws:
            await self._ws.close()


# Convenience function
connect = SquirrelDB.connect
