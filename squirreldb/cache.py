"""Redis-compatible async cache client using RESP protocol over TCP."""

import asyncio
from typing import Any, Dict, List, Optional


class CacheError(Exception):
    """Base exception for cache errors."""
    pass


class ConnectionError(CacheError):
    """Connection-related errors."""
    pass


class ProtocolError(CacheError):
    """RESP protocol errors."""
    pass


def encode_resp(*args: Any) -> bytes:
    """Encode arguments as RESP array."""
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        s = str(arg)
        parts.append(f"${len(s)}\r\n{s}\r\n".encode())
    return b"".join(parts)


class RespParser:
    """RESP protocol parser."""

    def __init__(self, reader: asyncio.StreamReader):
        self._reader = reader

    async def read_line(self) -> str:
        """Read a line ending with CRLF."""
        line = await self._reader.readline()
        if not line:
            raise ConnectionError("Connection closed")
        return line.decode().rstrip("\r\n")

    async def parse(self) -> Any:
        """Parse a RESP response."""
        line = await self.read_line()
        if not line:
            raise ProtocolError("Empty response")

        prefix = line[0]
        data = line[1:]

        if prefix == "+":
            return data
        elif prefix == "-":
            raise CacheError(data)
        elif prefix == ":":
            return int(data)
        elif prefix == "$":
            length = int(data)
            if length == -1:
                return None
            content = await self._reader.readexactly(length + 2)
            return content[:-2].decode()
        elif prefix == "*":
            count = int(data)
            if count == -1:
                return None
            return [await self.parse() for _ in range(count)]
        else:
            raise ProtocolError(f"Unknown RESP type: {prefix}")


class Cache:
    """Redis-compatible cache client for SquirrelDB."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
    ):
        self._host = host
        self._port = port
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._parser: Optional[RespParser] = None
        self._lock = asyncio.Lock()
        self._closed = False

    @classmethod
    async def connect(
        cls,
        host: str = "localhost",
        port: int = 6379,
    ) -> "Cache":
        """Connect to cache server."""
        client = cls(host, port)
        await client._connect()
        return client

    async def _connect(self) -> None:
        """Establish TCP connection."""
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port
        )
        self._parser = RespParser(self._reader)

    async def _execute(self, *args: Any) -> Any:
        """Execute a command and return the response."""
        if not self._writer or not self._parser:
            raise ConnectionError("Not connected")

        async with self._lock:
            self._writer.write(encode_resp(*args))
            await self._writer.drain()
            return await self._parser.parse()

    async def get(self, key: str) -> Optional[str]:
        """Get the value of a key."""
        return await self._execute("GET", key)

    async def set(
        self,
        key: str,
        value: str,
        ttl: Optional[int] = None,
    ) -> None:
        """Set a key to a value with optional TTL in seconds."""
        if ttl is not None:
            await self._execute("SET", key, value, "EX", ttl)
        else:
            await self._execute("SET", key, value)

    async def delete(self, key: str) -> bool:
        """Delete a key. Returns True if the key existed."""
        result = await self._execute("DEL", key)
        return result > 0

    async def exists(self, key: str) -> bool:
        """Check if a key exists."""
        result = await self._execute("EXISTS", key)
        return result > 0

    async def expire(self, key: str, seconds: int) -> bool:
        """Set a timeout on a key. Returns True if timeout was set."""
        result = await self._execute("EXPIRE", key, seconds)
        return result == 1

    async def ttl(self, key: str) -> int:
        """Get the TTL of a key in seconds. Returns -1 if no TTL, -2 if key doesn't exist."""
        return await self._execute("TTL", key)

    async def persist(self, key: str) -> bool:
        """Remove the timeout on a key. Returns True if timeout was removed."""
        result = await self._execute("PERSIST", key)
        return result == 1

    async def incr(self, key: str) -> int:
        """Increment the value of a key by 1."""
        return await self._execute("INCR", key)

    async def decr(self, key: str) -> int:
        """Decrement the value of a key by 1."""
        return await self._execute("DECR", key)

    async def incrby(self, key: str, amount: int) -> int:
        """Increment the value of a key by the given amount."""
        return await self._execute("INCRBY", key, amount)

    async def keys(self, pattern: str = "*") -> List[str]:
        """Get all keys matching a pattern."""
        result = await self._execute("KEYS", pattern)
        return result if result else []

    async def mget(self, *keys: str) -> List[Optional[str]]:
        """Get values of multiple keys."""
        if not keys:
            return []
        result = await self._execute("MGET", *keys)
        return result if result else []

    async def mset(self, pairs: Dict[str, str]) -> None:
        """Set multiple key-value pairs."""
        if not pairs:
            return
        args = ["MSET"]
        for k, v in pairs.items():
            args.extend([k, v])
        await self._execute(*args)

    async def dbsize(self) -> int:
        """Get the number of keys in the database."""
        return await self._execute("DBSIZE")

    async def flush(self) -> None:
        """Delete all keys in the current database."""
        await self._execute("FLUSHDB")

    async def info(self) -> Dict[str, Any]:
        """Get server information."""
        result = await self._execute("INFO")
        if not result:
            return {}
        info: Dict[str, Any] = {}
        section = ""
        for line in result.split("\r\n"):
            if not line:
                continue
            if line.startswith("#"):
                section = line[2:].strip().lower()
                info[section] = {}
            elif ":" in line:
                key, value = line.split(":", 1)
                if section:
                    info[section][key] = value
                else:
                    info[key] = value
        return info

    async def ping(self) -> bool:
        """Ping the server. Returns True if successful."""
        result = await self._execute("PING")
        return result == "PONG"

    async def close(self) -> None:
        """Close the connection."""
        self._closed = True
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
        self._reader = None
        self._writer = None
        self._parser = None

    async def __aenter__(self) -> "Cache":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()
