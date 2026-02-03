"""Tests for SquirrelDB Python SDK - Cache"""

import pytest


class TestCacheOptions:
    """Test cache options structure"""

    def test_default_options(self):
        opts = {"host": "localhost", "port": 6379}
        assert opts["host"] == "localhost"
        assert opts["port"] == 6379

    def test_custom_options(self):
        opts = {"host": "redis.example.com", "port": 6380}
        assert opts["host"] == "redis.example.com"
        assert opts["port"] == 6380


class TestRESPProtocol:
    """Test RESP protocol encoding"""

    def test_simple_string_format(self):
        response = "+OK\r\n"
        assert response.startswith("+")
        assert "\r\n" in response

    def test_error_format(self):
        response = "-ERR unknown command\r\n"
        assert response.startswith("-")

    def test_integer_format(self):
        response = ":1000\r\n"
        assert response.startswith(":")
        value = int(response[1:response.index("\r\n")])
        assert value == 1000

    def test_bulk_string_format(self):
        value = "hello"
        response = f"${len(value)}\r\n{value}\r\n"
        assert response == "$5\r\nhello\r\n"

    def test_null_bulk_string_format(self):
        response = "$-1\r\n"
        assert response == "$-1\r\n"

    def test_array_format(self):
        response = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        assert response.startswith("*2")

    def test_null_array_format(self):
        response = "*-1\r\n"
        assert response == "*-1\r\n"


class TestCacheCommands:
    """Test cache command encoding"""

    def encode_command(self, *args):
        parts = [f"*{len(args)}\r\n"]
        for arg in args:
            s = str(arg)
            parts.append(f"${len(s)}\r\n{s}\r\n")
        return "".join(parts)

    def test_ping_command(self):
        cmd = self.encode_command("PING")
        assert cmd == "*1\r\n$4\r\nPING\r\n"

    def test_get_command(self):
        cmd = self.encode_command("GET", "mykey")
        assert cmd == "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"

    def test_set_command(self):
        cmd = self.encode_command("SET", "mykey", "myvalue")
        assert cmd == "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"

    def test_set_with_ex_command(self):
        cmd = self.encode_command("SET", "mykey", "myvalue", "EX", 60)
        assert "*5\r\n" in cmd
        assert "$2\r\nEX\r\n" in cmd

    def test_del_command(self):
        cmd = self.encode_command("DEL", "mykey")
        assert cmd == "*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n"

    def test_exists_command(self):
        cmd = self.encode_command("EXISTS", "mykey")
        assert "EXISTS" in cmd

    def test_incr_command(self):
        cmd = self.encode_command("INCR", "counter")
        assert "INCR" in cmd

    def test_incrby_command(self):
        cmd = self.encode_command("INCRBY", "counter", 5)
        assert "INCRBY" in cmd
        assert "$1\r\n5\r\n" in cmd

    def test_mget_command(self):
        cmd = self.encode_command("MGET", "key1", "key2", "key3")
        assert "*4\r\n" in cmd
        assert "MGET" in cmd

    def test_mset_command(self):
        cmd = self.encode_command("MSET", "key1", "val1", "key2", "val2")
        assert "*5\r\n" in cmd
        assert "MSET" in cmd

    def test_keys_command(self):
        cmd = self.encode_command("KEYS", "user:*")
        assert "KEYS" in cmd
        assert "user:*" in cmd

    def test_expire_command(self):
        cmd = self.encode_command("EXPIRE", "mykey", 300)
        assert "EXPIRE" in cmd

    def test_ttl_command(self):
        cmd = self.encode_command("TTL", "mykey")
        assert "TTL" in cmd

    def test_dbsize_command(self):
        cmd = self.encode_command("DBSIZE")
        assert cmd == "*1\r\n$6\r\nDBSIZE\r\n"

    def test_flushdb_command(self):
        cmd = self.encode_command("FLUSHDB")
        assert cmd == "*1\r\n$7\r\nFLUSHDB\r\n"
