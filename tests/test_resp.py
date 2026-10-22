import asyncio

import pytest

import resp


def parse(data: bytes):
    async def run():
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()
        return await resp.read_command(reader)
    return asyncio.run(run())


# ---------- encoders ----------

def test_encoders():
    assert resp.simple_string("OK") == b"+OK\r\n"
    assert resp.error("ERR x") == b"-ERR x\r\n"
    assert resp.integer(-2) == b":-2\r\n"
    assert resp.bulk_string(b"hi") == b"$2\r\nhi\r\n"
    assert resp.bulk_string("hi") == b"$2\r\nhi\r\n"
    assert resp.bulk_string(b"") == b"$0\r\n\r\n"
    assert resp.bulk_string(None) == b"$-1\r\n"


def test_array_encoder():
    assert resp.array([]) == b"*0\r\n"
    assert resp.array([resp.bulk_string(b"a"), resp.integer(1)]) == b"*2\r\n$1\r\na\r\n:1\r\n"


# ---------- parser ----------

def test_parse_array_command():
    assert parse(b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nmanav\r\n") == [b"SET", b"name", b"manav"]


def test_parse_inline_command():
    assert parse(b"PING\r\n") == [b"PING"]
    assert parse(b"SET a 1\r\n") == [b"SET", b"a", b"1"]


def test_parse_is_binary_safe():
    # the value contains \r\n inside it; the length prefix must be what decides
    assert parse(b"*2\r\n$3\r\nSET\r\n$5\r\na\r\nbc\r\n") == [b"SET", b"a\r\nbc"]


def test_parse_disconnect_returns_none():
    assert parse(b"") is None


def test_parse_blank_line_and_empty_array():
    assert parse(b"\r\n") == []
    assert parse(b"*0\r\n") == []


def test_parse_protocol_errors():
    with pytest.raises(resp.ProtocolError):
        parse(b"*x\r\n")
    with pytest.raises(resp.ProtocolError):
        parse(b"*1\r\n+PING\r\n")
    with pytest.raises(resp.ProtocolError):
        parse(b"*1\r\n$-5\r\n")


def test_parse_truncated_payload_raises():
    with pytest.raises(asyncio.IncompleteReadError):
        parse(b"*1\r\n$5\r\nab")
