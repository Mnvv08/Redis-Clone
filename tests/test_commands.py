import pytest

import server


@pytest.fixture(autouse=True)
def clean_state():
    server.store._data.clear()
    server.store._expires.clear()
    server.store.on_expire = None     # no AOF file is open during tests
    yield


def run(*parts):
    return server.execute([p if isinstance(p, bytes) else p.encode() for p in parts])


def test_ping_and_echo():
    assert run("PING") == b"+PONG\r\n"
    assert run("PING", "hi") == b"$2\r\nhi\r\n"
    assert run("ECHO", "manav") == b"$5\r\nmanav\r\n"


def test_set_and_get():
    assert run("SET", "k", "v") == b"+OK\r\n"
    assert run("GET", "k") == b"$1\r\nv\r\n"
    assert run("GET", "missing") == b"$-1\r\n"


def test_unknown_command_and_wrong_arity():
    assert run("NOPE").startswith(b"-ERR unknown command")
    assert run("GET").startswith(b"-ERR wrong number of arguments")


def test_set_option_validation():
    assert run("SET", "k", "v", "EX", "abc") == b"-ERR value is not an integer or out of range\r\n"
    assert run("SET", "k", "v", "EX", "0").startswith(b"-ERR invalid expire time")
    assert run("SET", "k", "v", "BOGUS").startswith(b"-ERR syntax error")


def test_incr_family():
    assert run("INCR", "c") == b":1\r\n"
    assert run("INCRBY", "c", "5") == b":6\r\n"
    assert run("DECR", "c") == b":5\r\n"
    assert run("DECRBY", "c", "2") == b":3\r\n"
    run("SET", "w", "hi")
    assert run("INCR", "w") == b"-ERR value is not an integer or out of range\r\n"


def test_wrongtype_reply():
    run("RPUSH", "l", "a")
    assert run("GET", "l").startswith(b"-WRONGTYPE")


def test_list_flow():
    assert run("RPUSH", "l", "a", "b", "c") == b":3\r\n"
    assert run("LPUSH", "l", "z") == b":4\r\n"
    assert run("LRANGE", "l", "0", "-1") == b"*4\r\n$1\r\nz\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    assert run("LPOP", "l") == b"$1\r\nz\r\n"
    assert run("RPOP", "l") == b"$1\r\nc\r\n"
    assert run("LLEN", "l") == b":2\r\n"


def test_del_counts_only_existing_keys():
    run("SET", "a", "1")
    run("SET", "b", "2")
    assert run("DEL", "a", "b", "c") == b":2\r\n"
    assert run("EXISTS", "a", "b") == b":0\r\n"


def test_aof_records_turn_relative_ttl_into_absolute():
    records = server.aof_records("SET", [b"SET", b"k", b"v", b"EX", b"10"], b"+OK\r\n")
    assert records[0] == [b"SET", b"k", b"v"]
    assert records[1][0] == b"PEXPIREAT"
    assert records[1][1] == b"k"
    assert int(records[1][2]) > 0


def test_aof_records_skip_failed_expire():
    assert server.aof_records("EXPIRE", [b"EXPIRE", b"k", b"10"], b":0\r\n") == []


def test_snapshot_replay_roundtrip():
    run("SET", "name", "manav")
    run("RPUSH", "l", "a", "b", "c")
    run("SET", "t", "x", "EX", "100")

    records = list(server.snapshot_records())
    server.store._data.clear()
    server.store._expires.clear()
    for r in records:
        server.execute(r)

    assert run("GET", "name") == b"$5\r\nmanav\r\n"
    assert run("LRANGE", "l", "0", "-1") == b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    assert 95 <= server.store.ttl(b"t") <= 100
