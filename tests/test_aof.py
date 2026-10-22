from aof import AOF

SET_A = [b"SET", b"a", b"1"]


def test_encode_parse_roundtrip():
    cmds = [SET_A, [b"INCR", b"c"], [b"SET", b"bin", b"a\r\nb"]]
    data = b"".join(AOF._encode(c) for c in cmds)
    parsed, good = AOF._parse(data)
    assert parsed == cmds
    assert good == len(data)


def test_parse_stops_at_torn_tail():
    full = AOF._encode(SET_A)
    torn = AOF._encode([b"SET", b"b", b"2"])[:-5]
    parsed, good = AOF._parse(full + torn)
    assert parsed == [SET_A]
    assert good == len(full)


def test_load_replays_and_truncates_torn_tail(tmp_path):
    path = tmp_path / "t.aof"
    full = AOF._encode(SET_A)
    path.write_bytes(full + b"*3\r\n$3\r\nSET\r\n$1\r\nx")   # simulated crash mid-write
    seen = []
    n = AOF(str(path)).load(seen.append)
    assert n == 1
    assert seen == [SET_A]
    assert path.read_bytes() == full


def test_load_missing_file_is_empty(tmp_path):
    assert AOF(str(tmp_path / "none.aof")).load(lambda a: None) == 0


def test_append_then_reload(tmp_path):
    path = str(tmp_path / "t.aof")
    aof = AOF(path, fsync="always")
    aof.open()
    aof.append(SET_A)
    aof.append([b"INCR", b"c"])
    aof.close()
    seen = []
    assert AOF(path).load(seen.append) == 2
    assert seen == [SET_A, [b"INCR", b"c"]]


def test_rewrite_compacts_and_keeps_appending(tmp_path):
    path = str(tmp_path / "t.aof")
    aof = AOF(path)
    aof.open()
    for _ in range(100):
        aof.append([b"INCR", b"c"])
    before = aof.size()

    aof.rewrite([[b"SET", b"c", b"100"]])
    assert aof.size() < before

    aof.append([b"INCR", b"c"])      # must go to the NEW file
    aof.close()

    seen = []
    AOF(path).load(seen.append)
    assert seen == [[b"SET", b"c", b"100"], [b"INCR", b"c"]]
