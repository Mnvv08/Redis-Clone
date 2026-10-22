import time

import pytest

from store import Store, WrongTypeError


def test_set_and_get():
    s = Store()
    s.set(b"a", b"1")
    assert s.get(b"a") == b"1"


def test_get_missing_returns_none():
    assert Store().get(b"nope") is None


def test_delete_and_exists():
    s = Store()
    s.set(b"a", b"1")
    assert s.exists(b"a")
    assert s.delete(b"a") is True
    assert not s.exists(b"a")
    assert s.delete(b"a") is False


def test_lazy_expiry():
    s = Store()
    s.set(b"k", b"v", ttl=0.05)
    time.sleep(0.1)
    assert s.get(b"k") is None
    assert not s.exists(b"k")


def test_active_expiry_sweep():
    s = Store()
    s.set(b"k", b"v", ttl=0.05)
    time.sleep(0.1)
    assert s.purge_expired() == 1
    assert s.dbsize() == 0


def test_on_expire_callback_fires():
    s = Store()
    expired = []
    s.on_expire = expired.append
    s.set(b"k", b"v", ttl=0.05)
    time.sleep(0.1)
    s.purge_expired()
    assert expired == [b"k"]


def test_plain_set_clears_ttl():
    s = Store()
    s.set(b"k", b"v", ttl=100)
    assert 0 < s.ttl(b"k") <= 100
    s.set(b"k", b"v2")
    assert s.ttl(b"k") == -1


def test_ttl_special_values():
    s = Store()
    assert s.ttl(b"missing") == -2
    s.set(b"k", b"v")
    assert s.ttl(b"k") == -1


def test_expire_missing_key_returns_false():
    assert Store().expire(b"nope", 10) is False


def test_pexpireat_sets_absolute_deadline():
    s = Store()
    s.set(b"k", b"v")
    assert s.pexpireat(b"k", int((time.time() + 100) * 1000))
    assert 98 <= s.ttl(b"k") <= 100


def test_loading_mode_pauses_expiry():
    s = Store()
    s.set(b"k", b"v", ttl=0.05)
    time.sleep(0.1)
    s.loading = True
    assert s.get(b"k") == b"v"      # not purged while replaying the AOF
    s.loading = False
    assert s.get(b"k") is None


def test_incr_by():
    s = Store()
    assert s.incr_by(b"c", 1) == 1
    assert s.incr_by(b"c", 5) == 6
    assert s.incr_by(b"c", -2) == 4
    assert s.get(b"c") == b"4"


def test_incr_non_integer_raises():
    s = Store()
    s.set(b"w", b"hi")
    with pytest.raises(ValueError):
        s.incr_by(b"w", 1)


def test_incr_overflow_raises():
    s = Store()
    s.set(b"big", str(2 ** 63 - 1).encode())
    with pytest.raises(ValueError):
        s.incr_by(b"big", 1)


def test_incr_keeps_ttl():
    s = Store()
    s.set(b"c", b"1", ttl=100)
    s.incr_by(b"c", 1)
    assert s.ttl(b"c") > 0


def test_append_and_strlen():
    s = Store()
    assert s.append(b"g", b"hello") == 5
    assert s.append(b"g", b" world") == 11
    assert s.get(b"g") == b"hello world"
    assert s.strlen(b"g") == 11
    assert s.strlen(b"missing") == 0


def test_list_push_pop():
    s = Store()
    assert s.push(b"l", [b"a", b"b", b"c"], left=False) == 3
    assert s.push(b"l", [b"z"], left=True) == 4
    assert s.lrange(b"l", 0, -1) == [b"z", b"a", b"b", b"c"]
    assert s.pop(b"l", left=True) == b"z"
    assert s.pop(b"l", left=False) == b"c"
    assert s.llen(b"l") == 2


def test_lrange_indices():
    s = Store()
    s.push(b"l", [b"a", b"b", b"c", b"d", b"e"], left=False)
    assert s.lrange(b"l", 1, 2) == [b"b", b"c"]
    assert s.lrange(b"l", -2, -1) == [b"d", b"e"]
    assert s.lrange(b"l", 3, 100) == [b"d", b"e"]
    assert s.lrange(b"l", 10, 20) == []
    assert s.lrange(b"l", 2, 1) == []
    assert s.lrange(b"missing", 0, -1) == []


def test_empty_list_is_removed():
    s = Store()
    s.push(b"l", [b"a"], left=False)
    assert s.pop(b"l", left=True) == b"a"
    assert not s.exists(b"l")
    assert s.pop(b"l", left=True) is None


def test_wrongtype_errors():
    s = Store()
    s.push(b"l", [b"a"], left=False)
    with pytest.raises(WrongTypeError):
        s.get(b"l")
    s.set(b"str", b"x")
    with pytest.raises(WrongTypeError):
        s.push(b"str", [b"a"], left=False)
    with pytest.raises(WrongTypeError):
        s.llen(b"str")


def test_type():
    s = Store()
    s.set(b"s", b"x")
    s.push(b"l", [b"a"], left=False)
    assert s.type(b"s") == "string"
    assert s.type(b"l") == "list"
    assert s.type(b"nope") == "none"


def test_keys_pattern():
    s = Store()
    for k in (b"a1", b"a2", b"b1"):
        s.set(k, b"x")
    assert sorted(s.keys(b"a*")) == [b"a1", b"a2"]
    assert len(s.keys(b"*")) == 3


def test_dump_includes_remaining_ttl():
    s = Store()
    s.set(b"a", b"1")
    s.set(b"b", b"2", ttl=100)
    d = {k: (v, r) for k, v, r in s.dump()}
    assert d[b"a"] == (b"1", None)
    assert d[b"b"][0] == b"2"
    assert 99 <= d[b"b"][1] <= 100
