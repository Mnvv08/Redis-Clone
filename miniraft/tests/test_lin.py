import itertools
import random

import pytest

from kv import KVStore
from lin import ABSENT, INF, Op, TooComplex, check, model_step, validate


def op(i, client, seq, kind, invoke, ret, result, **fields):
    """A hand-written operation. ret=None means the client never got a response."""
    return Op(i, client, seq, {"op": kind, **fields}, invoke, INF if ret is None else ret, None if ret is None else result)


def ok(value=None):
    return {"ok": value}


def history(*ops):
    return [Op(i, o.client, o.seq, o.command, o.invoke, o.ret, o.result) for i, o in enumerate(ops)]


def assert_proved(result, ops):
    """A positive answer must come with an order that an independent checker accepts."""
    assert result.ok, result.explain()
    for key, order in result.orders.items():
        good, why = validate(order, [o for o in ops if o.command.get("k") == key])
        assert good, why


# ---------- the specification ----------

def test_the_model_is_a_plain_register():
    s = ABSENT
    s, r = model_step(s, {"op": "get", "k": "x"}); assert r == ok(None) and s == ABSENT
    s, r = model_step(s, {"op": "append", "k": "x", "v": "a"}); assert r == ok("a")
    s, r = model_step(s, {"op": "append", "k": "x", "v": "b"}); assert r == ok("ab")
    s, r = model_step(s, {"op": "cas", "k": "x", "old": "ab", "new": "z"}); assert r == ok(True) and s == ("val", "z")
    s, r = model_step(s, {"op": "cas", "k": "x", "old": "ab", "new": "q"}); assert r == ok(False)
    s, r = model_step(s, {"op": "delete", "k": "x"}); assert r == ok(True) and s == ABSENT
    s, r = model_step(s, {"op": "delete", "k": "x"}); assert r == ok(False)
    s, r = model_step(s, {"op": "put", "k": "x", "v": 5})
    assert model_step(s, {"op": "append", "k": "x", "v": "a"})[1] == {"error": "append needs string values"}
    assert model_step(s, {"op": "nope", "k": "x"})[1]["error"].startswith("unknown operation")
    assert model_step(s, {"op": "get", "k": 3})[1] == {"error": "key must be a string"}


def test_the_model_agrees_with_the_real_store_on_random_sequences():
    rng = random.Random(1)
    for _ in range(200):
        store, state = KVStore(), ABSENT
        for seq in range(1, 30):
            command = {"op": rng.choice(["get", "put", "delete", "cas", "append"]), "k": "x",
                       "v": rng.choice(["a", "b", None, 3]), "old": rng.choice([None, "a", "ab"]), "new": rng.choice([None, "a", "z"])}
            state, expected = model_step(state, command)
            assert store.apply({"c": "me", "s": seq, **command}) == expected, command


# ---------- hand-written histories ----------

def test_empty_and_single_operation_histories():
    assert check([]).ok
    h = history(op(0, "a", 1, "get", 0, 1, ok(None), k="x"))
    assert_proved(check(h), h)
    assert not check(history(op(0, "a", 1, "get", 0, 1, ok("ghost"), k="x"))).ok            # read of a value never written


def test_sequential_history_must_match_the_order_exactly():
    good = history(op(0, "a", 1, "put", 0, 1, ok(), k="x", v=1), op(1, "a", 2, "get", 2, 3, ok(1), k="x"),
                   op(2, "a", 3, "put", 4, 5, ok(), k="x", v=2), op(3, "a", 4, "get", 6, 7, ok(2), k="x"))
    assert_proved(check(good), good)
    stale = history(op(0, "a", 1, "put", 0, 1, ok(), k="x", v=1), op(1, "a", 2, "put", 2, 3, ok(), k="x", v=2),
                    op(2, "b", 1, "get", 4, 5, ok(1), k="x"))
    assert not check(stale).ok                                                                # the old value after the new one


def test_concurrent_operations_may_take_effect_in_either_order():
    for seen in (None, 1):
        h = history(op(0, "a", 1, "put", 0, 10, ok(), k="x", v=1), op(1, "b", 1, "get", 2, 3, ok(seen), k="x"))
        assert_proved(check(h), h)
    assert not check(history(op(0, "a", 1, "put", 0, 10, ok(), k="x", v=1), op(1, "b", 1, "get", 2, 3, ok(2), k="x"))).ok


def test_a_write_that_finished_must_be_seen_by_later_reads():
    h = history(op(0, "a", 1, "put", 0, 5, ok(), k="x", v=1), op(1, "b", 1, "get", 6, 7, ok(None), k="x"))
    result = check(h)
    assert not result.ok
    assert "NOT LINEARIZABLE on key 'x'" in result.explain() and "would have answered {'ok': 1}" in result.explain()


def test_new_old_inversion_is_caught():
    """Two reads one after the other while a write is in flight: once a read has seen the new value
    the next read must not see the old one."""
    h = history(op(0, "w", 1, "put", 0, 100, ok(), k="x", v=1),
                op(1, "r1", 1, "get", 10, 20, ok(1), k="x"), op(2, "r2", 1, "get", 30, 40, ok(None), k="x"))
    assert not check(h).ok
    h = history(op(0, "w", 1, "put", 0, 100, ok(), k="x", v=1),
                op(1, "r1", 1, "get", 10, 20, ok(None), k="x"), op(2, "r2", 1, "get", 30, 40, ok(1), k="x"))
    assert_proved(check(h), h)


def test_readers_must_agree_on_the_order_of_concurrent_writes():
    writes = [op(0, "a", 1, "put", 0, 50, ok(), k="x", v="A"), op(1, "b", 1, "put", 0, 50, ok(), k="x", v="B")]
    reads = lambda *values: [op(2 + i, f"r{i}", 1, "get", 60 + 10 * i, 65 + 10 * i, ok(v), k="x") for i, v in enumerate(values)]
    assert check(history(*writes, *reads("A", "A"))).ok
    assert check(history(*writes, *reads("B", "B"))).ok
    assert not check(history(*writes, *reads("A", "B"))).ok                                  # both writes finished before the reads
    assert not check(history(*writes, *reads("A", "B", "A"))).ok


def test_append_results_pin_down_the_order():
    h = history(op(0, "a", 1, "append", 0, 10, ok("A"), k="x", v="A"), op(1, "b", 1, "append", 0, 10, ok("AB"), k="x", v="B"))
    assert_proved(check(h), h)
    h = history(op(0, "a", 1, "append", 0, 10, ok("A"), k="x", v="A"), op(1, "b", 1, "append", 0, 10, ok("B"), k="x", v="B"))
    assert not check(h).ok                                                                    # both cannot have been first
    h = history(op(0, "a", 1, "append", 0, 10, ok("AB"), k="x", v="A"), op(1, "b", 1, "append", 0, 10, ok("AB"), k="x", v="B"))
    assert not check(h).ok


def test_compare_and_set_and_delete():
    h = history(op(0, "a", 1, "cas", 0, 10, ok(True), k="x", old=None, new="a"), op(1, "b", 1, "cas", 0, 10, ok(True), k="x", old=None, new="b"))
    assert not check(h).ok                                                                    # only one cas can win
    h = history(op(0, "a", 1, "cas", 0, 10, ok(True), k="x", old=None, new="a"), op(1, "b", 1, "cas", 0, 10, ok(False), k="x", old=None, new="b"))
    assert_proved(check(h), h)
    h = history(op(0, "a", 1, "put", 0, 1, ok(), k="x", v=1), op(1, "a", 2, "delete", 2, 3, ok(True), k="x"),
                op(2, "a", 3, "delete", 4, 5, ok(True), k="x"))
    assert not check(h).ok


def test_keys_are_checked_independently_and_the_failure_names_the_key():
    h = history(op(0, "a", 1, "put", 0, 1, ok(), k="good", v=1), op(1, "a", 2, "get", 2, 3, ok(1), k="good"),
                op(2, "a", 3, "put", 4, 5, ok(), k="bad", v=1), op(3, "a", 4, "get", 6, 7, ok(None), k="bad"))
    result = check(h)
    assert not result.ok and result.failure.key == "bad"


def test_touching_intervals_count_as_concurrent():
    h = history(op(0, "a", 1, "put", 0, 5, ok(), k="x", v=1), op(1, "b", 1, "get", 5, 6, ok(None), k="x"))
    assert check(h).ok                                                                        # the same instant: no real-time order
    h = history(op(0, "a", 1, "put", 0, 5, ok(), k="x", v=1), op(1, "b", 1, "get", 5.001, 6, ok(None), k="x"))
    assert not check(h).ok


# ---------- requests nobody heard back from ----------

def test_a_request_with_no_response_may_have_happened_or_not():
    pending = op(0, "a", 1, "put", 0, None, None, k="x", v=1)
    for seen in (None, 1):
        h = history(pending, op(1, "b", 1, "get", 10, 11, ok(seen), k="x"))
        assert_proved(check(h), h)
    assert not check(history(pending, op(1, "b", 1, "get", 10, 11, ok(2), k="x"))).ok


def test_a_request_with_no_response_cannot_unhappen():
    pending = op(0, "a", 1, "put", 0, None, None, k="x", v=1)
    h = history(pending, op(1, "b", 1, "get", 10, 11, ok(1), k="x"), op(2, "b", 2, "get", 20, 21, ok(None), k="x"))
    assert not check(h).ok
    h = history(pending, op(1, "b", 1, "get", 10, 11, ok(None), k="x"), op(2, "b", 2, "get", 20, 21, ok(1), k="x"))
    assert_proved(check(h), h)                                                                # it took effect between the reads


def test_a_request_with_no_response_cannot_take_effect_before_it_was_invoked():
    h = history(op(0, "b", 1, "get", 0, 1, ok(1), k="x"), op(1, "a", 1, "put", 5, None, None, k="x", v=1))
    assert not check(h).ok


def test_pending_reads_are_ignored():
    h = history(op(0, "a", 1, "get", 0, None, None, k="x"), op(1, "b", 1, "put", 1, 2, ok(), k="x", v=1))
    assert check(h).ok


def test_the_state_limit_raises_instead_of_running_forever():
    h = history(*[op(i, f"c{i}", 1, "put", 0, 100, ok(), k="x", v=i) for i in range(12)],
                op(99, "z", 1, "get", 101, 102, ok("nope"), k="x"))     # 12 writes in any order, then an impossible read
    with pytest.raises(TooComplex):
        check(h, limit=50)
    assert not check(h).ok                                                                    # memoisation still finishes it quickly


def test_the_proof_checker_rejects_bad_orders():
    put = op(0, "a", 1, "put", 0, 1, ok(), k="x", v=1)
    get = op(1, "b", 1, "get", 2, 3, ok(1), k="x")
    assert validate([put, get], [put, get]) == (True, "completed operations left out: []")
    good, why = validate([get, put], [put, get])                       # real-time order broken
    assert not good and "returned before" in why
    wrong = op(2, "b", 2, "get", 4, 5, ok(2), k="x")
    good, why = validate([put, wrong], [put, wrong])                   # an answer the model would not give
    assert not good and "the model answers" in why
    good, why = validate([put], [put, get])                            # a completed operation left out
    assert not good and "left out" in why
    pending = op(3, "c", 1, "put", 0, None, None, k="x", v=9)
    assert validate([put, get], [put, get, pending])[0]                # a pending operation may be left out


# ---------- testing the checker against brute force ----------

def brute_force(ops):
    """Try every order of every subset of the pending operations. Slow and obviously right."""
    completed = [o for o in ops if not o.pending]
    pending = [o for o in ops if o.pending and o.command["op"] != "get"]
    for r in range(len(pending) + 1):
        for chosen in itertools.combinations(pending, r):
            members = completed + list(chosen)
            for perm in itertools.permutations(members):
                if any(perm[j].ret < perm[i].invoke for i in range(len(perm)) for j in range(i + 1, len(perm))):
                    continue
                state, good = ABSENT, True
                for o in perm:
                    state, answer = model_step(state, o.command)
                    if not o.pending and answer != o.result:
                        good = False
                        break
                if good:
                    return True
    return False


def random_history(rng):
    ops, replay_state = [], ABSENT
    for i in range(rng.randrange(1, 7)):
        invoke = rng.uniform(0, 20)
        pending = rng.random() < 0.15
        command = {"op": rng.choice(["get", "put", "append", "cas", "delete"]), "k": "x", "v": rng.choice(["a", "b"]),
                   "old": rng.choice([None, "a", "b"]), "new": rng.choice([None, "a"])}
        # answers: usually plausible (taken from a model run in a random order), sometimes wrong
        replay_state, answer = model_step(replay_state, command)
        if rng.random() < 0.25:
            answer = rng.choice([ok(None), ok("a"), ok("b"), ok("ab"), ok(True), ok(False)])
        ops.append(Op(i, f"c{i}", 1, command, invoke, INF if pending else invoke + rng.uniform(0.1, 12), None if pending else answer))
    return ops


def test_the_checker_agrees_with_brute_force_on_random_small_histories():
    rng = random.Random(7)
    verdicts = {True: 0, False: 0}
    for _ in range(600):
        h = random_history(rng)
        expected = brute_force(h)
        result = check(h)
        assert result.ok == expected, "\n".join(str(o) for o in h)
        if expected:
            assert_proved(result, h)
        verdicts[expected] += 1
    assert verdicts[True] > 100 and verdicts[False] > 100                                     # both answers are really exercised
