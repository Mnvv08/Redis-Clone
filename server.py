import asyncio
import time
from collections import deque

import resp
from aof import AOF
from pubsub import PubSub
from store import Store, WrongTypeError

HOST = "127.0.0.1"
PORT = 6380
AOF_PATH = "appendonly.aof"

store = Store()
pubsub = PubSub()
aof = AOF(AOF_PATH, fsync="everysec")
store.on_expire = lambda key: aof.append([b"DEL", key])

# Auto-rewrite when the log is at least this big AND has doubled since the last rewrite
AUTO_REWRITE_MIN_SIZE = 1024 * 1024   # 1 MB
aof_state = {"base": 0}               # log size right after the last rewrite/load

NOT_INT = "ERR value is not an integer or out of range"
WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"

WRITE_COMMANDS = {
    "SET", "DEL", "EXPIRE", "PEXPIREAT", "INCR", "DECR", "INCRBY", "DECRBY",
    "APPEND", "LPUSH", "RPUSH", "LPOP", "RPOP",
}


def parse_int(b: bytes):
    try:
        return int(b)
    except ValueError:
        return None


def wrong_args(name: str) -> bytes:
    return resp.error(f"ERR wrong number of arguments for '{name}' command")


# ---------- connection / generic ----------

def cmd_ping(args):
    if len(args) == 0:
        return resp.simple_string("PONG")
    if len(args) == 1:
        return resp.bulk_string(args[0])
    return wrong_args("ping")


def cmd_echo(args):
    if len(args) != 1:
        return wrong_args("echo")
    return resp.bulk_string(args[0])


def cmd_del(args):
    if len(args) < 1:
        return wrong_args("del")
    return resp.integer(sum(1 for k in args if store.delete(k)))


def cmd_exists(args):
    if len(args) < 1:
        return wrong_args("exists")
    return resp.integer(sum(1 for k in args if store.exists(k)))


def cmd_expire(args):
    if len(args) != 2:
        return wrong_args("expire")
    n = parse_int(args[1])
    if n is None:
        return resp.error(NOT_INT)
    return resp.integer(1 if store.expire(args[0], n) else 0)


def cmd_pexpireat(args):
    if len(args) != 2:
        return wrong_args("pexpireat")
    ms = parse_int(args[1])
    if ms is None:
        return resp.error(NOT_INT)
    return resp.integer(1 if store.pexpireat(args[0], ms) else 0)


def cmd_ttl(args):
    if len(args) != 1:
        return wrong_args("ttl")
    return resp.integer(store.ttl(args[0]))


def cmd_type(args):
    if len(args) != 1:
        return wrong_args("type")
    return resp.simple_string(store.type(args[0]))


def cmd_keys(args):
    if len(args) != 1:
        return wrong_args("keys")
    return resp.array([resp.bulk_string(k) for k in store.keys(args[0])])


def cmd_dbsize(args):
    if args:
        return wrong_args("dbsize")
    return resp.integer(store.dbsize())


# ---------- strings ----------

def cmd_set(args):
    # SET key value [EX seconds | PX milliseconds]
    if len(args) < 2:
        return wrong_args("set")
    key, value = args[0], args[1]
    ttl = None
    i = 2
    while i < len(args):
        opt = args[i].upper()
        if opt in (b"EX", b"PX"):
            if i + 1 >= len(args):
                return resp.error("ERR syntax error")
            n = parse_int(args[i + 1])
            if n is None:
                return resp.error(NOT_INT)
            if n <= 0:
                return resp.error("ERR invalid expire time in 'set' command")
            ttl = n if opt == b"EX" else n / 1000
            i += 2
        else:
            return resp.error("ERR syntax error")
    store.set(key, value, ttl)
    return resp.simple_string("OK")


def cmd_get(args):
    if len(args) != 1:
        return wrong_args("get")
    return resp.bulk_string(store.get(args[0]))


def _incr(args, name, sign):
    n = 1
    if name.endswith("by"):
        if len(args) != 2:
            return wrong_args(name)
        n = parse_int(args[1])
        if n is None:
            return resp.error(NOT_INT)
    elif len(args) != 1:
        return wrong_args(name)
    try:
        return resp.integer(store.incr_by(args[0], sign * n))
    except ValueError:
        return resp.error(NOT_INT)


def cmd_incr(args):
    return _incr(args, "incr", 1)


def cmd_decr(args):
    return _incr(args, "decr", -1)


def cmd_incrby(args):
    return _incr(args, "incrby", 1)


def cmd_decrby(args):
    return _incr(args, "decrby", -1)


def cmd_append(args):
    if len(args) != 2:
        return wrong_args("append")
    return resp.integer(store.append(args[0], args[1]))


def cmd_strlen(args):
    if len(args) != 1:
        return wrong_args("strlen")
    return resp.integer(store.strlen(args[0]))


# ---------- lists ----------

def cmd_lpush(args):
    if len(args) < 2:
        return wrong_args("lpush")
    return resp.integer(store.push(args[0], args[1:], left=True))


def cmd_rpush(args):
    if len(args) < 2:
        return wrong_args("rpush")
    return resp.integer(store.push(args[0], args[1:], left=False))


def cmd_lpop(args):
    if len(args) != 1:
        return wrong_args("lpop")
    return resp.bulk_string(store.pop(args[0], left=True))


def cmd_rpop(args):
    if len(args) != 1:
        return wrong_args("rpop")
    return resp.bulk_string(store.pop(args[0], left=False))


def cmd_llen(args):
    if len(args) != 1:
        return wrong_args("llen")
    return resp.integer(store.llen(args[0]))


def cmd_lrange(args):
    if len(args) != 3:
        return wrong_args("lrange")
    start, stop = parse_int(args[1]), parse_int(args[2])
    if start is None or stop is None:
        return resp.error(NOT_INT)
    items = store.lrange(args[0], start, stop)
    return resp.array([resp.bulk_string(i) for i in items])


# ---------- AOF compaction ----------

def snapshot_records():
    """Yield the minimal commands that rebuild the current dataset."""
    now_ms = int(time.time() * 1000)
    for key, value, remaining in store.dump():
        if isinstance(value, deque):
            yield [b"RPUSH", key, *value]
        else:
            yield [b"SET", key, value]
        if remaining is not None:
            yield [b"PEXPIREAT", key, str(now_ms + int(remaining * 1000)).encode()]


def rewrite_aof():
    before = aof.size()
    aof.rewrite(snapshot_records())
    after = aof.size()
    aof_state["base"] = after
    print(f"[~] AOF rewritten: {before} -> {after} bytes")


def maybe_auto_rewrite():
    size = aof.size()
    if size >= AUTO_REWRITE_MIN_SIZE and size >= 2 * max(aof_state["base"], 1):
        rewrite_aof()


def cmd_rewriteaof(args):
    if args:
        return wrong_args("rewriteaof")
    rewrite_aof()
    return resp.simple_string("OK")


# ---------- pub/sub ----------

def cmd_publish(args):
    if len(args) != 2:
        return wrong_args("publish")
    return resp.integer(pubsub.publish(args[0], args[1]))


def pubsub_dispatch(name, args, writer):
    """Return a reply if pub/sub handles this command, otherwise None."""
    if name == "SUBSCRIBE":
        if not args:
            return wrong_args("subscribe")
        return pubsub.subscribe(writer, args)
    if name == "UNSUBSCRIBE":
        return pubsub.unsubscribe(writer, args)
    if pubsub.count(writer) > 0:
        # a subscribed client may only run a few commands
        if name == "PING":
            msg = args[0] if args else b""
            return resp.array([resp.bulk_string(b"pong"), resp.bulk_string(msg)])
        return resp.error(f"ERR Can't execute '{name.lower()}': only SUBSCRIBE / UNSUBSCRIBE / PING are allowed in this context")
    return None


COMMANDS = {
    "PING": cmd_ping, "ECHO": cmd_echo,
    "DEL": cmd_del, "EXISTS": cmd_exists, "EXPIRE": cmd_expire,
    "PEXPIREAT": cmd_pexpireat, "TTL": cmd_ttl,
    "TYPE": cmd_type, "KEYS": cmd_keys, "DBSIZE": cmd_dbsize,
    "SET": cmd_set, "GET": cmd_get,
    "INCR": cmd_incr, "DECR": cmd_decr, "INCRBY": cmd_incrby, "DECRBY": cmd_decrby,
    "APPEND": cmd_append, "STRLEN": cmd_strlen,
    "LPUSH": cmd_lpush, "RPUSH": cmd_rpush, "LPOP": cmd_lpop, "RPOP": cmd_rpop,
    "LLEN": cmd_llen, "LRANGE": cmd_lrange,
    "REWRITEAOF": cmd_rewriteaof,
    "PUBLISH": cmd_publish,
}


def execute(args: list) -> bytes:
    """Run a command against the store. Does NOT touch the AOF (replay uses this)."""
    name = args[0].upper().decode(errors="replace")
    handler = COMMANDS.get(name)
    if handler is None:
        return resp.error(f"ERR unknown command '{name.lower()}'")
    try:
        return handler(args[1:])
    except WrongTypeError:
        return resp.error(WRONGTYPE)


# ---------- persistence ----------

def aof_records(name: str, args: list, reply: bytes) -> list:
    """Translate a successful write command into what we log.
    Relative TTLs become absolute PEXPIREAT timestamps so replays are deterministic."""
    a = args[1:]
    now_ms = int(time.time() * 1000)

    if name == "SET":
        records = [[b"SET", a[0], a[1]]]
        ttl_ms = None
        i = 2
        while i < len(a):
            n = int(a[i + 1])
            ttl_ms = n * 1000 if a[i].upper() == b"EX" else n
            i += 2
        if ttl_ms is not None:
            records.append([b"PEXPIREAT", a[0], str(now_ms + ttl_ms).encode()])
        return records

    if name == "EXPIRE":
        if reply == b":1\r\n":
            return [[b"PEXPIREAT", a[0], str(now_ms + int(a[1]) * 1000).encode()]]
        return []

    if name == "PEXPIREAT":
        return [args] if reply == b":1\r\n" else []

    return [args]


def process(args: list) -> bytes:
    """Execute a client command and log it to the AOF if it was a successful write."""
    name = args[0].upper().decode(errors="replace")
    reply = execute(args)
    if name in WRITE_COMMANDS and not reply.startswith(b"-"):
        for record in aof_records(name, args, reply):
            aof.append(record)
    return reply


# ---------- networking ----------

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    print(f"[+] Client connected: {addr}")

    try:
        while True:
            args = await resp.read_command(reader)
            if args is None:
                break
            if not args:
                continue
            print(f"[<] {addr}: {args}")
            name = args[0].upper().decode(errors="replace")
            reply = pubsub_dispatch(name, args[1:], writer)
            if reply is None:
                reply = process(args)
            writer.write(reply)
            await writer.drain()
    except resp.ProtocolError as e:
        writer.write(resp.error(f"ERR Protocol error: {e}"))
        await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionResetError):
        pass
    finally:
        pubsub.remove_client(writer)
        print(f"[-] Client disconnected: {addr}")
        writer.close()
        await writer.wait_closed()


async def background_loop():
    """Once per second: expire old keys, fsync the AOF, compact it if needed."""
    while True:
        await asyncio.sleep(1)
        removed = store.purge_expired()
        if removed:
            print(f"[~] Expired {removed} key(s)")
        aof.sync()
        maybe_auto_rewrite()


async def main():
    # 1) rebuild state from disk (expiry paused while replaying)
    store.loading = True
    count = aof.load(execute)
    store.loading = False
    aof.open()
    aof_state["base"] = aof.size()
    store.purge_expired()   # drop keys that expired while the server was down
    print(f"Loaded {count} command(s) from {AOF_PATH}")

    # 2) start serving
    server = await asyncio.start_server(handle_client, HOST, PORT)
    task = asyncio.create_task(background_loop())
    print(f"minidis listening on {HOST}:{PORT}")
    try:
        async with server:
            await server.serve_forever()
    finally:
        task.cancel()
        aof.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
