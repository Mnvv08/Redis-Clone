import asyncio

import resp
from store import Store, WrongTypeError

HOST = "127.0.0.1"
PORT = 6380

store = Store()

NOT_INT = "ERR value is not an integer or out of range"
WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"


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


COMMANDS = {
    "PING": cmd_ping, "ECHO": cmd_echo,
    "DEL": cmd_del, "EXISTS": cmd_exists, "EXPIRE": cmd_expire, "TTL": cmd_ttl,
    "TYPE": cmd_type, "KEYS": cmd_keys, "DBSIZE": cmd_dbsize,
    "SET": cmd_set, "GET": cmd_get,
    "INCR": cmd_incr, "DECR": cmd_decr, "INCRBY": cmd_incrby, "DECRBY": cmd_decrby,
    "APPEND": cmd_append, "STRLEN": cmd_strlen,
    "LPUSH": cmd_lpush, "RPUSH": cmd_rpush, "LPOP": cmd_lpop, "RPOP": cmd_rpop,
    "LLEN": cmd_llen, "LRANGE": cmd_lrange,
}


def execute(args: list) -> bytes:
    name = args[0].upper().decode(errors="replace")
    handler = COMMANDS.get(name)
    if handler is None:
        return resp.error(f"ERR unknown command '{name.lower()}'")
    try:
        return handler(args[1:])
    except WrongTypeError:
        return resp.error(WRONGTYPE)


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
            writer.write(execute(args))
            await writer.drain()
    except resp.ProtocolError as e:
        writer.write(resp.error(f"ERR Protocol error: {e}"))
        await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionResetError):
        pass
    finally:
        print(f"[-] Client disconnected: {addr}")
        writer.close()
        await writer.wait_closed()


async def expiry_loop():
    while True:
        await asyncio.sleep(1)
        removed = store.purge_expired()
        if removed:
            print(f"[~] Expired {removed} key(s)")


async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    expiry_task = asyncio.create_task(expiry_loop())
    print(f"minidis listening on {HOST}:{PORT}")
    try:
        async with server:
            await server.serve_forever()
    finally:
        expiry_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
