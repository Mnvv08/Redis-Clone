import asyncio

import resp
from store import Store

HOST = "127.0.0.1"
PORT = 6380

store = Store()


def parse_int(b: bytes):
    try:
        return int(b)
    except ValueError:
        return None


def wrong_args(name: str) -> bytes:
    return resp.error(f"ERR wrong number of arguments for '{name}' command")


# ---------- command handlers: each takes args (without the command name) ----------

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
                return resp.error("ERR value is not an integer or out of range")
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
        return resp.error("ERR value is not an integer or out of range")
    return resp.integer(1 if store.expire(args[0], n) else 0)


def cmd_ttl(args):
    if len(args) != 1:
        return wrong_args("ttl")
    return resp.integer(store.ttl(args[0]))


COMMANDS = {
    "PING": cmd_ping,
    "ECHO": cmd_echo,
    "SET": cmd_set,
    "GET": cmd_get,
    "DEL": cmd_del,
    "EXISTS": cmd_exists,
    "EXPIRE": cmd_expire,
    "TTL": cmd_ttl,
}


def execute(args: list) -> bytes:
    name = args[0].upper().decode(errors="replace")
    handler = COMMANDS.get(name)
    if handler is None:
        return resp.error(f"ERR unknown command '{name.lower()}'")
    return handler(args[1:])


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
    """Background task: actively delete expired keys once per second."""
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
