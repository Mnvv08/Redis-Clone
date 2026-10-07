import asyncio

import resp

HOST = "127.0.0.1"
PORT = 6380


def execute(args: list) -> bytes:
    """Take a parsed command and return the RESP reply."""
    cmd = args[0].upper().decode(errors="replace")

    if cmd == "PING":
        if len(args) == 1:
            return resp.simple_string("PONG")
        if len(args) == 2:
            return resp.bulk_string(args[1])
        return resp.error("ERR wrong number of arguments for 'ping' command")

    if cmd == "ECHO":
        if len(args) != 2:
            return resp.error("ERR wrong number of arguments for 'echo' command")
        return resp.bulk_string(args[1])

    return resp.error(f"ERR unknown command '{cmd.lower()}'")


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    print(f"[+] Client connected: {addr}")

    try:
        while True:
            args = await resp.read_command(reader)
            if args is None:      # disconnected
                break
            if not args:          # blank line, ignore
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


async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    print(f"minidis listening on {HOST}:{PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
