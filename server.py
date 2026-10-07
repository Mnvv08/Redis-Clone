import asyncio

HOST = "127.0.0.1"
PORT = 6380


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    print(f"[+] Client connected: {addr}")

    try:
        while True:
            data = await reader.read(1024)   # wait for bytes from the client
            if not data:                     # empty bytes = client disconnected
                break
            print(f"[<] {addr} sent: {data!r}")
            writer.write(data)               # echo back for now
            await writer.drain()
    except ConnectionResetError:
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
