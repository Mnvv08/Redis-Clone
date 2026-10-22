import argparse
import asyncio
import statistics
import time


def command(*parts) -> bytes:
    out = b"*" + str(len(parts)).encode() + b"\r\n"
    for p in parts:
        p = p if isinstance(p, bytes) else str(p).encode()
        out += b"$" + str(len(p)).encode() + b"\r\n" + p + b"\r\n"
    return out


async def read_reply(reader):
    line = await reader.readline()
    if line[:1] == b"-":
        raise RuntimeError(line)
    if line[:1] == b"$":
        n = int(line[1:])
        if n >= 0:
            await reader.readexactly(n + 2)


async def worker(host, port, kind, wid, n, pipeline, latencies):
    reader, writer = await asyncio.open_connection(host, port)
    value = b"x" * 64
    sent = 0
    while sent < n:
        batch = min(pipeline, n - sent)
        buf = b""
        for i in range(batch):
            key = f"bench:{wid}:{(sent + i) % 1000}"
            buf += command("SET", key, value) if kind == "SET" else command("GET", key)
        t0 = time.perf_counter()
        writer.write(buf)
        await writer.drain()
        for _ in range(batch):
            await read_reply(reader)
        latencies.append((time.perf_counter() - t0) / batch)
        sent += batch
    writer.close()
    await writer.wait_closed()


async def run_phase(args, kind):
    latencies = []
    per_client = args.requests // args.clients
    t0 = time.perf_counter()
    await asyncio.gather(*[
        worker(args.host, args.port, kind, w, per_client, args.pipeline, latencies)
        for w in range(args.clients)
    ])
    elapsed = time.perf_counter() - t0
    total = per_client * args.clients
    lat = sorted(latencies)
    p50 = statistics.median(lat) * 1000
    p99 = lat[int(len(lat) * 0.99) - 1] * 1000
    print(f"{kind:4}  {total / elapsed:>10,.0f} ops/sec   p50 {p50:6.3f} ms   p99 {p99:6.3f} ms   ({total:,} ops in {elapsed:.2f}s)")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6380)
    ap.add_argument("--clients", type=int, default=50)
    ap.add_argument("--requests", type=int, default=100_000)
    ap.add_argument("--pipeline", type=int, default=1)
    args = ap.parse_args()
    print(f"target {args.host}:{args.port}  clients={args.clients}  requests={args.requests:,}  pipeline={args.pipeline}")
    await run_phase(args, "SET")
    await run_phase(args, "GET")


asyncio.run(main())
