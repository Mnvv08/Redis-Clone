import socket
import sys


def read_reply(f):
    line = f.readline()
    if not line:
        return None
    kind, rest = line[:1], line[1:-2]
    if kind == b"*":
        return [read_reply(f) for _ in range(int(rest))]
    if kind == b"$":
        n = int(rest)
        if n < 0:
            return None
        return f.read(n + 2)[:-2].decode(errors="replace")
    if kind == b":":
        return int(rest)
    return rest.decode(errors="replace")


channels = sys.argv[1:]
if not channels:
    print("usage: python subscriber.py channel [channel ...]")
    sys.exit(1)

args = [b"SUBSCRIBE"] + [c.encode() for c in channels]
msg = f"*{len(args)}\r\n".encode()
for a in args:
    msg += b"$" + str(len(a)).encode() + b"\r\n" + a + b"\r\n"

sock = socket.create_connection(("127.0.0.1", 6380))
sock.sendall(msg)
f = sock.makefile("rb")
print(f"listening on {channels} (Ctrl+C to quit)")
try:
    while True:
        reply = read_reply(f)
        if reply is None:
            print("server closed the connection")
            break
        print(reply)
except KeyboardInterrupt:
    pass
