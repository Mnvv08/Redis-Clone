import socket
import sys

args = sys.argv[1:]
if not args:
    print("usage: python cli.py COMMAND [args...]")
    sys.exit(1)

# Encode as a RESP array of bulk strings
msg = f"*{len(args)}\r\n".encode()
for a in args:
    b = a.encode()
    msg += b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n"

with socket.create_connection(("127.0.0.1", 6380)) as s:
    s.sendall(msg)
    print(s.recv(4096))
