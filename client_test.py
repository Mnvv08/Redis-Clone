import socket

with socket.create_connection(("127.0.0.1", 6380)) as s:
    s.sendall(b"hello minidis")
    print("Server replied:", s.recv(1024))
