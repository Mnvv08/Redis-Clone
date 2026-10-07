import asyncio


class ProtocolError(Exception):
    """Raised when a client sends malformed RESP."""


async def read_command(reader: asyncio.StreamReader):
    """
    Read one command from the client.
    Returns a list of bytes (e.g. [b"SET", b"name", b"manav"]),
    an empty list for a blank line, or None if the client disconnected.
    """
    line = await reader.readline()
    if not line:
        return None  # client closed the connection

    # RESP array: *<count>\r\n followed by <count> bulk strings
    if line[:1] == b"*":
        try:
            count = int(line[1:].strip())
        except ValueError:
            raise ProtocolError("invalid multibulk length")
        if count <= 0:
            return []

        args = []
        for _ in range(count):
            header = await reader.readline()
            if not header:
                return None
            if header[:1] != b"$":
                raise ProtocolError(f"expected '$', got {header[:1]!r}")
            try:
                length = int(header[1:].strip())
            except ValueError:
                raise ProtocolError("invalid bulk length")
            if length < 0 or length > 512 * 1024 * 1024:
                raise ProtocolError("invalid bulk length")
            data = await reader.readexactly(length + 2)  # payload + \r\n
            args.append(data[:-2])
        return args

    # Inline command, e.g. typed by hand in telnet/nc: "PING\r\n"
    return line.strip().split()


# ---------- Encoders: Python value -> RESP bytes ----------

def simple_string(s: str) -> bytes:
    return f"+{s}\r\n".encode()


def error(msg: str) -> bytes:
    return f"-{msg}\r\n".encode()


def integer(n: int) -> bytes:
    return f":{n}\r\n".encode()


def bulk_string(b) -> bytes:
    if b is None:
        return b"$-1\r\n"  # RESP "null"
    if isinstance(b, str):
        b = b.encode()
    return b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n"


def array(encoded_items: list) -> bytes:
    return b"*" + str(len(encoded_items)).encode() + b"\r\n" + b"".join(encoded_items)
