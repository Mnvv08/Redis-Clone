import os


class AOF:
    """Append-only file: logs write commands as RESP, replays them on startup."""

    def __init__(self, path: str, fsync: str = "everysec"):
        # fsync policy: "always" (safest, slowest), "everysec" (default), "no"
        self.path = path
        self.fsync = fsync
        self._file = None
        self._dirty = False

    @staticmethod
    def _encode(args: list) -> bytes:
        out = b"*" + str(len(args)).encode() + b"\r\n"
        for a in args:
            out += b"$" + str(len(a)).encode() + b"\r\n" + a + b"\r\n"
        return out

    @staticmethod
    def _parse(data: bytes):
        """Return (commands, good_end). good_end = offset after the last COMPLETE command."""
        commands, pos, good, n = [], 0, 0, len(data)
        while pos < n:
            if data[pos:pos + 1] != b"*":
                break
            end = data.find(b"\r\n", pos)
            if end == -1:
                break
            try:
                count = int(data[pos + 1:end])
            except ValueError:
                break
            pos = end + 2
            args, ok = [], True
            for _ in range(count):
                if data[pos:pos + 1] != b"$":
                    ok = False
                    break
                end = data.find(b"\r\n", pos)
                if end == -1:
                    ok = False
                    break
                try:
                    length = int(data[pos + 1:end])
                except ValueError:
                    ok = False
                    break
                start = end + 2
                stop = start + length
                if stop + 2 > n or data[stop:stop + 2] != b"\r\n":
                    ok = False
                    break
                args.append(data[start:stop])
                pos = stop + 2
            if not ok:
                break
            commands.append(args)
            good = pos
        return commands, good

    def load(self, execute) -> int:
        """Replay the file through execute(). Truncates a torn tail. Returns command count."""
        if not os.path.exists(self.path):
            return 0
        with open(self.path, "rb") as f:
            data = f.read()
        commands, good = self._parse(data)
        for args in commands:
            execute(args)
        if good < len(data):
            print(f"[!] AOF had {len(data) - good} bytes of incomplete data at the end; truncating")
            with open(self.path, "r+b") as f:
                f.truncate(good)
        return len(commands)

    def open(self):
        self._file = open(self.path, "ab")

    def append(self, args: list):
        self._file.write(self._encode(args))
        self._file.flush()                 # hand it to the OS
        if self.fsync == "always":
            os.fsync(self._file.fileno())  # force it onto the disk
        else:
            self._dirty = True

    def sync(self):
        """Called every second: fsync if anything was written since last time."""
        if self.fsync == "everysec" and self._dirty and self._file:
            os.fsync(self._file.fileno())
            self._dirty = False

    def close(self):
        if self._file:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
            self._file = None

    def size(self) -> int:
        return os.path.getsize(self.path) if os.path.exists(self.path) else 0

    def rewrite(self, records):
        """Write `records` to a temp file, fsync it, then atomically replace the log."""
        tmp = self.path + ".tmp"
        with open(tmp, "wb") as f:
            for args in records:
                f.write(self._encode(args))
            f.flush()
            os.fsync(f.fileno())
        if self._file:
            self._file.flush()
            self._file.close()
        os.replace(tmp, self.path)         # atomic on POSIX
        self._file = open(self.path, "ab")
        self._dirty = False
