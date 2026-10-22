import fnmatch
import time
from collections import deque

INT_MIN = -(2 ** 63)
INT_MAX = 2 ** 63 - 1


class WrongTypeError(Exception):
    """Operation used on a key holding a different type."""


class Store:
    """In-memory store. A value is either bytes (string) or a deque (list)."""

    def __init__(self):
        self._data = {}        # key -> bytes | deque
        self._expires = {}     # key -> deadline (time.monotonic seconds)
        self.loading = False   # True while replaying the AOF (expiry paused)
        self.on_expire = None  # callback(key) fired when a key expires

    # ---------- expiry helpers ----------

    def _purge_if_expired(self, key) -> bool:
        if self.loading:
            return False
        deadline = self._expires.get(key)
        if deadline is not None and time.monotonic() >= deadline:
            self._data.pop(key, None)
            self._expires.pop(key, None)
            if self.on_expire:
                self.on_expire(key)
            return True
        return False

    def purge_expired(self) -> int:
        now = time.monotonic()
        dead = [k for k, d in self._expires.items() if d <= now]
        for k in dead:
            self._data.pop(k, None)
            self._expires.pop(k, None)
            if self.on_expire:
                self.on_expire(k)
        return len(dead)

    # ---------- generic key commands ----------

    def set(self, key, value, ttl=None):
        self._data[key] = value          # SET overwrites any type
        if ttl is None:
            self._expires.pop(key, None)
        else:
            self._expires[key] = time.monotonic() + ttl

    def delete(self, key) -> bool:
        if self._purge_if_expired(key):
            return False
        existed = key in self._data
        self._data.pop(key, None)
        self._expires.pop(key, None)
        return existed

    def exists(self, key) -> bool:
        if self._purge_if_expired(key):
            return False
        return key in self._data

    def expire(self, key, seconds) -> bool:
        if not self.exists(key):
            return False
        self._expires[key] = time.monotonic() + seconds
        return True

    def pexpireat(self, key, unix_ms) -> bool:
        """Expire at an absolute unix time in milliseconds (used by AOF replay)."""
        if not self.exists(key):
            return False
        remaining = unix_ms / 1000 - time.time()
        self._expires[key] = time.monotonic() + remaining
        return True

    def ttl(self, key) -> int:
        """-2 = key missing, -1 = no expiry, otherwise seconds remaining."""
        if not self.exists(key):
            return -2
        deadline = self._expires.get(key)
        if deadline is None:
            return -1
        return max(0, round(deadline - time.monotonic()))

    def type(self, key) -> str:
        if not self.exists(key):
            return "none"
        return "list" if isinstance(self._data[key], deque) else "string"

    def keys(self, pattern: bytes) -> list:
        self.purge_expired()
        pat = pattern.decode(errors="replace")
        return [k for k in self._data
                if fnmatch.fnmatchcase(k.decode(errors="replace"), pat)]

    def dbsize(self) -> int:
        self.purge_expired()
        return len(self._data)

    # ---------- string commands ----------

    def get(self, key):
        if self._purge_if_expired(key):
            return None
        value = self._data.get(key)
        if value is not None and not isinstance(value, bytes):
            raise WrongTypeError
        return value

    def incr_by(self, key, delta: int) -> int:
        """Raises ValueError if the stored value isn't a valid integer."""
        current = self.get(key)
        n = 0 if current is None else int(current)
        n += delta
        if not (INT_MIN <= n <= INT_MAX):
            raise ValueError("overflow")
        self._data[key] = str(n).encode()   # keeps any existing TTL
        return n

    def append(self, key, value: bytes) -> int:
        current = self.get(key) or b""
        new = current + value
        self._data[key] = new
        return len(new)

    def strlen(self, key) -> int:
        return len(self.get(key) or b"")

    # ---------- list commands ----------

    def _get_list(self, key):
        if self._purge_if_expired(key):
            return None
        value = self._data.get(key)
        if value is not None and not isinstance(value, deque):
            raise WrongTypeError
        return value

    def push(self, key, values, left: bool) -> int:
        lst = self._get_list(key)
        if lst is None:
            lst = deque()
            self._data[key] = lst
        for v in values:
            lst.appendleft(v) if left else lst.append(v)
        return len(lst)

    def pop(self, key, left: bool):
        lst = self._get_list(key)
        if lst is None:
            return None
        value = lst.popleft() if left else lst.pop()
        if not lst:                      # Redis removes empty lists
            self.delete(key)
        return value

    def llen(self, key) -> int:
        lst = self._get_list(key)
        return 0 if lst is None else len(lst)

    def lrange(self, key, start: int, stop: int) -> list:
        lst = self._get_list(key)
        if lst is None:
            return []
        n = len(lst)
        if start < 0:
            start = max(n + start, 0)
        if stop < 0:
            stop = n + stop
        if start > stop or start >= n:
            return []
        stop = min(stop, n - 1)
        return list(lst)[start:stop + 1]

    # ---------- snapshot (used by AOF rewrite) ----------

    def dump(self):
        """Return [(key, value, seconds_remaining_or_None)] for all live keys."""
        self.purge_expired()
        now = time.monotonic()
        out = []
        for key, value in list(self._data.items()):
            deadline = self._expires.get(key)
            remaining = None if deadline is None else max(0.0, deadline - now)
            out.append((key, value, remaining))
        return out
