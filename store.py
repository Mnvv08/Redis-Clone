import time


class Store:
    """In-memory key-value store with optional per-key expiry."""

    def __init__(self):
        self._data = {}      # key -> value
        self._expires = {}   # key -> deadline (time.monotonic seconds)

    # ---------- expiry helpers ----------

    def _purge_if_expired(self, key) -> bool:
        """Lazy expiry: delete the key if its deadline has passed."""
        deadline = self._expires.get(key)
        if deadline is not None and time.monotonic() >= deadline:
            self._data.pop(key, None)
            self._expires.pop(key, None)
            return True
        return False

    def purge_expired(self) -> int:
        """Active expiry: sweep all keys, return how many were removed."""
        now = time.monotonic()
        dead = [k for k, d in self._expires.items() if d <= now]
        for k in dead:
            self._data.pop(k, None)
            self._expires.pop(k, None)
        return len(dead)

    # ---------- commands ----------

    def set(self, key, value, ttl=None):
        self._data[key] = value
        if ttl is None:
            self._expires.pop(key, None)   # a plain SET clears any old TTL
        else:
            self._expires[key] = time.monotonic() + ttl

    def get(self, key):
        if self._purge_if_expired(key):
            return None
        return self._data.get(key)

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

    def ttl(self, key) -> int:
        """-2 = key missing, -1 = no expiry, otherwise seconds remaining."""
        if not self.exists(key):
            return -2
        deadline = self._expires.get(key)
        if deadline is None:
            return -1
        return max(0, round(deadline - time.monotonic()))
