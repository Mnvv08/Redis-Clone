"""A real disk for RaftStorage: the same interface as SimDisk, but backed by actual files.

SimDisk models what a power cut does to unsynced bytes. This class does the real thing: write() hands
bytes to the operating system, sync() makes them durable with fsync, rename() is os.replace plus an
fsync of the directory (without that, the rename itself can be lost in a power cut), and a file that was
just created gets its directory entry synced the first time its data is.

On macOS plain fsync does NOT force data out of the drive's cache; the real guarantee needs
fcntl(F_FULLFSYNC), so that is used there. Windows is not supported.
"""
import os
import sys

try:
    import fcntl
except ImportError:                                       # pragma: no cover
    fcntl = None


class RealDisk:
    def __init__(self, directory):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)
        self._fds = {}                                    # name -> an append-only descriptor
        self._dir_dirty = False                           # a file was created or removed since the last directory sync
        self.owner = None                                 # RaftStorage's owner sets this; SimDisk has the same attribute
        self.stats = {"writes": 0, "syncs": 0, "renames": 0, "dir_syncs": 0}

    def _path(self, name):
        return os.path.join(self.dir, name)

    def _fd(self, name):
        fd = self._fds.get(name)
        if fd is None:
            if not os.path.exists(self._path(name)):
                self._dir_dirty = True
            fd = os.open(self._path(name), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
            self._fds[name] = fd
        return fd

    def _close(self, name):
        fd = self._fds.pop(name, None)
        if fd is not None:
            os.close(fd)

    @staticmethod
    def _flush(fd):
        """Force the file's data to stable storage."""
        if sys.platform == "darwin" and fcntl is not None and hasattr(fcntl, "F_FULLFSYNC"):
            fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
        else:
            os.fsync(fd)

    def _sync_dir(self):
        fd = os.open(self.dir, os.O_RDONLY)
        try:
            self._flush(fd)
        finally:
            os.close(fd)
        self._dir_dirty = False
        self.stats["dir_syncs"] += 1

    # ----- the SimDisk interface

    def write(self, name, data):
        view = memoryview(bytes(data))
        fd = self._fd(name)
        while view:
            view = view[os.write(fd, view):]
        self.stats["writes"] += 1

    def sync(self, name):
        self._flush(self._fd(name))
        if self._dir_dirty:
            self._sync_dir()
        self.stats["syncs"] += 1

    def read(self, name):
        try:
            with open(self._path(name), "rb") as f:
                return f.read()
        except FileNotFoundError:
            return b""

    def rename(self, src, dst):
        """Atomically replace dst with src, then make the rename itself durable."""
        self._close(src)
        self._close(dst)
        os.replace(self._path(src), self._path(dst))
        self._sync_dir()
        self.stats["renames"] += 1

    def remove(self, name):
        self._close(name)
        try:
            os.remove(self._path(name))
        except FileNotFoundError:
            return
        self._dir_dirty = True

    def truncate(self, name, size):
        if not os.path.exists(self._path(name)):
            return
        os.truncate(self._path(name), size)
        self._flush(self._fd(name))

    def crash(self):
        """A real process that dies has no hook to run, so there is nothing to do; this only frees descriptors."""
        self.close()

    def close(self):
        for name in list(self._fds):
            self._close(name)
