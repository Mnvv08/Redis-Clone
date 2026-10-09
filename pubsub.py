import resp

MAX_OUTPUT_BUFFER = 1024 * 1024   # disconnect subscribers that fall > 1 MB behind


class PubSub:
    def __init__(self):
        self._channels = {}   # channel (bytes) -> set of writers
        self._subs = {}       # writer -> set of channels

    @staticmethod
    def _reply(kind: bytes, channel, count: int) -> bytes:
        return resp.array([
            resp.bulk_string(kind),
            resp.bulk_string(channel),
            resp.integer(count),
        ])

    def count(self, writer) -> int:
        return len(self._subs.get(writer, ()))

    def subscribe(self, writer, channels) -> bytes:
        out = b""
        for ch in channels:
            self._channels.setdefault(ch, set()).add(writer)
            self._subs.setdefault(writer, set()).add(ch)
            out += self._reply(b"subscribe", ch, len(self._subs[writer]))
        return out

    def _drop(self, writer, channel):
        members = self._channels.get(channel)
        if members is not None:
            members.discard(writer)
            if not members:
                del self._channels[channel]
        chans = self._subs.get(writer)
        if chans is not None:
            chans.discard(channel)
            if not chans:
                del self._subs[writer]

    def unsubscribe(self, writer, channels) -> bytes:
        # no arguments = unsubscribe from everything
        channels = list(channels) if channels else list(self._subs.get(writer, ()))
        if not channels:
            return self._reply(b"unsubscribe", None, 0)
        out = b""
        for ch in channels:
            self._drop(writer, ch)
            out += self._reply(b"unsubscribe", ch, len(self._subs.get(writer, ())))
        return out

    def remove_client(self, writer):
        for ch in list(self._subs.get(writer, ())):
            self._drop(writer, ch)

    def publish(self, channel: bytes, message: bytes) -> int:
        subs = self._channels.get(channel)
        if not subs:
            return 0
        payload = resp.array([
            resp.bulk_string(b"message"),
            resp.bulk_string(channel),
            resp.bulk_string(message),
        ])
        delivered = 0
        for w in list(subs):
            if w.is_closing():
                self.remove_client(w)
                continue
            if w.transport.get_write_buffer_size() > MAX_OUTPUT_BUFFER:
                print("[!] slow subscriber, disconnecting")
                self.remove_client(w)
                w.close()
                continue
            w.write(payload)       # no drain: one slow client must not block the others
            delivered += 1
        return delivered
