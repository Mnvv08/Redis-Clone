from pubsub import PubSub


class FakeTransport:
    def __init__(self):
        self.size = 0

    def get_write_buffer_size(self):
        return self.size


class FakeWriter:
    def __init__(self):
        self.written = []
        self.transport = FakeTransport()
        self.closed = False

    def write(self, data):
        self.written.append(data)

    def is_closing(self):
        return self.closed

    def close(self):
        self.closed = True


def test_subscribe_reply_and_count():
    ps, w = PubSub(), FakeWriter()
    reply = ps.subscribe(w, [b"news"])
    assert reply == b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n"
    ps.subscribe(w, [b"sports"])
    assert ps.count(w) == 2


def test_publish_reaches_only_subscribers():
    ps, a, b = PubSub(), FakeWriter(), FakeWriter()
    ps.subscribe(a, [b"news"])
    ps.subscribe(b, [b"sports"])
    assert ps.publish(b"news", b"hi") == 1
    assert a.written == [b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n"]
    assert b.written == []


def test_publish_with_no_subscribers_returns_zero():
    assert PubSub().publish(b"nobody", b"hi") == 0


def test_unsubscribe_one_and_all():
    ps, w = PubSub(), FakeWriter()
    ps.subscribe(w, [b"a", b"b", b"c"])
    ps.unsubscribe(w, [b"a"])
    assert ps.count(w) == 2
    ps.unsubscribe(w, [])                 # no args = everything
    assert ps.count(w) == 0
    assert ps.publish(b"b", b"x") == 0


def test_remove_client_cleans_up_both_maps():
    ps, w = PubSub(), FakeWriter()
    ps.subscribe(w, [b"a", b"b"])
    ps.remove_client(w)
    assert ps._channels == {}
    assert ps._subs == {}


def test_slow_subscriber_is_disconnected():
    ps, slow, fast = PubSub(), FakeWriter(), FakeWriter()
    ps.subscribe(slow, [b"news"])
    ps.subscribe(fast, [b"news"])
    slow.transport.size = 2 * 1024 * 1024    # over the 1 MB cap
    assert ps.publish(b"news", b"hi") == 1   # only the healthy one got it
    assert slow.closed is True
    assert fast.closed is False
    assert slow.written == []
