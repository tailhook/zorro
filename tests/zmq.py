from .base import Test, interactive, passive
import zmq

class TestZeromq(Test):

    def replier(self, *args):
        return reversed(args)

    def setup_reply(self):
        sock = self.z.zmq.rep_socket(self.replier)
        sock.connect('tcp://127.0.0.1:9999')

    @interactive(setup_reply)
    def test_rep(self):
        ctx = zmq.Context(1)
        sock = ctx.socket(zmq.REQ)
        sock.bind('tcp://127.0.0.1:9999')
        sock.send_multipart([b"hello", b"world"])
        self.assertEquals(sock.recv_multipart(), [b"world", b"hello"])

    def replieru(self, a):
        if a == b'a':
            return "hello"
        else:
            return b"world"

    def setup_replyu(self):
        sock = self.z.zmq.rep_socket(self.replieru)
        sock.connect('tcp://127.0.0.1:9999')

    @interactive(setup_replyu)
    def test_repu(self):
        ctx = zmq.Context(1)
        sock = ctx.socket(zmq.REQ)
        sock.bind('tcp://127.0.0.1:9999')
        sock.send_multipart([b"a"])
        self.assertEquals(sock.recv_multipart(), [b"hello"])
        sock.send_multipart([b"b"])
        self.assertEquals(sock.recv_multipart(), [b"world"])

    def make_request(self):
        sock = self.z.zmq.req_socket()
        sock.bind('tcp://127.0.0.1:9999')
        self.assertEquals(sock.request([b"hello", b"world"]),
            [b"world", b"hello"])
        self.assertEquals(sock.request([b"abra", b"kadabra"]),
            [b"kadabra", b"abra"])

    @interactive(make_request)
    def test_req(self):
        ctx = zmq.Context(1)
        sock = ctx.socket(zmq.REP)
        sock.connect('tcp://127.0.0.1:9999')
        data = sock.recv_multipart()
        sock.send_multipart(list(reversed(data)))
        data = sock.recv_multipart()
        sock.send_multipart(list(reversed(data)))

    def subscriber(self, *words):
        if len(words) < 5:
            self.pub.publish('real', *words)
        else:
            self.pub2.publish(repr(words))

    @passive
    def test_pubsub(self):
        sock = self.z.zmq.sub_socket(self.subscriber)
        sock.bind('tcp://127.0.0.1:9999')
        sock.setsockopt(self.z.zmq.SUBSCRIBE, b"")
        self.z.sleep(0.1)
        self.pub = self.z.zmq.pub_socket()
        self.pub.connect('tcp://127.0.0.1:9999')
        self.pub.publish('hello', 'world')
        f = self.z.Future()
        sock = self.z.zmq.sub_socket(f.set)
        sock.bind('tcp://127.0.0.1:9998')
        sock.setsockopt(self.z.zmq.SUBSCRIBE, b"")
        self.pub2 = self.z.zmq.pub_socket()
        self.pub2.connect('tcp://127.0.0.1:9998')
        self.assertEquals(f.get(),
            b"(b'real', b'real', b'real', b'hello', b'world')")

    def puller(self, *words):
        if len(words) < 5:
            self.push.push('real', *words)
        else:
            self.push2.push(repr(words))

    @passive
    def test_pushpull(self):
        sock = self.z.zmq.pull_socket(self.puller)
        sock.bind('tcp://127.0.0.1:9999')
        self.z.sleep(0.1)
        self.push = self.z.zmq.push_socket()
        self.push.connect('tcp://127.0.0.1:9999')
        self.push.push('hello', 'world')
        f = self.z.Future()
        sock = self.z.zmq.pull_socket(f.set)
        sock.bind('tcp://127.0.0.1:9998')
        self.push2 = self.z.zmq.push_socket()
        self.push2.connect('tcp://127.0.0.1:9998')
        self.assertEquals(f.get(),
            b"(b'real', b'real', b'real', b'hello', b'world')")

if __name__ == '__main__':
    import unittest
    unittest.main()
