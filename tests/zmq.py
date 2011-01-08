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

if __name__ == '__main__':
    import unittest
    unittest.main()
