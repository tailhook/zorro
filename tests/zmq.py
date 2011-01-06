from .base import Test, interactive, passive
import zmq

class TestZeromq(Test):

    def replier(self, *args):
        self.z.sleep(0.1)
        return reversed(args)
    
    def setup_reply(self):
        sock = self.z.zmq.rep_socket(self.replier)
        sock.connect('tcp://127.0.0.1:9999')

    @interactive(setup_reply)
    def test_req(self):
        ctx = zmq.Context(1)
        sock = ctx.socket(zmq.REQ)
        sock.bind('tcp://127.0.0.1:9999')
        sock.send_multipart([b"hello", b"world"])
        self.assertEquals(sock.recv_multipart(), [b"world", b"hello"])
        
