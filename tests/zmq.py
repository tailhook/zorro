import time
import pickle

import zmq

from .base import Test, interactive, passive


class TestZeromq(Test):

    def replier(self, *args):
        return reversed(args)

    def setup_reply(self):
        sock = self.z.zmq.rep_socket(self.replier)
        sock.connect('ipc:///tmp/zorro-test-zmq')

    @interactive(setup_reply)
    def test_rep(self):
        ctx = zmq.Context(1)
        sock = ctx.socket(zmq.REQ)
        sock.bind('ipc:///tmp/zorro-test-zmq')
        sock.send_multipart([b"hello", b"world"])
        self.assertEqual(sock.recv_multipart(), [b"world", b"hello"])

    def replieru(self, a):
        if a == b'a':
            return "hello"
        else:
            return b"world"

    def setup_replyu(self):
        sock = self.z.zmq.rep_socket(self.replieru)
        sock.connect('ipc:///tmp/zorro-test-zmq')

    @interactive(setup_replyu)
    def test_repu(self):
        ctx = zmq.Context(1)
        sock = ctx.socket(zmq.REQ)
        sock.bind('ipc:///tmp/zorro-test-zmq')
        sock.send_multipart([b"a"])
        self.assertEqual(sock.recv_multipart(), [b"hello"])
        sock.send_multipart([b"b"])
        self.assertEqual(sock.recv_multipart(), [b"world"])

    def make_request(self):
        sock = self.z.zmq.req_socket()
        sock.connect('tcp://127.0.0.1:9999')
        self.assertEqual(sock.request([b"hello", b"world"]).get(),
            [b"world", b"hello"])
        self.assertEqual(sock.request([b"abra", b"kadabra"]).get(),
            [b"kadabra", b"abra"])

    @interactive(make_request)
    def test_req(self):
        ctx = zmq.Context(1)
        sock = ctx.socket(zmq.REP)
        sock.bind('tcp://127.0.0.1:9999')
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
        self.assertEqual(f.get(),
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
        self.assertEqual(f.get(),
            b"(b'real', b'real', b'real', b'hello', b'world')")


class TestPools(Test):
    timeout = 5

    def setUp(self):
        super().setUp()
        import zorro.pool

    def replier(self, name, time):
        self.data.append(name+b'_start')
        self.z.sleep(float(time))
        self.data.append(name+b'_end')
        return name

    def setup_reply(self):
        sock = self.z.zmq.rep_socket(self.z.pool.Pool(self.replier,
            limit=2,
            timeout=0.2))
        sock.connect('ipc:///tmp/zorro-pool-test')

    @interactive(setup_reply)
    def test_pool_rep(self):
        self.data = []
        ctx = zmq.Context(1)
        sock = ctx.socket(zmq.XREQ)
        sock.bind('ipc:///tmp/zorro-pool-test')
        sock.send_multipart([b'', b'1', b"0.1"])
        sock.send_multipart([b'', b'2', b"0.1"])
        sock.send_multipart([b'', b'3', b"0.1"])
        sock.send_multipart([b'', b'4', b"0.1"])
        self.assertEqual(sock.recv_multipart(), [b'', b"1"])
        self.assertEqual(sock.recv_multipart(), [b'', b"2"])
        self.assertEqual(sock.recv_multipart(), [b'', b"3"])
        self.assertEqual(sock.recv_multipart(), [b'', b"4"])
        self.assertEqual(self.data, [
            b'1_start',
            b'2_start',
            b'1_end',
            b'2_end',
            b'3_start',
            b'4_start',
            b'3_end',
            b'4_end',
            ])

    def subscriber(self, name, time):
        self.data.append(name+b'_start')
        if float(time) > 0.15:
            with self.assertRaises(self.z.pool.TimeoutError):
                self.z.sleep(float(time))
            self.data.append(name+b'_timeout')
        else:
            self.z.sleep(float(time))
        self.data.append(name+b'_end')

    def setup_subscr(self):
        sock = self.z.zmq.pull_socket(self.z.pool.Pool(self.subscriber,
            limit=2,
            timeout=0.15))
        sock.connect('ipc:///tmp/zorro-pool-test')

    @interactive(setup_subscr)
    def test_pool_sub(self):
        self.data = []
        ctx = zmq.Context(1)
        sock = ctx.socket(zmq.PUSH)
        sock.bind('ipc:///tmp/zorro-pool-test')
        sock.send_multipart([b'1', b"0.1"])
        sock.send_multipart([b'2', b"0.1"])
        sock.send_multipart([b'3', b"0.1"])
        sock.send_multipart([b'4', b"0.1"])
        time.sleep(0.5)
        self.assertEqual(self.data, [
            b'1_start',
            b'2_start',
            b'1_end',
            b'2_end',
            b'3_start',
            b'4_start',
            b'3_end',
            b'4_end',
            ])

    @interactive(setup_subscr)
    def test_pool_timeout(self):
        self.data = []
        ctx = zmq.Context(1)
        sock = ctx.socket(zmq.PUSH)
        sock.bind('ipc:///tmp/zorro-pool-test')
        sock.send_multipart([b'1', b"0.1"])
        sock.send_multipart([b'2', b"0.5"])
        sock.send_multipart([b'3', b"0.1"])
        sock.send_multipart([b'4', b"0.1"])
        sock.send_multipart([b'5', b"0.1"])
        sock.send_multipart([b'6', b"0.1"])
        time.sleep(0.7)
        self.assertEqual(self.data, [
            b'1_start',
            b'2_start',
            b'1_end',
            b'3_start',
            b'2_timeout',
            b'2_end',
            b'4_start',
            b'3_end',
            b'5_start',
            b'4_end',
            b'6_start',
            b'5_end',
            b'6_end',
            ])


class TestRPC(Test):

    def setup_svc(self):

        class Responder(self.z.zmq.Responder):

            def __init__(self, name):
                self.name = name

            def hello(self):
                return 'my name is ' + self.name

            def hi(self, name):
                return 'hello, {} from {}'.format(name, self.name)

        sock = self.z.zmq.rep_socket(self.z.zmq.Dispatcher(
            Responder('Ghost'),
            jim=Responder('Jim'),
            pit=Responder('Pit'),
            ))
        sock.connect('ipc:///tmp/zorro-pool-test')


    @interactive(setup_svc)
    def testService(self):
        ctx = zmq.Context(1)
        sock = ctx.socket(zmq.REQ)
        sock.bind('ipc:///tmp/zorro-pool-test')
        sock.send_multipart([b'jim.hello'])
        self.assertEqual(sock.recv_multipart(),
            [b'_result', pickle.dumps('my name is Jim')])
        sock.send_multipart([b'pit.hello'])
        self.assertEqual(sock.recv_multipart(),
            [b'_result', pickle.dumps('my name is Pit')])
        sock.send_multipart([b'jim.hi', pickle.dumps('John')])
        self.assertEqual(sock.recv_multipart(),
            [b'_result', pickle.dumps('hello, John from Jim')])
        sock.send_multipart([b'hi', pickle.dumps('Casper')])
        self.assertEqual(sock.recv_multipart(),
            [b'_result', pickle.dumps('hello, Casper from Ghost')])
        sock.send_multipart([b'test', pickle.dumps('Casper')])
        self.assertEqual(sock.recv_multipart(), [b'_error', b'no_method'])
        sock.send_multipart([b'hello', pickle.dumps(1)])
        exc = sock.recv_multipart()
        self.assertEqual(exc[0], b'_exception')
        # actual message differs between python versions
        self.assertTrue(exc[1].startswith(b"TypeError('hello()"))
        sock.send_multipart([b'_hi', pickle.dumps(b'Casper')])
        self.assertEqual(sock.recv_multipart(), [b'_error', b'bad_name'])

    def setup_req(self):
        sock = self.z.zmq.req_socket()
        sock.connect('ipc:///tmp/zorro-pool-test')
        req = self.z.zmq.Requester(sock)
        req2 = self.z.zmq.Requester(sock, 'jim.')
        self.assertEqual('hi', req.hello('Test'))
        self.assertEqual('hihi', req2.hello('John'))
        with self.assertRaises(self.z.zmq.MethodCallError):
            req2.hello()
        with self.assertRaises(self.z.zmq.MethodException):
            req2.hello_world()


    @interactive(setup_req)
    def testRequests(self):
        ctx = zmq.Context(1)
        sock = ctx.socket(zmq.REP)
        sock.bind('ipc:///tmp/zorro-pool-test')
        self.assertEqual(sock.recv_multipart(),
            [b'hello', pickle.dumps('Test')])
        sock.send_multipart([b'_result', pickle.dumps('hi')])
        self.assertEqual(sock.recv_multipart(),
            [b'jim.hello', pickle.dumps('John')])
        sock.send_multipart([b'_result', pickle.dumps('hihi')])
        self.assertEqual(sock.recv_multipart(), [b'jim.hello'])
        sock.send_multipart([b'_error', b'bad_method'])
        self.assertEqual(sock.recv_multipart(), [b'jim.hello_world'])
        sock.send_multipart([b'_exception',
            repr(ValueError('test')).encode('ascii')])


if __name__ == '__main__':
    import unittest
    unittest.main()
