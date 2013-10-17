import os
import time
import socket

import zmq

from .base import Test, interactive, passive


TEST_SOCKET = '/tmp/zorro-collectd-test'


class TestCollectd(Test):

    def setUp(self):
        super().setUp()
        import zorro.collectd

    def collectd_putval(self):
        self.z.sleep(0.1)
        sock = self.z.collectd.Connection(unixsock=TEST_SOCKET)
        self.assertEqual((bytearray(b'0 Success'),),
            sock.putval('test/test/test', [('123', '256')], time=1234))
        self.assertEqual((bytearray(b'0 Success'),),
            sock.putnotif("hello", time=1234))
        self.assertEqual((bytearray(b'0 Success'),), sock.flush())
        self.assertEqual({'in': 123, 'out': 234},
            sock.getval('test/test/test'))
        self.assertEqual({'test/test/test': 235, 'test/test/test2': 657},
            dict(sock.listval()))

    @interactive(collectd_putval)
    def test_hello(self):
        if os.access(TEST_SOCKET, os.F_OK):
            os.unlink(TEST_SOCKET)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        with sock:
            sock.bind(TEST_SOCKET)
            sock.listen(1024)
            s, a = sock.accept()
        with s:
            self.assertEqual(s.recv(4096),
                b'PUTVAL test/test/test 1234:123.0:256.0')
            s.sendall(b'0 Success\n')
            self.assertEqual(s.recv(4096),
                b'PUTNOTIF message="hello" severity=warning time=1234')
            s.sendall(b'0 Success\n')
            self.assertEqual(s.recv(4096), b'FLUSH')
            s.sendall(b'0 Success\n')
            self.assertEqual(s.recv(4096), b'GETVAL test/test/test')
            s.sendall(b'2 Lines\nin=123\nout=234\n')
            self.assertEqual(s.recv(4096), b'LISTVAL')
            s.sendall(b'2 Lines\n235 test/test/test\n657 test/test/test2\n')
