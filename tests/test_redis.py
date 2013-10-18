# -*- coding: utf-8 -*-
import socket

from .base import Test, passive


class Redis(Test):

    def setUp(self):
        super().setUp()
        import zorro.redis
        self.r = zorro.redis.Redis(db=13)


class SingleThread(Redis):

    @passive
    def test_execute(self):
        self.assertEquals(self.r.execute('SET', 'test:key1', 'value'), 'OK')
        self.assertEquals(self.r.execute('GET', 'test:key1'), b'value')
        self.assertEquals(self.r.execute('DEL', 'test:key1'), 1)

    @passive
    def test_reconnect(self):
        self.assertEquals(self.r.execute('SET', 'test:key1', 'value'), 'OK')
        self.r._channel._sock.shutdown(socket.SHUT_RDWR)
        self.z.sleep(0.01)
        self.assertEquals(self.r.execute('GET', 'test:key1'), b'value')
        self.assertEquals(self.r.execute('DEL', 'test:key1'), 1)

    @passive
    def test_disconnect(self):
        fut = self.r.future('SET',
            'test:big', b'0123456789abcdef'*1000000)
        self.r._channel._sock.shutdown(socket.SHUT_RDWR)
        self.z.sleep(0.01)
        with self.assertRaises(self.z.channel.PipeError):
            fut.get()

    @passive
    def test_bulk(self):
        self.assertEquals(self.r.bulk([
            ('MULTI',),
            ('SET', 'test:key1', '10'),
            ('INCR', 'test:key1'),
            ('DEL', 'test:key1'),
            ('EXEC',),
            ]), ['OK', 11, 1])

    @passive
    def test_keys(self):
        self.r.execute('DEL', 'test:big')
        self.assertEqual(self.r.execute('SET', 'test:key1', 'value'), 'OK')
        self.assertEqual(self.r.execute('SET', 'test:key2', 'value'), 'OK')
        self.assertEqual(set(map(bytes, self.r.execute('KEYS', '*'))),
            set([b'test:key1', b'test:key2']))
        val = self.r.bulk([('MULTI',),
            ('GET', 'test:key1'),
            ('MGET', 'test:key1', 'test:key2'),
            ('KEYS', '*'),
            ('EXEC',)])
        self.assertEqual(val[0], b'value')
        self.assertEqual(val[1], [b'value', b'value'])
        self.assertSetEqual(set(map(bytes, val[2])),
            set([b'test:key1', b'test:key2']))
        self.assertEquals(self.r.execute('DEL', 'test:key1'), 1)
        self.assertEquals(self.r.execute('DEL', 'test:key2'), 1)


class BigTest(Redis):
    test_timeout = 10

    @passive
    def test_time(self):
        def get100():
            for i in range(100):
                self.r.execute('GET', 'test:key1')
        import time
        old = time.time()
        f = []
        for i in range(200):
            f.append(self.z.Future(get100))
        for i in f:
            i.get()
        print("TOTAL TIME", time.time() - old)


if __name__ == '__main__':
    import unittest
    unittest.main()
