# -*- coding: utf-8 -*-
from .base import Test, passive

class Redis(Test):

    def setUp(self):
        super().setUp()
        self.r = self.z.redis.plug(self.hub, db=13)
        #if self.r.keys('*'):
        #    raise Exception('Redis database #13 is not empty!')

class SingleThread(Redis):

    @passive
    def test_execute(self):
        self.assertEquals(self.r.execute('SET', 'test:key1', 'value'), 'OK')
        self.assertEquals(self.r.execute('GET', 'test:key1'), b'value')
        self.assertEquals(self.r.execute('DEL', 'test:key1'), 1)

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
        self.assertEquals(self.r.execute('SET', 'test:key1', 'value'), 'OK')
        self.assertEquals(self.r.execute('SET', 'test:key2', 'value'), 'OK')
        self.assertEquals(self.r.execute('KEYS', '*'),
            [b'test:key1', b'test:key2'])
        self.assertEquals(self.r.bulk([('MULTI',),
            ('GET', 'test:key1'),
            ('MGET', 'test:key1', 'test:key2'),
            ('KEYS', '*'),
            ('EXEC',)]), [
            b'value',
            [b'value', b'value'],
            [b'test:key1', b'test:key2'],
            ])
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
        for i in range(100):
            f.append(self.z.Future(get100))
        for i in f:
            i.get()
        print("TOTAL TIME", time.time() - old)

if __name__ == '__main__':
    import unittest
    unittest.main()
