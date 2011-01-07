# -*- coding: utf-8 -*-

import time
from .base import Test, passive

class SingleThread(Test):

    def setUp(self):
        super().setUp()
        self.r = self.z.redis.plug(self.hub, db=13)
        #if self.r.keys('*'):
        #    raise Exception('Redis database #13 is not empty!')

    def tearDown(self):
        super().tearDown()

    @passive
    def test_execute(self):
        self.assertEquals(self.r.execute('SET', 'test:key1', 'value'), 'OK')
        self.assertEquals(self.r.execute('GET', 'test:key1'), b'value')
        self.assertEquals(self.r.execute('DEL', 'test:key1'), 1)
