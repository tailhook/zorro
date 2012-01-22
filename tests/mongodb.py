# -*- coding: utf-8 -*-
import socket
from functools import partial

from .base import Test, passive


class Mongodb(Test):

    def setUp(self):
        super().setUp()
        import zorro.mongodb
        self.m = zorro.mongodb.Connection()
        self.c = self.m['test']['test_collection']

    def query_data(self, *args, **kw):
        lst = list(self.c.query(*args, **kw))
        for i in lst: i.pop('_id')
        return lst


class Simple(Mongodb):

    @passive
    def test_basic(self):
        self.c.clean()
        self.c.insert({'hello': 'world'})
        self.assertEqual([{'hello': 'world'}],
            self.query_data({'hello': 'world'}))

    @passive
    def test_disconnect(self):
        self.c.clean()
        self.c.insert({'hello': 'world'})
        fut1 = self.z.Future(partial(self.query_data, {'hello': 'world'}))
        fut2 = self.z.Future(partial(self.query_data, {'hello': 'world'}))
        self.c._conn._channel._sock.shutdown(socket.SHUT_RDWR)
        self.z.sleep(0.01)
        with self.assertRaises(self.z.channel.PipeError):
            fut1.get()
        with self.assertRaises(self.z.channel.PipeError):
            fut2.get()

    @passive
    def test_reconnect(self):
        self.c.clean()
        self.c.insert({'hello': 'world'*1000000})
        self.c._conn._channel._sock.shutdown(socket.SHUT_RDWR)
        self.z.sleep(0.01)
        self.assertEqual([{'hello': 'world'}],
            self.query_data({'hello': 'world'}))

    @passive
    def test_selector(self):
        self.c.clean()
        self.c.insert_many([
            {'hello': 'world'},
            {'hello': 'anyone'},
            ])
        self.assertEqual([{'hello': 'world'}],
            self.query_data({'hello': 'world'}))
        self.assertEqual([
            {'hello': 'world'},
            {'hello': 'anyone'},
            ], self.query_data({}))

    @passive
    def test_update(self):
        self.c.clean()
        self.c.insert({'test1': 1})
        self.assertEqual([{'test1': 1}],
            self.query_data({'test1': 1}))
        self.c.update({'test1': 1}, {'test1': 2})
        self.assertEqual([],
            self.query_data({'test1': 1}))
        self.assertEqual([{'test1': 2}],
            self.query_data({'test1': 2}))

    @passive
    def test_save(self):
        self.c.clean()
        self.c.insert({'test1': 1})
        doc = list(self.c.query({'test1': 1}))[0]
        doc['test1'] = 3
        self.c.save(doc)
        self.assertEqual([{'test1': 3}],
            self.query_data({}))


if __name__ == '__main__':
    import unittest
    unittest.main()
