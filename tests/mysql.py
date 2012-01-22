# -*- coding: utf-8 -*-
import socket
from functools import partial

from .base import Test, passive


class Mysql(Test):

    def setUp(self):
        super().setUp()
        import zorro.mysql
        self.m = zorro.mysql.Mysql()


class Simple(Mysql):

    @passive
    def test_basic(self):
        self.m.execute('drop table if exists test')
        self.m.execute('create table test (id int)')
        self.assertEqual(self.m.execute(
            'insert into test values (10)'), (0, 1))
        self.assertEqual(self.m.execute(
            'insert into test values (30),(40)'), (0, 2))

    @passive
    def test_select(self):
        self.m.execute('drop table if exists test')
        self.m.execute('create table test (id int, val varchar(10))')
        self.assertEqual(self.m.execute(
            'insert into test values (10, "11")'), (0, 1))
        self.assertEqual(set(self.m.query('select * from test').tuples()),
            set([(10, "11")]))
        self.assertEqual(list(self.m.query('select * from test').dicts())[0],
            {'id': 10, 'val': "11"})

    @passive
    def test_prepared(self):
        self.m.execute('drop table if exists test')
        self.m.execute('create table test (id int, val varchar(10))')
        self.assertEqual(self.m.execute(
            'insert into test values (10, "11"), (20, "222"), (30, "3333")'),
            (0, 3))
        self.assertEqual(set(self.m.query('select * from test').tuples()),
            set([(10, "11"), (20, "222"), (30, "3333")]))
        self.assertEqual(self.m.execute_prepared(
            'update test set id = id + ?', 2), (0, 3))
        self.assertEqual(set(self.m.query_prepared(
            'select * from test where id < ?', 30)),
            set([(12, "11"), (22, "222")]))



if __name__ == '__main__':
    import unittest
    unittest.main()
