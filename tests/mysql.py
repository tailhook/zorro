# -*- coding: utf-8 -*-
import socket
import unittest
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

    @passive
    def test_error(self):
        with self.assertRaises(self.z.mysql.MysqlError):
            self.m.execute('hello world')


class TestFormat(unittest.TestCase):


    def test_simple(self):
        from zorro.mysql import Formatter
        fmt = Formatter().format
        self.assertEqual(fmt('{0}+{1}, {2}, {2:5.2f}, {3}, {4}',
            10, -15, 0.5, "john's test", None),
            r"10+-15, 0.5, ' 0.50', 'john\'s test', NULL"),

    def test_fields(self):
        from zorro.mysql import Formatter
        fmt = Formatter().format
        self.assertEqual(fmt('SELECT {0}, {0!c} FROM {1!t}', 'test', 'select'),
            "SELECT 'test', test FROM `select`")

    def test_datetime(self):
        from datetime import date, time, datetime, timedelta
        from zorro.mysql import Formatter
        fmt = Formatter().format
        self.assertEqual(fmt('{0}, {0:%Y-%m}', date(2008, 1, 7)),
            "'2008-01-07', '2008-01'")
        self.assertEqual(fmt('{0}, {0:%Y-%m}', datetime(2008, 1, 7)),
            "'2008-01-07 00:00:00', '2008-01'")
        self.assertEqual(fmt('{0}, {0:%M-%H}', time(10, 46, 12)),
            "'10:46:12', '46-10'")
        self.assertEqual(fmt('DATEADD({0!c}, {1})',
            'date', timedelta(1, 3678, 33)),
            "DATEADD(date, INTERVAL '1 01:01:18.000033' DAY_MICROSECOND)")



if __name__ == '__main__':
    unittest.main()
