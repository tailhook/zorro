# -*- coding: utf-8 -*-
import socket
import unittest
import datetime
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
    def test_nulls(self):
        self.m.execute('drop table if exists test')
        self.m.execute('create table test (id int)')
        self.assertEqual(self.m.execute_prepared('insert into test values (?)'
            + ',(?)'*24, *((1,)+(None,)*24)), (0, 25))
        self.assertEqual(list(self.m.query('select * from test').tuples()),
            [(1,)] + [(None,)]*24)

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
        self.assertEqual(self.m.execute_prepared(
            'update test set val = ? where id < ?', None, 30), (0, 2))
        self.assertEqual(set(self.m.query_prepared('select * from test')),
            set([(12, None), (22, None), (32, "3333")]))

    @passive
    def test_error(self):
        with self.assertRaises(self.z.mysql.MysqlError):
            self.m.execute('hello world')


class TestTypes(Mysql):

    @passive
    def test_tinyint(self):
        self.exact_test('TINYINT', [1, 2, 5, 15, -20, None])
        self.approx_test('TINYINT', [
            (250, 127),
            (1000, 127),
            ])
        self.exact_test('TINYINT(1)', [1, 2, 5, 15, -20, None])
        self.approx_test('TINYINT(1)', [
            (250, 127),
            (1000, 127),
            ])
        self.exact_test('TINYINT UNSIGNED', [1, 2, 5, 15, None])
        self.approx_test('TINYINT UNSIGNED', [
            (250, 250),
            (1000, 255),
            (-20, 0),
            ])

    @passive
    def test_integer(self):
        self.exact_test('INTEGER', [10, 100, 1000, 10000, 1234567,
            16 << 24, -100, -1000, -10000, -100000000, None])
        self.exact_test('INTEGER UNSIGNED', [10, 100, 1000, 10000, 1234567,
            16 << 24, (1 << 31) + 100, None])
        self.approx_test('INTEGER', [
            (1 << 40, (1 << 31)-1),
            (-1 << 40, -(1 << 31)),
            ])
        self.approx_test('INTEGER UNSIGNED', [
            (1 << 40, (1 << 32)-1),
            (-1, 0),
            ])

    @passive
    def test_bigint(self):
        self.exact_test('BIGINT', [10, 100, 1 << 40,
            -10, -100, -1 << 40, None])
        self.exact_test('BIGINT UNSIGNED', [10, 100, 1 << 40,
            (1 << 63) + 100, None])
        self.approx_test('BIGINT', [
            (1 << 100, (1 << 63)-1),
            (-1 << 100, -(1 << 63)),
            ])
        self.approx_test('BIGINT UNSIGNED', [
            (1 << 100, (1 << 64)-1),
            (-1, 0),
            ])

    @passive
    def test_varchar(self):
        self.exact_test('VARCHAR(100)', [
            "hello",
            "test test test test test",
            None,
            ])

    @passive
    def test_text(self):
        self.exact_test('VARCHAR(1024)', [
            "hello",
            "t"*520,
            ])

    @passive
    def test_char(self):
        self.exact_test('CHAR(10)', [
            "hello",
            "test test",
            "hello test",
            None,
            ])
        self.approx_test('CHAR(10)', [
            ("hello world hello world", "hello worl"),
            ])

    @passive
    def test_time(self):
        self.exact_test('TIME', [
            datetime.datetime.now().replace(microsecond=0).time(),
            datetime.time(12, 3, 7),
            ])

    @passive
    def test_time(self):
        self.exact_test('DATE', [
            datetime.date.today(),
            datetime.date(2011, 3, 6),
            ])

    @passive
    def test_datetime(self):
        self.exact_test('DATETIME', [
            datetime.datetime.now().replace(microsecond=0),
            datetime.datetime(2011, 3, 6),
            datetime.datetime(2011, 3, 6, 12, 34),
            datetime.datetime(2011, 3, 6, 12, 34, 47),
            ])

    def exact_test(self, typ, values):
        self.m.execute('drop table if exists test')
        try:
            self.m.execute('create table test (value {0})'.format(typ))
            for val in values:
                self.m.execute('insert into test (value) VALUES ({0})', val)

            self.assertEqual(values, [val for val,
                in self.m.query('select value from test').tuples()])
            self.assertEqual(values, [val for val,
                in self.m.query_prepared('select value from test').tuples()])
            self.assertEqual([{'value': a} for a in values],
                list(self.m.query('select value from test').dicts()))
            self.assertEqual([{'value': a} for a in values],
                list(self.m.query_prepared('select value from test').dicts()))
            self.assertEqual(values, [val for val,
                in self.m.query_prepared('select value from test')])

            self.m.execute('delete from test')
            for val in values:
                self.m.execute_prepared('insert into test (value) VALUES (?)',
                    val)

            self.assertEqual(values, [val for val,
                in self.m.query('select value from test').tuples()])
            self.assertEqual(values, [val for val,
                in self.m.query_prepared('select value from test').tuples()])
            self.assertEqual([{'value': a} for a in values],
                list(self.m.query('select value from test').dicts()))
            self.assertEqual([{'value': a} for a in values],
                list(self.m.query_prepared('select value from test').dicts()))
            self.assertEqual(values, [val for val,
                in self.m.query_prepared('select value from test')])
        finally:
            self.m.execute('drop table if exists test')

    def approx_test(self, typ, pairs):
        self.m.execute('drop table if exists test')
        try:
            self.m.execute('create table test (value {0})'.format(typ))
            for a, b in pairs:
                self.m.execute('insert into test (value) VALUES ({0})', a)

            self.assertEqual([b for a, b in pairs], [val for val,
                in self.m.query('select value from test').tuples()])
            self.assertEqual([b for a, b in pairs], [val for val,
                in self.m.query_prepared('select value from test').tuples()])
            self.assertEqual([{'value': b} for a, b in pairs],
                list(self.m.query('select value from test').dicts()))
            self.assertEqual([{'value': b} for a, b in pairs],
                list(self.m.query_prepared('select value from test').dicts()))
            self.assertEqual([b for a, b in pairs], [val for val,
                in self.m.query_prepared('select value from test')])

            self.m.execute('delete from test')
            for a, b in pairs:
                self.m.execute_prepared('insert into test (value) VALUES (?)',
                    a)

            self.assertEqual([b for a, b in pairs], [val for val,
                in self.m.query('select value from test').tuples()])
            self.assertEqual([b for a, b in pairs], [val for val,
                in self.m.query_prepared('select value from test').tuples()])
            self.assertEqual([{'value': b} for a, b in pairs],
                list(self.m.query('select value from test').dicts()))
            self.assertEqual([{'value': b} for a, b in pairs],
                list(self.m.query_prepared('select value from test').dicts()))
            self.assertEqual([b for a, b in pairs], [val for val,
                in self.m.query_prepared('select value from test')])
        finally:
            self.m.execute('drop table if exists test')


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
