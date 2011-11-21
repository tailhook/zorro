import unittest
from zorro.mongodb import bson

class TestBson(unittest.TestCase):

    def test_loads(self):
        data = b"\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00"
        self.assertEqual({"hello": "world"}, bson.loads(data))
        data = (b"1\x00\x00\x00\x04BSON\x00&\x00\x00\x00\x020\x00\x08\x00\x00"
                 b"\x00awesome\x00\x011\x00333333\x14@\x102\x00\xc2\x07\x00"
                 b"\x00\x00\x00")
        self.assertEqual({"BSON": ["awesome", 5.05, 1986]}, bson.loads(data))

    def test_dumps(self):
        data = {"hello": "world"}
        coded = b"\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00"
        self.assertEqual(coded, bson.dumps(data))
        coded = (b"1\x00\x00\x00\x04BSON\x00&\x00\x00\x00\x020\x00\x08\x00\x00"
                 b"\x00awesome\x00\x011\x00333333\x14@\x102\x00\xc2\x07\x00"
                 b"\x00\x00\x00")
        data = {"BSON": ["awesome", 5.05, 1986]}
        self.assertEqual(coded, bson.dumps(data))


if __name__ == '__main__':
    unittest.main()
