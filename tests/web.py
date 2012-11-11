import unittest


class TestWeb(unittest.TestCase):

    def testLocalDispatch(self):
        from zorro.web import Resource, public, NotFound

        class MyRes(Resource):

            @public
            def hello(self):
                return 'hello'

            @public
            def _hidden(self):
                return 'hidden'

            def invisible(self):
                return 'invisible'

        r = MyRes()

        self.assertEqual(r.resolve_local('hello'), r.hello)
        with self.assertRaises(NotFound):
            r.resolve_local('_hidden')
        with self.assertRaises(NotFound):
            r.resolve_local('invisible')
        with self.assertRaises(NotFound):
            r.resolve_local('hello world')


if __name__ == '__main__':
    unittest.main()

