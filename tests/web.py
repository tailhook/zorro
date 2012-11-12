import unittest


class TestWeb(unittest.TestCase):

    def testLocalDispatch(self):
        from zorro import web

        class MyRes(web.Resource):

            @web.page
            def hello(self):
                return 'hello'

            @web.page
            def _hidden(self):
                return 'hidden'

            def invisible(self):
                return 'invisible'

        r = MyRes()

        self.assertEqual(r.resolve_local('hello'), r.hello)
        with self.assertRaises(web.NotFound):
            r.resolve_local('_hidden')
        with self.assertRaises(web.NotFound):
            r.resolve_local('invisible')
        with self.assertRaises(web.NotFound):
            r.resolve_local('hello world')

    def testResolve(self):
        from zorro import web

        class Request(web.Request):
            def __init__(self, uri):
                self.uri = uri

        class Root(web.Resource):

            @web.page
            def index(self):
                return 'index'

            @web.page
            def about(self):
                return 'about'

            @web.resource
            def forum(self, id:int = None):
                if id is not None:
                    return Forum(id)
                raise web.PathRewrite('/forums')

            @web.page
            def forums(self):
                return 'forums'

        class Forum(web.Resource):

            def __init__(self, id):
                self.id = id

            @web.page
            def index(self):
                return 'forum(%d).index' % self.id

            @web.page
            def new_topic(self):
                return 'forum(%d).new_topic' % self.id

            @web.page
            def topic(self, topic:int, *, offset:int = 0, num:int = 10):
                return 'forum({}).topic({})[{}:{}]'.format(
                    self.id, topic, offset, num)


        site = web.Site(request_class=Request, resources=[Root()])
        s = lambda v: site._resolve(Request(v))
        self.assertEqual(s(b'/'), 'index')
        self.assertEqual(s(b'/about'), 'about')
        self.assertEqual(s(b'/about/'), 'about')
        with self.assertRaises(web.NotFound):
            s(b'/about/test')
        self.assertEqual(site(b'/forum'), 'forums')
        self.assertEqual(site(b'/forum/'), 'forums')
        self.assertEqual(s(b'/forum/10'), 'forum(10).index')
        self.assertEqual(s(b'/forum/10/'), 'forum(10).index')
        self.assertEqual(s(b'/forum?id=10'), 'forum(10).index')
        self.assertEqual(s(b'/forum/?id=10'), 'forum(10).index')
        with self.assertRaises(web.NotFound):
            s(b'/forum/10?id=10')
        with self.assertRaises(web.NotFound):
            s(b'/forum/test')
        self.assertEqual(s(b'/forum/11/new_topic'),
            'forum(11).new_topic')
        self.assertEqual(s(b'/forum/11/new_topic/'),
            'forum(11).new_topic')
        self.assertEqual(s(b'/forum/12/topic/10'),
            'forum(12).topic(10)[0:10]')
        self.assertEqual(s(b'/forum/12/topic/10?offset=10'),
            'forum(12).topic(10)[10:10]')
        with self.assertRaises(web.NotFound):
            s(b'/forum/12/topic/10/10')
        self.assertEqual(s(b'/forum/12/topic/10?offset=20&num=20'),
            'forum(12).topic(10)[20:20]')
        self.assertEqual(
            s(b'/forum/12/topic?topic=13&offset=20&num=20'),
            'forum(12).topic(13)[20:20]')

    def testDecorators(self):
        from time import time
        from zorro import web
        from functools import wraps
        from collections import namedtuple
        User = namedtuple("User", 'id')

        def add_prefix(fun):
            @web.postprocessor(fun)
            def processor(self, resolver, value):
                return 'prefix:[' + value + ']'
            return processor

        def add_suffix(suffix):
            def decorator(fun):
                @web.postprocessor(fun)
                def processor(self, resolver, value):
                    return '[' + value + ']:' + suffix
                return processor
            return decorator

        def add_user(fun):
            @web.provider(fun, User)
            def provider(resolver):
                uid = int(resolver.keyword_args.pop('uid'))
                return User(uid)
            return provider

        def special_args(fun):
            @web.argument_parser(fun)
            def make_args(fun, resolver):
                return (1, 2), {'key': 7}
            return fun

        def hardcode_args(*args, **kw):
            def decorator(fun):
                @web.argument_parser(fun)
                def return_args(fun, resolver):
                    return args, kw
                return fun
            return decorator

        last_latency = None
        def timeit(fun):
            @wraps(fun)
            def wrapper(self, *args, **kwargs):
                nonlocal last_latency
                start = time()
                result = fun(self, *args, **kwargs)
                last_latency = time() - start
                return result
            return wrapper


        class Request(web.Request):
            def __init__(self, uri):
                self.uri = uri

        class Root(web.Resource):

            @web.page
            @add_prefix
            def about(self):
                return 'about'

            @web.page
            @add_user
            def profile(self, user: User):
                return 'profile(%d)' % (user.id)

            @add_user
            @web.page
            def friend(self, user: User, friend: int):
                return 'profile(%d).friend(%d)' % (user.id, friend)

            @add_prefix
            @web.page
            def info(self, uid: int):
                return 'info(%d)' % uid

            @web.page
            @add_user
            @timeit
            @add_prefix
            @add_suffix('suf')
            def banner(self, ad: int, user: User, *, position: str = "norm"):
                return 'banner(ad:{:d}, uid:{:d}, position:{})'.format(
                    ad, user.id, position)

            @web.resource
            @add_user
            def forum(self, user:User):
                return Forum(user)

            @special_args
            @web.page
            def special(self, a, b, key):
                return 'special({}, {}, {})'.format(a, b, key)

            @web.page
            @hardcode_args(7, 8, 9, value=4)
            def hardcode(self, a, b, c, value):
                return 'hardcode({}, {}, {}, {})'.format(a, b, c, value)

        class Forum(web.Resource):

            def __init__(self, user):
                self.user = user

            @web.page
            def index(self):
                return "forum(user:{})".format(self.user.id)


        site = web.Site(request_class=Request, resources=[Root()])
        s = lambda v: site._resolve(Request(v))
        self.assertEqual(s(b'/about'), 'prefix:[about]')
        self.assertEqual(s(b'/forum?uid=7'), 'forum(user:7)')
        self.assertEqual(s(b'/profile?uid=3'), 'profile(3)')
        self.assertEqual(s(b'/friend/2?uid=3'), 'profile(3).friend(2)')
        self.assertEqual(s(b'/info/3'), 'prefix:[info(3)]')
        self.assertEqual(s(b'/banner/3?uid=4'),
            'prefix:[[banner(ad:3, uid:4, position:norm)]:suf]')
        self.assertTrue(last_latency < 0.01)  # will also fail if it's None
        self.assertEqual(s(b'/banner/?ad=2&uid=5'),
            'prefix:[[banner(ad:2, uid:5, position:norm)]:suf]')
        self.assertEqual(s(b'/banner/3?uid=12&position=abc'),
            'prefix:[[banner(ad:3, uid:12, position:abc)]:suf]')
        self.assertEqual(s(b'/special'), 'special(1, 2, 7)')
        self.assertEqual(s(b'/special?key=123'), 'special(1, 2, 7)')
        self.assertEqual(s(b'/hardcode'), 'hardcode(7, 8, 9, 4)')
        self.assertEqual(s(b'/hardcode?value=123'), 'hardcode(7, 8, 9, 4)')


if __name__ == '__main__':
    unittest.main()

