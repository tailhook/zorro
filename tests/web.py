import unittest
from time import time
from functools import wraps
from collections import namedtuple

from zorro import web


class TestLocalDispatch(unittest.TestCase):

    def setUp(self):

        class MyRes(web.Resource):

            @web.page
            def hello(self):
                return 'hello'

            @web.page
            def _hidden(self):
                return 'hidden'

            def invisible(self):
                return 'invisible'

        self.r = MyRes()


    def testOK(self):
        self.assertEqual(self.r.resolve_local('hello'), self.r.hello)

    def testHidden(self):
        with self.assertRaises(web.ChildNotFound):
            self.r.resolve_local('_hidden')

    def testInvisible(self):
        with self.assertRaises(web.ChildNotFound):
            self.r.resolve_local('invisible')

    def testStrange(self):
        with self.assertRaises(web.ChildNotFound):
            self.r.resolve_local('hello world')


class TestResolve(unittest.TestCase):

    def setUp(self):

        class Request(web.Request):
            def __init__(self, uri):
                self.uri = uri

        self.Request = Request

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
        self.site = web.Site(request_class=Request, resources=[Root()])

    def resolve(self, uri):
        return self.site._resolve(self.Request(uri))

    def testIndex(self):
        self.assertEqual(self.resolve(b'/'), 'index')

    def testPage(self):
        self.assertEqual(self.resolve(b'/about'), 'about')

    def testSlash(self):
        self.assertEqual(self.resolve(b'/about/'), 'about')

    def testSuffix(self):
        with self.assertRaises(web.NotFound):
            self.resolve(b'/about/test')

    def testRedirect(self):
        self.assertEqual(self.site(b'/forum'), 'forums')

    def testSlashRedirect(self):
        self.assertEqual(self.site(b'/forum/'), 'forums')

    def testArg(self):
        self.assertEqual(self.resolve(b'/forum/10'), 'forum(10).index')

    def testArgSlash(self):
        self.assertEqual(self.resolve(b'/forum/10/'), 'forum(10).index')

    def testArgQuery(self):
        self.assertEqual(self.resolve(b'/forum?id=10'), 'forum(10).index')

    def testArgQuerySlash(self):
        self.assertEqual(self.resolve(b'/forum/?id=10'), 'forum(10).index')

    def testQueryAndPos(self):
        with self.assertRaises(web.NotFound):
            self.resolve(b'/forum/10?id=10')

    def testValueError(self):
        with self.assertRaises(web.NotFound):
            self.resolve(b'/forum/test')

    def testNested(self):
        self.assertEqual(self.resolve(b'/forum/11/new_topic'),
            'forum(11).new_topic')

    def testNestedSlash(self):
        self.assertEqual(self.resolve(b'/forum/11/new_topic/'),
            'forum(11).new_topic')

    def testNestedArg(self):
        self.assertEqual(self.resolve(b'/forum/12/topic/10'),
            'forum(12).topic(10)[0:10]')

    def testNestedQuery(self):
        self.assertEqual(self.resolve(b'/forum/12/topic/10?offset=10'),
            'forum(12).topic(10)[10:10]')

    def testNestedExcessive(self):
        with self.assertRaises(web.NotFound):
            self.resolve(b'/forum/12/topic/10/10')

    def testNestedQuery2(self):
        self.assertEqual(self.resolve(b'/forum/12/topic/10?offset=20&num=20'),
            'forum(12).topic(10)[20:20]')

    def testNestedAllQuery(self):
        self.assertEqual(
            self.resolve(b'/forum/12/topic?topic=13&offset=20&num=20'),
            'forum(12).topic(13)[20:20]')


class TestDecorators(unittest.TestCase):

    def setUp(self):

        @web.Sticker.register
        class User(object):
            def __init__(self, uid):
                self.id = uid
            @classmethod
            def create(cls, resolver):
                return cls(int(resolver.request.form_arguments.get('uid')))

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

        def form(fun):
            @web.decorator(fun)
            def wrapper(self, resolver, meth):
                if resolver.request.form_arguments:
                    return meth(self, resolver, 1, b=2)
                else:
                    return 'form'
            return wrapper

        def hidden(fun):
            @web.decorator(fun)
            def wrapper(self, resolver, meth, a, b):
                return meth(self, resolver, a, b=b, c=69)
            return wrapper

        self.last_latency = None
        def timeit(fun):
            @wraps(fun)
            def wrapper(me, *args, **kwargs):
                start = time()
                result = fun(me, *args, **kwargs)
                self.last_latency = time() - start
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
            def profile(self, user: User):
                return 'profile(%d)' % (user.id)

            @web.page
            def friend(self, user: User, friend: int):
                return 'profile(%d).friend(%d)' % (user.id, friend)

            @add_prefix
            @web.page
            def info(self, uid: int):
                return 'info(%d)' % uid

            @web.page
            @timeit
            @add_prefix
            @add_suffix('suf')
            def banner(self, ad: int, user: User, *, position: str = "norm"):
                return 'banner(ad:{:d}, uid:{:d}, position:{})'.format(
                    ad, user.id, position)

            @web.resource
            def forum(self, user:User):
                return Forum(user)

            @form
            @web.page
            def form1(self, a, b):
                return 'form1({}, {})'.format(a, b)

            @add_prefix
            @web.page
            @form
            @hidden
            def form2(self, u:User, a, b, c):
                return 'form2({}, {}, {}, {})'.format(a, b, c, u.id)

        class Forum(web.Resource):

            def __init__(self, user):
                self.user = user

            @web.page
            def index(self):
                return "forum(user:{})".format(self.user.id)

        self.site = web.Site(request_class=Request, resources=[Root()])
        self.Request = Request

    def resolve(self, uri):
        return self.site._resolve(self.Request(uri))

    def testPost(self):
        self.assertEqual(self.resolve(b'/about'), 'prefix:[about]')

    def testResourceSticker(self):
        self.assertEqual(self.resolve(b'/forum?uid=7'), 'forum(user:7)')

    def testSticker(self):
        self.assertEqual(self.resolve(b'/profile?uid=3'), 'profile(3)')

    def testStickerArg(self):
        self.assertEqual(self.resolve(b'/friend/2?uid=3'),
            'profile(3).friend(2)')

    def testPostArg(self):
        self.assertEqual(self.resolve(b'/info/3'), 'prefix:[info(3)]')

    def test2PostAndWrapDefPos(self):
        self.assertEqual(self.resolve(b'/banner/3?uid=4'),
            'prefix:[[banner(ad:3, uid:4, position:norm)]:suf]')
        self.assertTrue(self.last_latency < 0.01)  # also fail if it's None

    def test2PostAndWrapDefQuery(self):
        self.assertEqual(self.resolve(b'/banner/?ad=2&uid=5'),
            'prefix:[[banner(ad:2, uid:5, position:norm)]:suf]')
        self.assertTrue(self.last_latency < 0.01)  # also fail if it's None

    def test2PostAndWrapFull(self):
        self.assertEqual(self.resolve(b'/banner/3?uid=12&position=abc'),
            'prefix:[[banner(ad:3, uid:12, position:abc)]:suf]')

    def testDecoSkip(self):
        self.assertEqual(self.resolve(b'/form1'), 'form')

    def testDecoInvent(self):
        self.assertEqual(self.resolve(b'/form1?a=7'), 'form1(1, 2)')

    def test2DecoSkip(self):
        self.assertEqual(self.resolve(b'/form2'), 'prefix:[form]')

    def test2DecoInvent(self):
        self.assertEqual(self.resolve(b'/form2?uid=13'),
            'prefix:[form2(1, 2, 69, 13)]')


class TestMethod(unittest.TestCase):

    def setUp(self):

        class Request(web.Request):
            def __init__(self, meth, uri):
                self.method = meth
                self.uri = uri

        class Root(web.Resource):

            def __init__(self):
                super().__init__()
                self.about = About()

            @web.resource
            def forum(self, uid:int):
                return Forum(uid)


        class About(web.Resource):
            """Uses default resolver"""

            @web.page
            def index(self):
                return 'blank'

            @web.page
            def more(self, page:str):
                return 'PAGE:%s' % page

        class Forum(web.Resource):
            resolver_class = web.MethodResolver

            def __init__(self, user):
                self.user = user

            @web.page
            def GET(self):
                return "forum(user:{})".format(self.user)

            @web.page
            def PATCH(self, topic:int):
                return "set:forum(user:{},topic:{})".format(self.user, topic)

        self.site = web.Site(request_class=Request, resources=[Root()])
        self.Request = Request

    def resolve(self, meth, uri):
        return self.site._resolve(self.Request(meth, uri))

    def testAbout(self):
        self.assertEqual(self.resolve(b'GET', b'/about'), 'blank')

    def testLessArgs(self):
        with self.assertRaises(web.NotFound):
            self.resolve(b'GET', b'/about/more')
        with self.assertRaises(web.NotFound):
            self.resolve(b'GET', b'/about/more/')

    def testLonger(self):
        self.assertEqual(self.resolve(b'GET', b'/about/more/abc'), 'PAGE:abc')

    def testGet(self):
        self.assertEqual(self.resolve(b'GET', b'/forum?uid=37'),
            'forum(user:37)')

    def testLower(self):
        self.assertEqual(self.resolve(b'get', b'/forum?uid=37'),
            'forum(user:37)')

    def testNonExistent(self):
        with self.assertRaises(web.MethodNotAllowed):
            self.resolve(b'FIX', b'/forum?uid=37')

    @unittest.expectedFailure
    def testPatch(self):
        self.assertEqual(self.resolve(b'PATCH', b'/forum/12?uid=37'),
            'set:forum(user:37,topic:12)')

    def testPatchQuery(self):
        self.assertEqual(self.resolve(b'PATCH', b'/forum?uid=7&topic=6'),
            'set:forum(user:7,topic:6)')

    def testPatchQuerySlash(self):
        self.assertEqual(self.resolve(b'PATCH', b'/forum/?uid=9&topic=8'),
            'set:forum(user:9,topic:8)')

    @unittest.expectedFailure
    def testPatchSlash(self):
        self.assertEqual(self.resolve(b'PATCH', b'/forum/77/?uid=9'),
            'set:forum(user:9,topic:77)')

    def testNotEnough(self):
        with self.assertRaises(web.NotFound):
            self.resolve(b'PATCH', b'/forum?uid=9')


if __name__ == '__main__':
    unittest.main()

