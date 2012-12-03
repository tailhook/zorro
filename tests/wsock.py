import unittest
import json
from time import time
from functools import wraps
from collections import namedtuple

from zorro import web


class TestBase(unittest.TestCase):

    def setUp(self):

        @web.Sticker.register
        class User(object):

            def __init__(self, uid):
                self.id = uid

            @classmethod
            def create(cls, resolver):
                if isinstance(resolver, web.PathResolver):
                    # It's HTTP
                    return cls(int(resolver.request.cookies['uid']))
                elif isinstance(resolver, web.WebsockResolver):
                    # It's Websocket
                    return cls(int(resolver.request.marker))
                else:
                    raise RuntimeError("This resolver is not supported")

        class Root(web.Resource):

            @web.endpoint
            def get_articles(self):
                return ['a', 'b']

            @web.endpoint
            def get_article(self, name: str):
                return name + 'text'

            @web.resource
            def chat(self, room: str):
                return Chat(room)


        class Chat(web.Resource):

            def __init__(self, room):
                self.room = room

            @web.endpoint
            def publish(self, u: User, message: str):
                return '[{}] {}: {}'.format(self.room, u.id, message)

            @web.endpoint
            def get_messages(self):
                return [self.room + '1', self.room + '2']

        self.Root = Root


class TestWsock(TestBase):

    def setUp(self):
        super().setUp()
        self.web = web.Websockets(resources=[self.Root()])

    def resolve(self, *args):
        data = json.dumps((args[0], {},) + args[1:]).encode('utf-8')
        return self.web._resolve(web.WebsockCall(
            [b'testcid', b'message', data]))

    def resolve_cookie(self, *args):
        data = json.dumps((args[0], {},) + args[1:]).encode('utf-8')
        return self.web._resolve(web.WebsockCall(
            [b'testcid', b'msgfrom', b'7', data]))

    def testSimple(self):
        self.assertEqual(self.resolve('get_articles'), ['a', 'b'])

    def testArg(self):
        self.assertEqual(self.resolve('get_article', 'a'), 'atext')

    def testChat(self):
        self.assertEqual(self.resolve('chat.get_messages', 'A'), ['A1', 'A2'])

    def testUser(self):
        self.assertEqual(self.resolve_cookie('chat.publish', 'A', 'tx'),
            '[A] 7: tx')


class TestHTTP(TestBase):

    def setUp(self):
        super().setUp()

        class Request(web.Request):

            def __init__(self, uri, cookie=None):
                self.uri = uri
                self.cookie = cookie

        self.Request = Request

        self.site = web.Site(
            request_class=Request,
            resources=[self.Root()])

    def resolve(self, uri):
        return self.site._resolve(self.Request(uri))

    def resolve_cookie(self, uri):
        return self.site._resolve(self.Request(uri, b'uid=7'))

    def testSimple(self):
        self.assertEqual(self.resolve(b'/get_articles'), ['a', 'b'])

    def testArg(self):
        self.assertEqual(self.resolve(b'/get_article/a'), 'atext')

    def testChat(self):
        self.assertEqual(self.resolve(b'/chat/A/get_messages'), ['A1', 'A2'])

    def testUser(self):
        self.assertEqual(self.resolve_cookie(b'/chat/A/publish?message=tx'),
            '[A] 7: tx')


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

