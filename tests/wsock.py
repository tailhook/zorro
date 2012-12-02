import unittest
import json
from time import time
from functools import wraps
from collections import namedtuple

from zorro import wsock, web


class TestResolve(unittest.TestCase):

    def setUp(self):

        @web.Sticker.register
        class User(object):
            def __init__(self, uid):
                self.id = uid
            @classmethod
            def create(cls, resolver):
                return cls(int(resolver.request.marker))

        class Root(wsock.Resource):

            @wsock.method
            def get_articles(self):
                return ['a', 'b']

            @wsock.method
            def get_article(self, name: str):
                return name + 'text'

            @wsock.resource
            def chat(self, room: str):
                return Chat(room)


        class Chat(wsock.Resource):

            def __init__(self, room):
                self.room = room

            @wsock.method
            def publish(self, u: User, message: str):
                return '[{}] {}: {}'.format(self.room, u.id, message)

            @wsock.method
            def get_messages(self):
                return [self.room + '1', self.room + '2']


        self.wsock = wsock.Websockets(resources=[Root()])

    def resolve(self, *args):
        data = json.dumps((args[0], {},) + args[1:]).encode('utf-8')
        return self.wsock._resolve(wsock.WebsocketCall(
            [b'testcid', b'message', data]))

    def resolve_cookie(self, *args):
        data = json.dumps((args[0], {},) + args[1:]).encode('utf-8')
        return self.wsock._resolve(wsock.WebsocketCall(
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


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

