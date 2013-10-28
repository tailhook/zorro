import os.path
import time
import ssl
import socket
from socketserver import BaseServer
from http.server import HTTPServer, BaseHTTPRequestHandler

from .base import Test, interactive


BIGDATA = b'hello'*10000


class HTTPSServer(HTTPServer):

    def __init__(self,address,handler):
        BaseServer.__init__(self,address,handler)

        self.socket = ssl.SSLSocket(
            sock=socket.socket(self.address_family,self.socket_type),
            ssl_version=ssl.PROTOCOL_TLSv1,
            certfile=os.path.join(os.path.dirname(__file__), 'test.pem'),
            server_side=True)
        self.server_bind()
        self.server_activate()


class RequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Length', '5')
        self.end_headers()
        self.wfile.write(b'HELLO')
        self.wfile.flush()

    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-Length', str(len(BIGDATA)))
        self.end_headers()
        self.wfile.write(BIGDATA)
        self.wfile.flush()

    def do_FETCH(self):
        self.send_response(200)
        self.send_header('Transfer-Encoding', 'chunked')
        self.end_headers()
        self.wfile.write(b'5\r\nHELLO\r\n0\r\n\r\n')
        self.wfile.flush()


class Simple(Test):

    def do_request(self):
        self.z.sleep(0.1)
        cli = self.z.http.HTTPSClient('localhost', 9997)
        self.got_value = cli.request('/').body

    @interactive(do_request)
    def test_req(self):
        import zorro.http
        srv = HTTPSServer(('localhost', 9997), RequestHandler)
        srv.handle_request()
        self.thread.join(1)
        self.assertEqual(self.got_value, b'HELLO')

    def do_fetch(self):
        self.z.sleep(0.1)
        cli = self.z.http.HTTPSClient('localhost', 9997)
        self.fetched_value = cli.request('/', method='FETCH').body

    @interactive(do_fetch)
    def test_fetch(self):
        import zorro.http
        srv = HTTPSServer(('localhost', 9997), RequestHandler)
        srv.handle_request()
        self.thread.join(1)
        self.assertEqual(self.fetched_value, b'HELLO')

    def do_post(self):
        self.z.sleep(0.1)
        cli = self.z.http.HTTPSClient('localhost', 9997)
        self.post_value = cli.request('/', method='POST').body

    @interactive(do_post)
    def test_post(self):
        import zorro.http
        srv = HTTPSServer(('localhost', 9997), RequestHandler)
        srv.handle_request()
        self.thread.join(1)
        self.assertEqual(self.post_value, BIGDATA)


if __name__ == '__main__':
    import unittest
    unittest.main()
