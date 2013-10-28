import time
import http.server

from .base import Test, interactive


class RequestHandler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Length', '5')
        self.end_headers()
        self.wfile.write(b'HELLO')
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
        cli = self.z.http.HTTPClient('localhost', 9997)
        self.got_value = cli.request('/').body

    @interactive(do_request)
    def test_req(self):
        import zorro.http
        srv = http.server.HTTPServer(('localhost', 9997), RequestHandler)
        srv.handle_request()
        self.thread.join(1)
        self.assertEqual(self.got_value, b'HELLO')

    def do_fetch(self):
        self.z.sleep(0.1)
        cli = self.z.http.HTTPClient('localhost', 9997)
        self.fetched_value = cli.request('/', method='FETCH').body

    @interactive(do_fetch)
    def test_fetch(self):
        import zorro.http
        srv = http.server.HTTPServer(('localhost', 9997), RequestHandler)
        srv.handle_request()
        self.thread.join(1)
        self.assertEqual(self.fetched_value, b'HELLO')


if __name__ == '__main__':
    import unittest
    unittest.main()
