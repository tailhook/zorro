from .base import Test, interactive
import time
import http.server


class RequestHandler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Length', '5')
        self.end_headers()
        self.wfile.write(b'HELLO')
        self.wfile.flush()


class Simple(Test):

    def do_request(self):
        self.z.sleep(0.1)
        cli = self.z.http.HTTPClient('localhost', 9999)
        self.assertEqual(cli.request('/').body, b'HELLO')

    @interactive(do_request)
    def test_req(self):
        import zorro.http
        srv = http.server.HTTPServer(('localhost', 9999), RequestHandler)
        srv.handle_request()


if __name__ == '__main__':
    import unittest
    unittest.main()
