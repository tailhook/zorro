import socket
import errno
from urllib.parse import urlencode

from .core import gethub, Lock
from . import channel
from .util import setcloexec

try:
    import ssl
except ImportError:
    pass


class Response(object):

    def __init__(self, status, headers, body):
        self.status = status
        self.headers = headers
        self.body = body


class RequestChannel(channel.PipelinedReqChannel):
    BUFSIZE = 4096

    def __init__(self, host, port, unixsock):
        super().__init__()
        if unixsock is None:
            self._sock = socket.socket(socket.AF_INET,
                socket.SOCK_STREAM, socket.IPPROTO_TCP)
            addr = (host, port)
        else:
            self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            addr = unixsock
        setcloexec(self._sock)
        self._sock.setblocking(0)
        self._connect(addr)
        self._start()

    def _connect(self, addr):
        try:
            self._sock.connect(addr)
        except socket.error as e:
            if e.errno == errno.EINPROGRESS:
                gethub().do_write(self._sock)
            else:
                raise

    def _close_channel(self):
        self._sock.close()
        super()._close_channel()

    def sender(self):
        buf = bytearray()

        add_chunk = buf.extend
        wait_write = gethub().do_write

        while True:
            if not buf:
                self.wait_requests()
            if not self._alive:
                return
            wait_write(self._sock)
            for chunk in self.get_pending_requests():
                add_chunk(chunk)
            try:
                bytes = self._sock.send(buf)
            except socket.error as e:
                if e.errno in (errno.EAGAIN, errno.EINTR):
                    continue
                else:
                    raise
            if not bytes:
                raise EOFError("Connection closed by peer")
            del buf[:bytes]

    def _readmore(self, buf, pos):
        while True:
            try:
                if pos[0]*2 > len(buf):
                    del buf[:pos[0]]
                    pos[0] = 0
                bytes = self._sock.recv(self.BUFSIZE)
                if not bytes:
                    raise EOFError("Connection closed by peer")
                buf.extend(bytes)
            except socket.error as e:
                if e.errno in (errno.EAGAIN, errno.EINTR):
                    gethub().do_read(self._sock)
                    continue
                else:
                    raise
            else:
                break

    def receiver(self):
        buf = bytearray()

        sock = self._sock
        pos = [0]

        def readchunked():
            result = bytearray()
            while True:
                while True:
                    idx = buf.find(b'\r\n', pos[0])
                    if idx >= 0:
                        break
                    self._readmore(buf, pos)
                line = buf[pos[0]:idx]
                pos[0] = idx
                try:
                    num = int(line, 16)
                except ValueError:
                    raise EOFError("Wrong number of bytes, or some extension")
                if num == 0:
                    break
                pos[0] += 2  # eat endline
                while len(buf) < pos[0] + num:
                    self._readmore(buf, pos)
                result.extend(buf[pos[0]:pos[0]+num])
                pos[0] += num
                if buf[pos[0]:pos[0]+2] != b'\r\n':
                    raise EOFError("Chunk is not terminated properly")
                pos[0] += 2
            while True:
                idx = buf.find(b'\r\n\r\n', pos[0])
                if idx >= 0:
                    break
                self._readmore(buf, pos)
            pos[0] = idx + 4  # Can't be any headers,
                              # but let's ignore them anyway
            return result

        def readrequest():
            while True:
                idx = buf.find(b'\r\n\r\n', pos[0])
                if idx >= 0:
                    break
                self._readmore(buf, pos)
            head = buf[pos[0]:idx]
            pos[0] = idx + 4
            lines = iter(head.decode('ascii').split('\r\n'))
            status = next(lines)
            headers = {}
            last_header = None
            for line in lines:
                if line.startswith((' ', '\t')):
                    if last_header is not None:
                        headers[last_header] += line
                    else:
                        raise EOFError("Wrong http headers")
                elif ':' in line:
                    k, v = line.split(':', 1)
                    k = k.strip()
                    if k in headers:
                        headers[k] += ', ' + v.strip()
                    else:
                        headers[k] = v.strip()
                else:
                    raise EOFError("Wrong http headers")
            te = headers.get('Transfer-Encoding', '')
            if te == 'chunked':
                return status, headers, readchunked()
            elif te:
                raise EOFError('Wrong transfer encoding {!r}'.format(te))

            clen = headers.get('Content-Length', None)

            if clen is None:
                if headers.get('Connection', '').lower() != 'close':
                    raise EOFError('Impossible to determine content length')
                try:
                    while True:
                        self._readmore(buf, pos)
                except EOFError:
                     return status, headers, buf

            clen = int(clen)
            if clen < 0:
                raise EOFError("Wrong content length")
            while pos[0] + clen > len(buf):
                self._readmore(buf, pos)
            return status, headers, buf[pos[0]:pos[0]+clen]

        while True:
            self.produce(readrequest())


class SecureRequestChannel(RequestChannel):

    def _connect(self, addr):
        super()._connect(addr)
        self._sock = ssl.SSLSocket(sock=self._sock,
            do_handshake_on_connect=False,
            ssl_version=ssl.PROTOCOL_TLSv1)
        while True:
            try:
                self._sock.do_handshake()
            except ssl.SSLWantReadError:
                gethub().do_read(self._sock)
            except ssl.SSLWantWriteError:
                gethub().do_write(self._sock)
            else:
                break

    def sender(self):
        buf = bytearray()

        add_chunk = buf.extend
        wait_write = gethub().do_write
        wait_read = gethub().do_read

        while True:
            if not buf:
                self.wait_requests()
            if not self._alive:
                return
            for chunk in self.get_pending_requests():
                add_chunk(chunk)
            try:
                bytes = self._sock.send(buf)
            except ssl.SSLWantReadError:
                wait_read(self._sock)
            except ssl.SSLWantWriteError:
                wait_write(self._sock)
            if not bytes:
                raise EOFError("Connection closed by peer")
            del buf[:bytes]

    def _readmore(self, buf, pos):
        while True:
            try:
                if pos[0]*2 > len(buf):
                    del buf[:pos[0]]
                    pos[0] = 0
                bytes = self._sock.recv(self.BUFSIZE)
                if not bytes:
                    raise EOFError("Connection closed by peer")
                buf.extend(bytes)
            except ssl.SSLWantReadError:
                gethub().do_read(self._sock)
            except ssl.SSLWantWriteError:
                gethub().do_write(self._sock)
            else:
                break


class HTTPClient(object):

    def __init__(self, host, port=80, unixsock=None, response_class=Response):
        self.host = host
        self.port = port
        self.unixsock = unixsock
        self.response_class = response_class
        self._channel = None
        self._channel_lock = Lock()

    def connection(self):
        if not self._channel:
            with self._channel_lock:
                if not self._channel:
                    self._channel = RequestChannel(self.host, self.port,
                        unixsock=self.unixsock)
        return self._channel

    def request(self, uri, *,
            method='GET',
            query=None,
            headers={},
            body=None):
        conn = self.connection()
        assert method.isidentifier(), method
        assert uri.startswith('/'), uri
        if query:
            if '?' in uri:
                uri += '&' + urlencode(query)
            else:
                uri += '?' + urlencode(query)
        headers = headers.copy()
        statusline = '{} {} HTTP/1.1'.format(method.upper(), uri)
        lines = [statusline]
        if isinstance(body, dict):
            body = urlencode(body)
        if isinstance(body, str):
            body = body.encode('utf-8')  # there are no other encodings, right?
        if body is not None:
            clen = len(body)
        else:
            clen = 0
            body = b''
        headers['Content-Length'] = clen
        for k, v in headers.items():
            lines.append('{}: {}'.format(k, str(v)))
        lines.append('')
        lines.append('')
        buf = '\r\n'.join(lines).encode('ascii')
        return self.response_class(*conn.request(buf + body).get())


class HTTPSClient(HTTPClient):

    def __init__(self, host, port=443, unixsock=None, response_class=Response):
        self.host = host
        self.port = port
        self.unixsock = unixsock
        self.response_class = response_class
        self._channel = None
        self._channel_lock = Lock()

    def connection(self):
        if not self._channel:
            with self._channel_lock:
                if not self._channel:
                    self._channel = SecureRequestChannel(self.host, self.port,
                        unixsock=self.unixsock)
        return self._channel
