import socket
import errno

"""
This implementation have done under some drug pressure and not tested yet.
But if you want to test some feedback is appreciated.
"""

__all__ = [
    'bufferedsocket',
    ]

class bufferedsocket(object):

    def __init__(self, sock, bufsize=4096):
        self._sock = sock
        self._buf = bytearray()
        self.bufsize = bufsize

    def readline(self, endline='\r\n'):
        while True:
            try:
                idx = self._buf.index(endline)
            except IndexError:
                try:
                    self._read()
                except EOFError:
                    if self._buf:
                        res = self.buf
                        self.buf = bytearray()
                        return res
                    else:
                        raise
            else:
                idx += len(endline)
                res = self.buf[:idx]
                del self.buf[:idx]
                return res

    def read(self, bytes):
        while len(self._buf) < bytes:
            self._read(bytes - len(self._buf))
        res = self.buf[:bytes]
        del self.buf[:bytes]
        return res

    def _read(self, bytes=0):
        gethub().do_read(self._sock)
        try:
            val = self._sock.read(max(bytes, self.bufsize))
        except socket.error as e:
            if e.errno in (EINTR, EAGAIN):
                return
            else:
                raise
        if not val:
            raise EOFError()
        self._buf.extend(val)

    def write(self, bytes):
        hub = gethub()
        while bytes:
            hub.do_write(self._sock)
            try:
                done = self._sock.send(bytes)
            except socket.error as e:
                if e.errno in (EINTR, EAGAIN):
                    continue
                else:
                    raise
            else:
                if not done:
                    raise EOFError()
                bytes = bytes[done:]
