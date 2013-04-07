import socket
import errno

from .core import gethub, Lock
from . import channel
from .util import setcloexec


convert = {
    str: lambda a: a.encode('utf-8'),
    bytes: lambda a: a,
    bytearray: lambda a: a,
    int: lambda a: bytes(str(a), 'utf-8'),
    float: lambda a: bytes(repr(a), 'utf-8'),
    }


def encode_command(buf, parts):
    add = buf.extend
    cvt = convert
    add('*{:d}\r\n'.format(len(parts)).encode('ascii'))
    for part in parts:
        value = cvt[part.__class__](part)
        add('${:d}\r\n'.format(len(value)).encode("ascii"))
        add(value)
        add(b'\r\n')
    return buf


class RedisError(Exception):
    pass


class RedisChannel(channel.PipelinedReqChannel):
    BUFSIZE = 16384

    def __init__(self, host, port, unixsock, db):
        super().__init__()
        if unixsock:
            self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            self._sock = socket.socket(socket.AF_INET,
                socket.SOCK_STREAM, socket.IPPROTO_TCP)
        setcloexec(self._sock)
        self._sock.setblocking(0)
        try:
            if unixsock:
                self._sock.connect(unixsock)
            else:
                self._sock.connect((host, port))
        except socket.error as e:
            if e.errno == errno.EINPROGRESS:
                gethub().do_write(self._sock)
            else:
                raise
        self._start()
        db = str(db)
        assert self.request('*2\r\n$6\r\nSELECT\r\n${0}\r\n{1}\r\n'
            .format(len(db), db).encode('ascii')).get() == 'OK'

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
                raise EOFError()
            del buf[:bytes]

    def receiver(self):
        buf = bytearray()

        sock = self._sock
        wait_read = gethub().do_read
        add_chunk = buf.extend
        pos = [0]

        def readmore():
            while True:
                wait_read(sock)
                try:
                    if pos[0]*2 > len(buf):
                        del buf[:pos[0]]
                        pos[0] = 0
                    bytes = sock.recv(self.BUFSIZE)
                    if not bytes:
                        raise EOFError()
                    add_chunk(bytes)
                except socket.error as e:
                    if e.errno in (errno.EAGAIN, errno.EINTR):
                        continue
                    else:
                        raise
                else:
                    break

        def readchar():
            if len(buf) <= pos[0]:
                readmore()
            c = buf[pos[0]]
            pos[0] += 1
            return c

        def readline():
            if len(buf) < 2 or pos[0] >= len(buf):
                readmore()
            while True:
                try:
                    idx = buf.index(b'\r\n', pos[0])
                except ValueError:
                    pass
                else:
                    break
                readmore()
            res = buf[pos[0]:idx]
            pos[0] = idx + 2
            return res

        def readslice(ln):
            while len(buf) - pos[0] < ln:
                readmore()
            res = buf[pos[0]:pos[0]+ln]
            pos[0] += ln
            return res

        def readone():
            ch = readchar()
            if ch == 42: # b'*'
                cnt = int(readline())
                return [readone() for i in range(cnt)]
            elif ch == 43: # b'+'
                return readline().decode('ascii')
            elif ch == 45: # b'-'
                return RedisError(readline().decode('ascii'))
            elif ch == 58: # b':'
                return int(readline())
            elif ch == 36: # b'$'
                ln = int(readline())
                if ln < 0:
                    return None
                res = readslice(ln)
                assert readline() == b''
                return res
            else:
                raise NotImplementedError(ch)

        while True:
            self.produce(readone())


class Redis(object):
    def __init__(self, host='localhost', port=6379, unixsock=None, db=0):
        self.unixsock = unixsock
        self.host = host
        self.port = port
        self.db = db
        self._channel = None
        self._channel_lock = Lock()

    def check_connection(self):
        if not self._channel:
            with self._channel_lock:
                if not self._channel:
                    self._channel = RedisChannel(self.host, self.port,
                        db=self.db, unixsock=self.unixsock)

    # low-level stuff
    def execute(self, *args):
        self.check_connection()
        buf = bytearray()
        encode_command(buf, args)
        return self._channel.request(buf).get()

    def future(self, *args):
        self.check_connection()
        buf = bytearray()
        encode_command(buf, args)
        return self._channel.request(buf)

    def pipeline(self, commands):
        self.check_connection()
        buf = bytearray()
        for cmd in commands:
            encode_command(buf, cmd)
        return self._channel.request(buf, len(commands)).get()

    def bulk(self, commands):
        self.check_connection()
        if commands[0][0] != 'MULTI' or commands[-1][0] != 'EXEC':
            raise ValueError("Bulk must start with MULTI and end with EXEC")
        buf = bytearray()
        for cmd in commands:
            encode_command(buf, cmd)
        val = self._channel.request(buf, len(commands)).get()
        if val[0] != 'OK':
            raise RuntimeError(val, commands)
        for i in val[1:-1]:
            if isinstance(i, RedisError):
                raise i
            assert i == 'QUEUED'
        return val[-1]
    # high-level stuff
