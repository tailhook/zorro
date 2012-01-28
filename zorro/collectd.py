import socket
import errno
from time import time as current_time

from .core import gethub, Lock, Future
from .channel import PipelinedReqChannel


class Unix(PipelinedReqChannel):
    BUFSIZE = 1024

    def __init__(self, unixsock='/var/run/collectd-unixsock'):
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.setblocking(0)
        self._cur_producing = []
        self._todo = 0
        try:
            self._sock.connect(unixsock)
        except socket.error as e:
            if e.errno == errno.EINPROGRESS:
                gethub().do_write(self._sock)
            else:
                raise
        super().__init__()
        self._start()

    def produce(self, line):
        if self._todo:
            self._cur_producing.append(line)
            self._todo -= 1
        else:
            num, tail = line.split(b' ', 1)
            lines = int(num)
            self._todo = lines
            self._cur_producing.append(line)
        if not self._todo:
            res = tuple(self._cur_producing)
            del self._cur_producing[:]
            self._producing.popleft()[1].set(res)

    def sender(self):
        buf = bytearray()

        add_chunk = buf.extend
        wait_write = gethub().do_write
        sock = self._sock

        while True:
            if not buf:
                self.wait_requests()
            wait_write(sock)
            for chunk in self.get_pending_requests():
                add_chunk(chunk)
            try:
                bytes = sock.send(buf)
            except socket.error as e:
                if e.errno in (errno.EAGAIN, errno.EINTR):
                    continue
                elif e.errno in (errno.EPIPE, errno.ECONNRESET):
                    raise EOFError()
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
        pos = 0
        current = []

        while True:
            if pos*2 > len(buf):
                del buf[:pos]
                pos = 0
            wait_read(sock)
            try:
                bytes = sock.recv(self.BUFSIZE)
                if not bytes:
                    raise EOFError()
                add_chunk(bytes)
            except socket.error as e:
                if e.errno in (errno.EAGAIN, errno.EINTR):
                    continue
                elif e.errno in (errno.EPIPE, errno.ECONNRESET):
                    raise EOFError()
                else:
                    raise
            while True:
                idx = buf.find(b'\n', pos)
                if idx < 0:
                    break
                line = buf[pos:idx]
                self.produce(line)
                pos = idx + 1


class Connection(object):

    def __init__(self, unixsock='/var/run/collectd-unixsock'):
        # TODO(tailhook)
        self.unixsock = unixsock
        self._channel = None
        self._channel_lock = Lock()

    def channel(self):
        if not self._channel:
            with self._channel_lock:
                if not self._channel:
                    self._channel = Unix(unixsock=self.unixsock)
        return self._channel

    def putval(self, identifier, values, interval=None, time=None):
        return self.putval_future(identifier, values,
                                  interval=interval, time=time).get()

    def putval_future(self, identifier, values, interval=None, time=None):
        buf = bytearray(b'PUTVAL ')
        buf += identifier.encode('ascii')
        if interval is not None:
            buf += ' interval={0:d}'.format(interval).encode('ascii')
        for tup in values:
            if time is None:
                time = current_time()
            lst = [str(int(time))]
            for val in tup:
                if val is None:
                    lst.append('U')
                else:
                    lst.append(str(float(val)))
            buf += b' '
            buf += ':'.join(lst).encode('ascii')
        return self.channel().request(buf)

    def flush(self, timeout=None, plugin=None, identifier=None):
        buf = bytearray(b'FLUSH')
        if timeout is not None:
            buf += ' timeout={}'.format(timeout).encode('ascii')
        if plugin is not None:
            buf += ' plugin={}'.format(plugin).encode('ascii')
        if identifier is not None:
            buf += ' identifier={}'.format(identifier).encode('ascii')
        return self.channel().request(buf).get()

    def putnotif(self, message, severity='warning', time=None, host=None,
        plugin=None, plugin_instance=None, type=None, type_instance=None):
        buf = bytearray(b'PUTNOTIF')
        buf += ' message="{}"'.format(message.replace('"',"'")).encode('ascii')
        buf += ' severity={}'.format(severity).encode('ascii')
        if time is None:
            time = int(current_time())
        buf += ' time={}'.format(time).encode('ascii')
        if host:
            ' host={}'.format(host).encode('ascii')
        if plugin:
            ' plugin={}'.format(plugin).encode('ascii')
        if plugin_instance:
            ' plugin_instance={}'.format(plugin_instance).encode('ascii')
        if type:
            ' type={}'.format(type).encode('ascii')
        if type_instance:
            ' type_instance={}'.format(type_instance).encode('ascii')
        return self.channel().request(buf).get()

    def getval(self, identifier):
        buf = bytearray(b'GETVAL ')
        buf += identifier.encode('ascii')
        res = {}
        for line in self.channel().request(buf).get()[1:]:
            name, value = line.split(b'=')
            res[name.decode('ascii')] = float(value)
        return res

    def listval(self):
        buf = bytearray(b'LISTVAL')
        for line in self.channel().request(buf).get()[1:]:
            num, ident = line.split(b' ')
            num = int(num)
            yield ident.decode('ascii'), num

    # TODO(tailhook) implement other commands

