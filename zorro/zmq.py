import zmq
import errno
from functools import partial

from . import core, channel

DEFAULT_IO_THREADS = 1

def rep_responder(sock, address, callback, data):
    hub = core.gethub()
    reply = callback(*data)
    _rep = address
    if isinstance(reply, bytes):
        _rep.append(bytes)
    elif isinstance(reply, str):
        _rep.append(bytes.encode('utf-8'))
    elif reply is None:
        raise RuntimeError("Replier callback must return either string,"
            " bytes or sequence of strings or bytes")
    else:
        for a in reply:
            if isinstance(a, str):
                _rep.append(a.encode('utf-8'))
            else:
                _rep.append(a)
    while True:
        hub.do_write(sock)
        try:
            sock.send_multipart(_rep, zmq.NOBLOCK)
        except zmq.ZMQError as e:
            if e.errno == errno.EAGAIN or e.errno == errno.EINTR:
                continue
            else:
                raise
        else:
            break

def rep_listener(sock, callback):
    hub = core.gethub()
    while True:
        hub.do_read(sock)
        try:
            data = sock.recv_multipart(zmq.NOBLOCK)
        except zmq.ZMQError as e:
            if e.errno == errno.EAGAIN or e.errno == errno.EINTR:
                continue
            else:
                raise
        i = data.index(b'')
        addr = data[:i+1]
        data = data[i+1:]
        hub.do_spawn(partial(rep_responder, sock, addr, callback, data))

def rep_socket(callback):
    sock = context().socket(zmq.XREP)
    core.gethub().do_spawnservice(partial(rep_listener, sock, callback))
    return sock

def req_socket():
    return ReqChannel()

class ReqChannel(channel.MuxReqChannel):

    def __init__(self):
        super().__init__()
        self._sock = context().socket(zmq.XREQ)

    def bind(self, value):
        self._sock.bind(value)

    def connect(self, value):
        self._sock.connect(value)

    def sender(self):
        wait_write = core.gethub().do_write
        while True:
            wait_write(self._sock)
            id, data = self.peek_request()
            self._sock.send(id, zmq.SNDMORE)
            self._sock.send(b"", zmq.SNDMORE)
            self._sock.send_multipart(data)
            self.pop_request()

    def receiver(self):
        wait_read = core.gethub().do_read
        while True:
            wait_read(self._sock)
            data = self._sock.recv_multipart()
            assert data[1] == b''
            self.produce(data[0], data[2:])


def _get_fd(value):
    if isinstance(value, zmq.Socket):
        return value
    else:
        return value.fileno()

def plug(hub, io_threads=DEFAULT_IO_THREADS):
    assert not hasattr(hub, 'zmq_context')
    ctx = hub.zmq_context = zmq.Context(io_threads)
    hub.change_poller(zmq.Poller, filedes=_get_fd,
        POLLIN=zmq.POLLIN, POLLOUT=zmq.POLLOUT)
    hub.log_plugged(ctx, name='zmq_context')
    return ctx

def context():
    hub = core.gethub()
    ctx = getattr(hub, 'zmq_context', None)
    if ctx is None:
        return plug(hub)
    return ctx
