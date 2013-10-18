import os
import random
import struct
import errno
import time
import pickle
from functools import partial

import zmq
from zmq import *

from . import core, channel
from .core import gethub

try:
    REP_SOCKET = zmq.XREP
except AttributeError:
    REP_SOCKET = zmq.ROUTER
try:
    REQ_SOCKET = zmq.XREQ
except AttributeError:
    REQ_SOCKET = zmq.DEALER

DEFAULT_IO_THREADS = 1


class Socket(zmq.Socket):

    def dict_configure(self, data):
        if 'bind' in data:
            if isinstance(data['bind'], (tuple, list)):
                for addr in data['bind']:
                    self.bind(addr)
            else:
                self.bind(data['bind'])
        if 'connect' in data:
            if isinstance(data['connect'], (tuple, list)):
                for addr in data['connect']:
                    self.connect(addr)
            else:
                self.connect(data['connect'])
        # we have no identity as swap as they will be dropped
        if 'hwm' in data:
            self.setsockopt(zmq.HWM, int(data['hwm']))
        if 'affinity' in data:
            self.setsockopt(zmq.AFFINITY, int(data['affinity']))
        if 'backlog' in data:
            self.setsockopt(zmq.BACKLOG, int(data['backlog']))
        if 'linger' in data:
            self.setsockopt(zmq.LINGER, int(data['linger']))
        if 'sndbuf' in data:
            self.setsockopt(zmq.SNDBUF, int(data['sndbuf']))
        if 'rcvbuf' in data:
            self.setsockopt(zmq.RCVBUF, int(data['rcvbuf']))
        # TODO(tailhook) add other options


def send_data(sock, data, address=None):
    if address is None:
        _rep = []
    else:
        _rep = list(address)
    for a in data:
        if isinstance(a, str):
            _rep.append(a.encode('utf-8'))
        else:
            _rep.append(a)
    while True:
        try:
            sock.send_multipart(_rep, zmq.NOBLOCK)
        except zmq.ZMQError as e:
            if e.errno == errno.EAGAIN:
                gethub().do_write(sock)
                continue
            elif e.errno == errno.EINTR:
                continue
            else:
                raise
        else:
            break


def rep_responder(sock, address, callback, data):
    hub = core.gethub()
    reply = callback(*data)
    if isinstance(reply, bytes):
        send_data(sock, (reply,), address=address)
    elif isinstance(reply, str):
        send_data(sock, (reply.encode('utf-8'),), address=address)
    elif reply is None:
        raise RuntimeError("Replier callback must return either string,"
            " bytes or sequence of strings or bytes")
    else:
        send_data(sock, reply, address=address)


def rep_listener(sock, callback):
    hub = core.gethub()
    # hook for pools
    wait_slot = getattr(callback, 'wait_slot', None)
    while True:
        hub.do_read(sock)
        while True:
            if wait_slot is not None:
                wait_slot()
            try:
                data = sock.recv_multipart(zmq.NOBLOCK)
            except zmq.ZMQError as e:
                if e.errno == errno.EAGAIN or e.errno == errno.EINTR:
                    break
                else:
                    raise
            i = data.index(b'')
            addr = data[:i+1]
            data = data[i+1:]
            # we create new greenlet and immediately switch to it
            # this effectively starts processing request faster
            # but more importantly counts processing request in request pool
            hub.do_spawnswitch(
                partial(rep_responder, sock, addr, callback, data))


def rep_socket(callback):
    sock = Socket(context(), REP_SOCKET)
    core.gethub().do_spawnservice(partial(rep_listener, sock, callback))
    return sock


def sub_listener(sock, callback):
    hub = core.gethub()
    # hooks for pools
    wait_slot = getattr(callback, 'wait_slot', None)
    while True:
        hub.do_read(sock)
        while True:
            if wait_slot is not None:
                wait_slot()
            try:
                data = sock.recv_multipart(zmq.NOBLOCK)
            except zmq.ZMQError as e:
                if e.errno == errno.EAGAIN or e.errno == errno.EINTR:
                    break
                else:
                    raise
            # we create new greenlet and immediately switch to it
            # this effectively starts processing request faster
            # but more importantly counts processing request in request pool
            hub.do_spawnswitch(partial(callback, *data))


def sub_socket(callback):
    sock = Socket(context(), zmq.SUB)
    core.gethub().do_spawnservice(partial(sub_listener, sock, callback))
    return sock


def pub_socket():
    return PubChannel()


def pull_socket(callback):
    sock = Socket(context(), zmq.PULL)
    core.gethub().do_spawnservice(partial(sub_listener, sock, callback))
    return sock


def push_socket():
    return PushChannel()


def req_socket():
    return ReqChannel()


class ReqChannel(channel.MuxReqChannel):

    def __init__(self):
        super().__init__()
        self.init_id()
        self._sock = Socket(context(), REQ_SOCKET)
        self._start()

    def init_id(self):
        self.prefix = struct.pack('HHL',
            os.getpid() % 65536, random.randrange(65536), int(time.time()))
        self.counter = 0

    def new_id(self):
        self.counter += 1
        if self.counter >= 4294967296:
            self.init_id()
        return self.prefix + struct.pack('L', self.counter)

    def bind(self, value):
        self._sock.bind(value)

    def connect(self, value):
        self._sock.connect(value)

    def dict_configure(self, dic):
        self._sock.dict_configure(dic)

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


class OutputChannel(object):
    zmq_kind = None

    def __init__(self):
        super().__init__()
        self._sock = Socket(context(), self.zmq_kind)

    def bind(self, value):
        self._sock.bind(value)

    def connect(self, value):
        self._sock.connect(value)

    def dict_configure(self, dic):
        self._sock.dict_configure(dic)


class PubChannel(OutputChannel):
    zmq_kind = zmq.PUB

    def publish(self, *args):
        send_data(self._sock, args)


class PushChannel(OutputChannel):
    zmq_kind = zmq.PUSH

    def push(self, *args):
        send_data(self._sock, args)


def _get_fd(value):
    if isinstance(value, zmq.Socket):
        return value
    else:
        return value.fileno()


def plug(hub, io_threads=DEFAULT_IO_THREADS):
    assert not hasattr(hub, 'zmq_context')
    ctx = hub.zmq_context = zmq.Context(io_threads)
    hub.change_poller(zmq.Poller, filedes=_get_fd,
        POLLIN=zmq.POLLIN, POLLOUT=zmq.POLLOUT, POLLERR=zmq.POLLERR, POLLHUP=0)
    core.os_errors += (ZMQError,)
    hub.log_plugged(ctx, name='zmq_context')
    return ctx


def context():
    hub = core.gethub()
    ctx = getattr(hub, 'zmq_context', None)
    if ctx is None:
        return plug(hub)
    return ctx


class MethodCallError(Exception):
    pass


class MethodException(Exception):
    pass


class Method(object):
    __slots__ = ('owner', 'name')

    def __init__(self, owner, name):
        self.owner = owner
        self.name = name

    def __call__(self, *args):
        o = self.owner
        lst = [self.name]
        lst.extend(map(o.dumps, args))
        res = o.channel.request(lst).get()
        if res[0] == b'_result':
            return self.owner.loads(res[1])
        elif res[0] == b'_error':
            raise MethodCallError(res[1].decode('ascii'))
        elif res[0] == b'_exception':
            raise MethodException(res[1].decode('utf-8'))
        else:
            raise MethodException("Wrong reply")


class Requester(object):
    loads = pickle.loads
    dumps = pickle.dumps

    def __init__(self, channel, prefix=''):
        self.channel = channel
        if isinstance(prefix, str):
            self.prefix = prefix.encode('ascii')

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError("Method name can't start with underscore")
        fullname = self.prefix + name.encode('ascii')
        return Method(self, fullname)


class Responder(object):
    loads = pickle.loads
    dumps = pickle.dumps

    def __call__(self, name, *args):
        name = name.decode('ascii')
        if name.startswith('_'):
            return (b'_error', b'bad_name')
        meth = getattr(self, name, None)
        if meth is None or not callable(meth):
            return (b'_error', b'no_method')
        try:
            args = tuple(map(self.loads, args))
        except Exception as e:
            return (b'_error', b'unpacking_error')
        try:
            result = meth(*args)
        except Exception as e:
            # TODO(tailhook) log exception
            return (b'_exception', repr(e))
        try:
            result = self.dumps(result)
        except Exception as e:
            return (b'_error', b'packing_error')
        return (b'_result', result)


class Dispatcher(dict):

    def __init__(self, default, **kw):
        if isinstance(default, dict):
            super().__init__(default, **kw)
        else:
            self[None] = default
            super().__init__(**kw)

    def __call__(self, name, *args):
        parts = name.rsplit(b'.', 1)
        if len(parts) > 1:
            oname, name = parts
            oname = oname.decode('ascii')
        else:
            oname = None
            name = parts[0]
        obj = self.get(oname)
        if obj is None:
            return (b'_error', b'wrong_prefix')
        else:
            return obj(name, *args)


