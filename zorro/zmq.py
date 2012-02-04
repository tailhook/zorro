import os
import random
import struct
import errno
import time
from functools import partial

import zmq
from zmq import *

from . import core, channel


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
                hub.do_write(sock)
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
    sock = Socket(context(), zmq.XREP)
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
        self._sock = Socket(context(), zmq.XREQ)
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
        POLLIN=zmq.POLLIN, POLLOUT=zmq.POLLOUT)
    hub.log_plugged(ctx, name='zmq_context')
    return ctx


def context():
    hub = core.gethub()
    ctx = getattr(hub, 'zmq_context', None)
    if ctx is None:
        return plug(hub)
    return ctx
