import zmq
import errno
from functools import partial

from . import core

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
    hub = core.gethub()
    ctx = getattr(hub, 'zmq_context', None)
    if ctx is None:
        ctx = hub.zmq_context = zmq.Context(1)
    sock = ctx.socket(zmq.XREP)
    hub.do_spawnloop(partial(rep_listener, sock, callback))
    return sock
