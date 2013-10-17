import fcntl
from heapq import heappush, heappop
from collections import deque


class orderedset(object):

    def __init__(self):
        self.deque = deque()

    def add(self, *val):
        self.deque.append(val)
        def remover(*a):
            self.deque.remove(val)
        return remover

    def update(self, value):
        self.deque.extend(value)

    def first(self):
        if self.deque:
            return self.deque[0]
        return None

    def remove(self, value):
        self.deque.remove(value)

    def __bool__(self):
        return bool(self.deque)


class priorityqueue(object):

    def __init__(self):
        self.heap = []
        self.counter = 0

    def add(self, pri, task):
        self.counter += 1
        item = [pri, self.counter, task]
        heappush(self.heap, item)
        def remover(*a):
            item[2] = None
        return remover

    def min(self):
        while self.heap and self.heap[0][2] is None:
            heappop(self.heap)
        if self.heap:
            return self.heap[0][0]
        return None

    def pop(self, value):
        val = self.min()
        if val is not None and val <= value:
            return self.heap[0][2]
        return None

    def __bool__(self):
        return bool(self.heap)


try:
    from socket import socketpair
except ImportError:
    import socket
    def socketpair():
        s=socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', 0))
        host, port = s.getsockname()
        s.listen(1)
        a=socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        a.connect((host, port))
        b, _ = s.accept()
        s.close()
        a.setblocking(0)
        b.setblocking(0)
        return a, b


def socket_pair():
    a, b = socketpair()
    a.setblocking(0)
    b.setblocking(0)
    return a, b


class cached_property(object):

    def __init__(self, fun):
        self.function = fun
        self.name = fun.__name__

    def __get__(self, obj, cls):
        if obj is None:
            return self
        res = obj.__dict__[self.name] = self.function(obj)
        return res


class marker_object(object):
    __slots__ = ('name',)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<{}>'.format(self.name)


def setcloexec(sock):
    flags = fcntl.fcntl(sock, fcntl.F_GETFD)
    fcntl.fcntl(sock, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)
