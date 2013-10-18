import heapq
import time
import threading
import select
import weakref
import logging
import errno
from collections import deque, defaultdict
from functools import partial
from operator import methodcaller
from math import ceil

import greenlet

from .util import priorityqueue, orderedset, socket_pair, marker_object

__all__ = [
    'Zorrolet',
    'Hub',
    'gethub',
    'Future',
    'Condition',
    'Lock',
    ]

FUTURE_EXCEPTION = marker_object('FUTURE_EXCEPTION')
FUTURE_PENDING = marker_object('FUTURE_PENDING')

os_errors = (IOError, OSError)


class TimeoutError(Exception):
    pass


class WaitingError(Exception):
    """Is raised when poll returned POLLHUP or POLLERR"""


class Zorrolet(greenlet.greenlet):
    __slots__ = ('hub', 'cleanup')

    def __init__(self, fun, hub):
        super().__init__(fun, parent=hub._self)
        self.hub = hub
        self.cleanup = []

    def detach(self):
        for cleanup in self.cleanup:
            cleanup(self)
        del self.cleanup[:]
        return self

    def __repr__(self):
        if self.gr_frame:
            return '<Z{:x} {}:{}>'.format(id(self),
                self.gr_frame.f_code.co_filename, self.gr_frame.f_lineno)
        elif self.dead:
            return '<Z{:x} dead>'.format(id(self))
        else:
            r = getattr(self, 'run', None)
            if isinstance(r, partial):
                return '<Z{:x} {}>'.format(id(self),
                    getattr(r.func, '__name__', r.func))
            else:
                return '<Z{:x} {}>'.format(id(self),
                    getattr(r, '__name__', r))


class EpollWrapper(object):
    delegate_methods = ('register', 'unregister', 'modify', 'close')

    def __init__(self, poll):
        self._poll = poll
        for name in self.delegate_methods:
            setattr(self, name, getattr(poll, name))

    def poll(self, timeout=-1, maxevents=-1):
        if timeout > 0:
            return self._poll.poll(timeout/1000, maxevents)
        else:
            return self._poll.poll(timeout, maxevents)


class Hub(object):
    def __init__(self):
        self._log = logging.getLogger('zorro.hub.{:x}'.format(id(self)))
        self._queue = orderedset()
        try:
            self._poller = EpollWrapper(select.epoll())
            self.POLLIN = select.EPOLLIN
            self.POLLOUT = select.EPOLLOUT
            self.POLLHUP = select.EPOLLHUP
            self.POLLERR = select.EPOLLERR
        except AttributeError:
            self._poller = select.poll()
            self.POLLIN = select.POLLIN
            self.POLLOUT = select.POLLOUT
            self.POLLHUP = select.POLLHUP
            self.POLLERR = select.POLLERR
            self._log.info("Using poller %r", self._poller)
        self._filedes = methodcaller('fileno')
        self._timeouts = priorityqueue()
        self._in_sockets = defaultdict(list)
        self._out_sockets = defaultdict(list)
        self._control = socket_pair()
        self._poller.register(self._control[0], self.POLLIN)
        self._control_fd = self._control[0].fileno()
        self._start_tasks = []
        self._services = weakref.WeakKeyDictionary()
        self._tasks = weakref.WeakKeyDictionary()
        self._helpers = weakref.WeakKeyDictionary()

    # global methods

    def change_poller(self, cls, *, POLLIN, POLLOUT, POLLERR, POLLHUP,
        filedes=methodcaller('fileno')):
        self._log.info("Changing poller from %r to %r",
            self._poller.__class__, cls)
        if hasattr(self._poller, 'close'):
            self._poller.close()
        self._poller = cls()
        self.POLLIN = POLLIN
        self.POLLOUT = POLLOUT
        self.POLLHUP = POLLHUP
        self.POLLERR = POLLERR
        self._filedes = filedes
        for k in set(self._in_sockets).union(self._out_sockets):
            msk = 0
            if k in self._in_sockets:
                msk |= POLLIN
            elif k in self._out_sockets:
                msk |= POLLOUT
            self._poller.register(k, msk)
        if hasattr(self, '_control'):
            self._poller.register(self._control[0], self.POLLIN)
            self._control_fd = self._control[0].fileno()

    def wakeup(self):
        self._control[1].send(b'x')

    def stop(self):
        """Stop all services, and wait for other tasks to complete"""
        self._log.warning("Stop called from thread ``%s'' and %r",
            threading.current_thread().name, greenlet.getcurrent())
        self.stopping = True
        if threading.current_thread().ident != self._thread:
            self.wakeup()
        else:
            self.shutdown_tasks(self._services, self._tasks)

    def shutdown_tasks(self, src, tgt):
        for i in list(src.keys()):
            del src[i]
            tgt[i] = None
            try:
                i.detach().throw(greenlet.GreenletExit())
            except BaseException as e:
                self.log_exception(e)

    def crash(self):
        """Rude stop of hub at next iteration"""
        self._log.warning("Crash called from thread ``%s'' and %r",
            threading.current_thread().name, greenlet.getcurrent())
        self.stopped = True
        if threading.current_thread().ident != self._thread:
            self.wakeup()

    def run(self, *tasks):
        self.stopping = False
        self.stopped = False
        self._self = greenlet.getcurrent()
        self._thread = threading.current_thread().ident
        self._log.warning("Starting in thread ``%s'' and %r",
            threading.current_thread().name, self._self)
        for f in self._start_tasks:
            self.do_spawn(f)
        del self._start_tasks
        for f in tasks:
            self.do_spawn(f)
        while True:
            self.queue()
            self.timeouts()

            if self.stopped:
                self._log.warning("Breaking main loop")
                break
            elif self.stopping and self._services:
                self._log.warning("Stopping services")
                self.shutdown_tasks(self._services, self._tasks)
            elif not self._tasks and not self._services and self._helpers:
                self._log.warning("No more active tasks, stopping helpers")
                self.shutdown_tasks(self._helpers, self._tasks)
            if not self._tasks and not self._services:
                break

            if not self._queue:
                self.io()

        self._log.warning("Hub stopped")
        self.stopping = True
        self.stopped = True

    # Logging methods

    def log_plugged(self, plugin, name):
        self._log.info("Plugged in %r under the name %r", plugin, name)

    def log_exception(self, e):
        self._log.error("Exception in one of spawned tasks", exc_info=e)

    # Internals

    def queue(self):
        while not self.stopped:
            tsk = self._queue.first()
            if tsk is None:
                return
            t = tsk[0]
            t.detach()
            try:
                t.switch(*tsk[1:])
            except BaseException as e:
                self.log_exception(e)

    def io(self):
        timeo = self._timeouts.min()
        if timeo is not None:
            timeo = int(ceil(max(timeo - time.time(), 0)*1000))
        else:
            timeo = -1
        try:
            items = self._poller.poll(timeo)
        except os_errors as e:
            if e.errno in (errno.EINTR, errno.EAGAIN):
                return
            raise
        else:
            POLLOUT = self.POLLOUT
            POLLIN = self.POLLIN
            POLLERR = self.POLLHUP | self.POLLERR
            for fd, ev in items:
                if ev & POLLOUT:
                    if fd in self._out_sockets:
                        task = self._out_sockets[fd][0]
                        self.queue_task(task, 'read')
                if ev & POLLIN:
                    if fd in self._in_sockets:
                        task = self._in_sockets[fd][0]
                        self.queue_task(task, 'write')
                    elif fd == self._control_fd:
                        self._control[0].recv(1024)
                        # throw it, just need wake up
                if ev & POLLERR:
                    if fd in self._in_sockets:
                        task = self._in_sockets[fd][0]
                        self.queue_task(task, 'err')
                    if fd in self._out_sockets:
                        task = self._out_sockets[fd][0]
                        self.queue_task(task, 'err')


    def timeouts(self):
        if not self._timeouts:
            return
        now = time.time()
        while True:
            task = self._timeouts.pop(now)
            if task is None:
                break
            self.queue_task(task, 'timeout')

    def queue_task(self, task, *value):
        task.detach()
        task.cleanup.append(self._queue.add(task, *value))
        return task

    # Helper methods
    def do_sleep(self, tm, more=False):
        targ = time.time() + tm
        let = greenlet.getcurrent()
        let.cleanup.append(self._timeouts.add(targ, let))
        del let # no cycles
        if not more:
            self._self.switch()

    def _check_mask(self, fd, new=False):
        msk = 0
        if fd in self._in_sockets:
            msk |= self.POLLIN
        if fd in self._out_sockets:
            msk |= self.POLLOUT
        if new:
            self._poller.register(fd, msk)
        elif msk:
            self._poller.modify(fd, msk)
        else:
            self._poller.unregister(fd)

    def _queue_sock(self, sock, dic, odic, more=False):
        fd = self._filedes(sock)
        def deque_sock(let):
            items.remove(let)
            if not items:
                del dic[fd]
            self._check_mask(fd)
        let = greenlet.getcurrent()
        let.cleanup.append(deque_sock)
        items = dic[fd]
        items.append(let)
        self._check_mask(fd, new=len(items) == 1 and fd not in odic)
        del let
        if not more:
            val = self._self.switch()
            if val == 'err':
                raise WaitingError()

    def do_read(self, sock, more=False):
        self._queue_sock(sock, self._in_sockets, self._out_sockets, more=more)

    def do_write(self, sock, more=False):
        self._queue_sock(sock, self._out_sockets, self._in_sockets, more=more)

    def do_spawnservice(self, fun):
        if self.stopping:
            raise RuntimeError("Trying to spawn a service while stopping")
        let = Zorrolet(fun, self)
        self._services[let] = None
        self.queue_task(let)
        return let

    def do_spawn(self, fun):
        let = Zorrolet(fun, self)
        self._tasks[let] = None
        self.queue_task(let)
        return let

    def do_spawnswitch(self, fun):
        let = Zorrolet(fun, self)
        self._tasks[let] = None
        self.queue_task(greenlet.getcurrent())
        let.switch()
        return let

    def do_spawnhelper(self, fun):
        let = Zorrolet(fun, self)
        self._helpers[let] = None
        self.queue_task(let)
        return let

    def add_task(self, fun):
        self._start_tasks.append(fun)


def gethub():
    let = greenlet.getcurrent()
    return let.hub


class Condition(object):

    def __init__(self):
        self._queue = deque()

    def notify(self):
        if self._queue:
            tsk = self._queue[0]
            tsk.hub.queue_task(tsk)

    def wait(self, timeout=None):
        cur = greenlet.getcurrent()
        cur.cleanup.append(self._queue.remove)
        self._queue.append(cur)
        hub = cur.hub
        if timeout is not None:
            targ = time.time() + timeout
            cur.cleanup.append(hub._timeouts.add(targ, cur))
        del cur # no cycles
        hub._self.switch()


class Future(object):
    def __init__(self, fun=None):
        self._listeners = []
        self._value = FUTURE_PENDING
        if fun is not None:
            def future():
                try:
                    result = fun()
                except Exception as e:
                    self.throw(e)
                else:
                    self.set(result)
            gethub().do_spawn(future)

    def get(self, timeout=None):
        val = self._value
        if val is not FUTURE_PENDING:
            if val is FUTURE_EXCEPTION:
                raise self._exception
            else:
                return val
        cur = greenlet.getcurrent()
        cur.cleanup.append(self._listeners.remove)
        self._listeners.append(cur)
        hub = cur.hub
        if timeout is not None:
            targ = time.time() + timeout
            cur.cleanup.append(hub._timeouts.add(targ, cur))
        del cur # no cycles
        hub._self.switch()
        val = self._value
        if val is FUTURE_PENDING:
            raise TimeoutError()
        if val is FUTURE_EXCEPTION:
            raise self._exception
        else:
            return val

    def set(self, value):
        if self._value is not FUTURE_PENDING:
            raise RuntimeError("Value is already set")
        self._value = value
        lst = self._listeners
        del self._listeners
        hub = gethub()
        for one in lst:
            hub.queue_task(one)

    def throw(self, exception):
        self._value = FUTURE_EXCEPTION
        self._exception = exception
        lst = self._listeners
        del self._listeners
        for one in lst:
            gethub().queue_task(one)

    def check(self):
        return self._value is FUTURE_PENDING


class Lock(Condition):
    def __init__(self):
        super().__init__()
        self._locked = False

    def acquire(self):
        while self._locked:
            self.wait()
        self._locked = True

    def release(self):
        self._locked = False
        self.notify()

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.release()
