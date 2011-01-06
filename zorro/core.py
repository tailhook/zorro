import greenlet
import heapq
import time
import threading
import select
from collections import deque, defaultdict
from functools import partial
from operator import methodcaller

from .util import priorityqueue, orderedset, socket_pair

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
        elif isinstance(self.run, partial):
            return '<Z{:x} {}>'.format(id(self),
                getattr(self.run.func, '__name__', self.run.func))
        else:
            return '<Z{:x} {}>'.format(id(self),
                getattr(self.run, '__name__', self.run))
        
class Hub(object):
    def __init__(self):
        self._queue = orderedset()
        try:
            self._poller = select.epoll()
            self.POLLIN = select.EPOLLIN
            self.POLLOUT = select.EPOLLOUT
        except AttributeError:
            self._poller = select.poll()
            self.POLLIN = select.POLLIN
            self.POLLOUT = select.EPOLLOUT
        self._filedes = methodcaller('fileno')
        self._timeouts = priorityqueue()
        self._in_sockets = defaultdict(list)
        self._out_sockets = defaultdict(list)
        self._loops = set()
        self._control = socket_pair()
        self._poller.register(self._control[0], self.POLLIN)
        self._control_fd = self._control[0].fileno()
    
    # global methods
    
    def change_poller(self, cls, POLLIN, POLLOUT,
        filedes=methodcaller('fileno')):
        if hasattr(self._poller, 'close'):
            self._poller.close()
        self._poller = cls()
        self.POLLIN = POLLIN
        self.POLLOUT = POLLOUT
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
        """Stop all infinite loops, and wait for other tasks to complete"""
        self.stopping = True
        if threading.current_thread().ident != self._thread:
            self.wakeup()
        else:
            while self._loops:
                self._loops.pop().detach().throw(greenlet.GreenletExit())
    
    def crash(self):
        """Rude stop of hub at next iteration"""
        self.stopped = True
        if threading.current_thread().ident != self._thread:
            self.wakeup()
        
    def run(self, *funcs):
        self.stopping = False
        self.stopped = False
        self._self = greenlet.getcurrent()
        self._thread = threading.current_thread().ident
        for f in funcs:
            self.do_spawn(f)
        while self.queue() or self.io() or self.timeouts():
            if self.stopped:
                break
            elif self.stopping and self._loops:
                self.stop()
        self.stopping = True
        self.stopped = True
    
    # Internals
    
    def queue(self):
        while not self.stopped:
            tsk = self._queue.first()
            if tsk is None:
                return
            t = tsk[0]
            t.detach()
            t.switch(*tsk[1:])
    
    def io(self):
        timeo = self._timeouts.min()
        if timeo is None and not self._in_sockets and not self._out_sockets:
            return False
        if timeo is not None:
            timeo = max(timeo - time.time(), 0)
        items = self._poller.poll(timeout=timeo)
        if not items:
            return False
        for fd, ev in items:
            if ev & self.POLLOUT:
                if fd in self._out_sockets:
                    task = self._out_sockets[fd][0]
                    self.queue_task(task, 'read')
            if ev & self.POLLIN:
                if fd in self._in_sockets:
                    task = self._in_sockets[fd][0]
                    self.queue_task(task, 'write')
                elif fd == self._control_fd:
                    self._control[0].recv(1024) # throw it, just need wake up
        return True
    
    def timeouts(self):
        if not self._timeouts:
            return False
        now = time.time()
        while True:
            task = self._timeouts.pop(now)
            if task is None:
                break
            self.queue_task(task, 'timeout')
        return True
    
    def queue_task(self, task, *value):
        task.detach()
        task.cleanup.append(self._queue.add(task, *value))
        return task
        
    # Helper methods
    def do_sleep(self, tm, more=False):
        targ = time.time() + tm
        let = greenlet.getcurrent()
        let.cleanup.append(self._timeouts.add(targ, let))
        if not more:
            self._self.switch()
    
    def _check_mask(self, fd):
        msk = 0
        if fd in self._in_sockets:
            msk |= self.POLLIN
        if fd in self._out_sockets:
            msk |= self.POLLOUT
        if msk:
            self._poller.modify(fd, msk)
        else:
            self._poller.unregister(fd)
    
    def _queue_sock(self, sock, dic, more=False):
        let = greenlet.getcurrent()
        fd = self._filedes(sock)
        def deque_sock(let):
            items.remove(let)
            if not items:
                del dic[fd]
            self._check_mask(fd)
        let.cleanup.append(deque_sock)
        items = dic[fd]
        items.append(let)
        self._check_mask(fd)
        if not more:
            self._self.switch()
    
    def do_read(self, sock, more=False):
        self._queue_sock(sock, self._in_sockets, more=more)
        
    def do_write(self, sock, more=False):
        self._queue_sock(sock, self._out_sockets, more=more)
    
    def do_spawnloop(self, fun):
        if self.stopping:
            raise RuntimeError("Trying to spawn a loop while stopping")
        def _spawnloop():
            let = greenlet.getcurrent()
            self._loops.add(let)
            try:
                fun()
            finally:
                self._loops.discard(let) # could be removed by hub.stop()
        self.do_spawn(_spawnloop)
    
    def do_spawn(self, fun):
        return self.queue_task(Zorrolet(fun, self))
    
        
def gethub():
    let = greenlet.getcurrent()
    return let.hub
