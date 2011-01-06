import greenlet
import heapq
import time
import zmq
import threading
from collections import deque, defaultdict
from functools import partial

from .util import priorityqueue, orderedset, socketpair

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
        
class Hub(object):
    def __init__(self):
        self._queue = orderedset()
        self._poller = zmq.Poller()
        self._timeouts = priorityqueue()
        self._in_sockets = defaultdict(list)
        self._out_sockets = defaultdict(list)
        self._loops = set()
        self._control = socketpair()
        self._poller.register(self._control[0], zmq.POLLIN)
        self._control_fd = self._control[0].fileno()
    
    # global methods
    
    def wakeup(self):
        self._control[1].send(b'x')
    
    def stop(self):
        """Stop all infinite loops, and wait for other tasks to complete"""
        self.stopping = True
        if threading.current_thread().ident != self._thread:
            self.wakeup()
        else:
            while self._loops:
                self._loops.pop().detach().throw(GeneratorExit())
    
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
            tsk[0].detach().switch(*tsk[1:])
    
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
            if ev & zmq.POLLOUT:
                if fd in self._out_sockets:
                    task = self._out_sockets[fd][0]
                    self.queue_task(task, 'read')
            if ev & zmq.POLLIN:
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
            msk |= zmq.POLLIN
        if fd in self._out_sockets:
            msk |= zmq.POLLOUT
        if msk:
            self._poller.register(fd, msk)
        else:
            self._poller.unregister(fd)
    
    def _queue_sock(self, sock, dic, more=False):
        let = greenlet.getcurrent()
        if isinstance(sock, zmq.Socket):
            fd = sock
        else:
            fd = sock.fileno()
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
            except GeneratorExit:
                pass
            finally:
                self._loops.discard(let) # could be removed by hub.stop()
        self.do_spawn(_spawnloop)
    
    def do_spawn(self, fun):
        return self.queue_task(Zorrolet(fun, self))
    
        
def gethub():
    let = greenlet.getcurrent()
    return let.hub
