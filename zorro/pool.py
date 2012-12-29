from functools import partial

from greenlet import getcurrent, GreenletExit

from .core import gethub, Condition
from . import sleep, TimeoutError


class Pool(object):

    def __init__(self, callback, *, limit, timeout):
        self.callback = callback
        self.limit = limit
        self.timeout = timeout
        self.current = 0
        self._cond = Condition()

    def __call__(self, *args, **kw):
        self.current += 1
        cur = getcurrent()
        killer = gethub().do_spawn(partial(self._timeout, cur))
        try:
            return self.callback(*args, **kw)
        finally:
            if not killer.dead:
                killer.detach().parent = cur
                killer.throw(GreenletExit())
            self.current -= 1
            self._cond.notify()

    def _timeout(self, task):
        sleep(self.timeout)
        cur = getcurrent()
        cur.parent = task.detach()
        raise TimeoutError()

    def wait_slot(self):
        while self.current >= self.limit:
            self._cond.wait()

