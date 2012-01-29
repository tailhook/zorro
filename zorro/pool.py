from .core import gethub, Condition


class Pool(object):

    def __init__(self, callback, *, limit, timeout):
        self.callback = callback
        self.limit = limit
        self.timeout = timeout
        self.current = 0
        self._cond = Condition()

    def __call__(self, *args, **kw):
        self.current += 1
        try:
            return self.callback(*args, **kw)
        finally:
            self.current -= 1
            self._cond.notify()

    def wait_slot(self):
        while self.current >= self.limit:
            self._cond.wait()

