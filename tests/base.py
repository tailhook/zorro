import unittest
import threading
import sys
from functools import wraps, partial

def interactive(zfun):
    def wrapper(fun):
        @wraps(fun)
        def wrapping(self, *a, **kw):
            self.hub.add_task(partial(zfun, self, *a, **kw))
            self.thread.start()
            fun(self, *a, **kw)
        return wrapping
    return wrapper

def passive(zfun):
    @wraps(zfun)
    def wrapping(self, *a, **kw):
        exc = []
        def catch():
            try:
                zfun(self, *a, **kw)
            except BaseException as e:
                exc.append(e)
        self.hub.add_task(catch)
        self.thread.start()
        try:
            self.thread.join(self.test_timeout)
        finally:
            if exc:
                raise exc[0]
    return wrapping

class Test(unittest.TestCase):
    test_timeout = 1

    def setUp(self):
        import zorro
        from zorro import zmq
        self.z = zorro
        self.hub = self.z.Hub()
        self.thread = threading.Thread(target=self.hub.run)

    def tearDown(self):
        if not self.hub.stopping:
            self.hub.stop()
        self.thread.join(self.test_timeout)
        if self.thread.is_alive():
            try:
                if not getattr(self, 'should_timeout', False):
                    raise AssertionError("test timed out")
            finally:
                self.hub.crash()
                self.thread.join()
        for key in list(sys.modules.keys()):
            if key == 'zorro' or key.startswith('zorro.'):
                del sys.modules[key]

