import unittest
import threading
import sys
from functools import wraps, partial

def interactive(zfun):
    def wrapper(fun):
        @wraps(fun)
        def wrapping(self, *a, **kw):
            myhub = self.z.Hub()
            thread = threading.Thread(target=myhub.run,
                args=(partial(zfun, self, *a, **kw),))
            thread.start()
            fun(self, *a, **kw)
            myhub.stop()
            thread.join(self.test_timeout)
            if thread.is_alive():
                try:
                    raise AssertionError("test timed out")
                finally:
                    myhub.crash()
                    thread.join()
        return wrapping
    return wrapper
    
def passive(zfun):
    @wraps(zfun)
    def wrapping(self, *a, **kw):
        myhub = zorro.Hub(partial(zfun, self, *a, **kw))
        thread = threading.Thread(target=myhub.run)
        thread.start()
        thread.join(self.test_timeout)
        if thread.is_alive():
            try:
                raise AssertionError("test timed out")
            finally:
                myhub.crash()
                thread.join()
    return wrapping

class Test(unittest.TestCase):
    test_timeout = 1

    def setUp(self):
        import zorro
        from zorro import zmq, redis
        self.z = zorro
    
    def tearDown(self):
        for key in list(sys.modules.keys()):
            if key == 'zorro' or key.startswith('zorro.'):
                del sys.modules[key]
        
