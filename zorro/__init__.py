from .core import Hub, gethub, Future, Condition, Lock
from .net import bufferedsocket
from contextlib import contextmanager
from functools import wraps

__all__ = [
    'Hub',
    'gethub',
    'sleep',
    'timeout',
    'with_timeout',
    'TimeoutException',
    'Future',
    'Condition',
    ]

def sleep(value):
    gethub().do_sleep(value)

class TimeoutException(Exception):
    """Raised when you use timeout context manager and process timed out"""

class FinishedException(Exception):
    """Internal exception for timeout context manager"""

@contextmanager
def timeout(value, description="Process timed out"):
    """Context manager to limit execution time of code path to time in seconds
    """
    def _timeout():
        try:
            sleep(value)
        except FinishedException:
            pass
        else:
            raise TimeoutException(description)
    let = gethub().do_spawn(timeout)
    yield let
    let.throw(FinishedException())

def with_timeout(value, description="Process timed out"):
    """Decorator which limits execution time of function call to time in seconds
    """
    def wrapper(fun):
        @wraps(fun)
        def wrapping(*a, **k):
            with timeout(value, description):
                return fun(*a, **kw)
        return wrapping
    return wrapper
