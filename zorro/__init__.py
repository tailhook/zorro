from .core import Hub, gethub, Future, Condition, Lock
from contextlib import contextmanager
from functools import wraps

__version__ = '0.2.a0'

__all__ = [
    'Hub',
    'gethub',
    'sleep',
    'Future',
    'Condition',
    'Lock',
    ]

def sleep(value):
    gethub().do_sleep(value)
