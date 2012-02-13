from .core import Hub, gethub, Future, Condition, Lock
from contextlib import contextmanager
from functools import wraps

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
