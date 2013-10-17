from functools import wraps
from contextlib import contextmanager

from .core import Hub, gethub, Future, Condition, Lock, TimeoutError


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
