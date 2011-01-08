import struct
import time
from . import Future, Condition, gethub
from collections import deque

class BaseChannel(object):
    def __init__(self):
        self._pending = deque()
        self._cond = Condition()
        hub = gethub()
        hub.do_spawnhelper(self.sender)
        hub.do_spawnhelper(self.receiver)

    def peek_request(self):
        self.wait_requests()
        return self._pending[0]

    def pop_request(self):
        return self._pending.popleft()

    def wait_requests(self):
        while not self._pending:
            self._cond.wait()

    def get_pending_requests(self):
        while self._pending:
            yield self._pending.popleft()

class PipelinedReqChannel(BaseChannel):

    def __init__(self):
        super().__init__()
        self._producing = deque()
        self._cur_producing = []

    def produce(self, value):
        val = self._cur_producing.append(value)
        num = self._producing[0][0]
        if num is None:
            self._producing.popleft()[1].set(value)
        elif len(self._cur_producing) >= num:
            res = tuple(self._cur_producing)
            self._producing.popleft()[1].set(res)
        else:
            return
        del self._cur_producing[:]

    def request(self, input, num_output=None):
        val = Future()
        self._pending.append(input)
        self._producing.append((num_output, val))
        self._cond.notify()
        return val.get()

    def wait_requests(self):
        while True:
            if self._pending:
                return
            self._cond.wait()

    def get_pending_requests(self):
        while self._pending:
            yield self._pending.popleft()

class MuxReqChannel(BaseChannel):
    def __init__(self):
        super().__init__()
        self.requests = {}
        self.init_id()

    def init_id(self):
        import os, random, struct
        self.prefix = struct.pack('HHL',
            os.getpid() % 65536, random.randrange(65536), int(time.time()))
        self.counter = 0

    def new_id(self):
        self.counter += 1
        if self.counter >= 4294967296:
            self.init_id()
            self.counter -= 4294967296
        return self.prefix + struct.pack('L', self.counter)

    def request(self, input):
        id = self.new_id()
        assert not id in self.requests
        val = Future()
        self.requests[id] = val
        self._pending.append((id, input))
        self._cond.notify()
        return val.get()

    def produce(self, id, data):
        self.requests.pop(id).set(data)

