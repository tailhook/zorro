from . import Future, Condition, gethub
from collections import deque

class PipelinedReqChannel(object):

    def __init__(self):
        self._pending = deque()
        self._producing = deque()
        self._cur_producing = []
        self._cond = Condition()
        hub = gethub()
        hub.do_spawnhelper(self.sender)
        hub.do_spawnhelper(self.receiver)

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
