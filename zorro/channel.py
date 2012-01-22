from . import Future, Condition, gethub
from collections import deque


class PipeError(Exception):
    pass

class ShutdownException(Exception):
    pass


class BaseChannel(object):
    def __init__(self):
        self._alive = True
        self._pending = deque()
        self._cond = Condition()
        hub = gethub()
        hub.do_spawnhelper(self._sender_wrapper)
        hub.do_spawnhelper(self._receiver_wrapper)

    def __bool__(self):
        return self._alive

    def _sender_wrapper(self):
        try:
            self.sender()
        except (EOFError, ShutdownException):
            pass
        finally:
            if self._alive:
                self._alive = False
                self._stop_producing()

    def _receiver_wrapper(self):
        try:
            self.receiver()
        except (EOFError, ShutdownException):
            pass
        finally:
            if self._alive:
                self._alive = False
                self._stop_producing()

    def _stop_producing(self):
        pass

    def peek_request(self):
        self.wait_requests()
        return self._pending[0]

    def pop_request(self):
        return self._pending.popleft()

    def wait_requests(self):
        while not self._pending:
            if not self._alive:
                raise ShutdownException()
            self._cond.wait()

    def get_pending_requests(self):
        while self._pending:
            yield self._pending.popleft()

class PipelinedReqChannel(BaseChannel):

    def __init__(self):
        super().__init__()
        self._producing = deque()
        self._cur_producing = []

    def _stop_producing(self):
        prod = self._producing
        del self._producing
        for num, fut in prod:
            fut.throw(PipeError())

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
        if not self._alive:
            raise PipeError()
        val = Future()
        self._pending.append(input)
        self._producing.append((num_output, val))
        self._cond.notify()
        return val


class MuxReqChannel(BaseChannel):
    def __init__(self):
        super().__init__()
        self.requests = {}

    def new_id(self):
        raise NotImplementedError("Abstract method")

    def _stop_producing(self):
        reqs = self.requests
        del self.requests
        for fut in reqs.values():
            fut.throw(PipeError())

    def request(self, input):
        if not self._alive:
            raise PipeError()
        id = self.new_id()
        assert not id in self.requests
        val = Future()
        self.requests[id] = val
        self._pending.append((id, input))
        self._cond.notify()
        return val

    def push(self, input):
        """For requests which do not need an answer"""
        id = self.new_id()
        assert not id in self.requests
        self._pending.append((id, input))
        self._cond.notify()

    def produce(self, id, data):
        fut = self.requests.pop(id, None)
        if fut is not None:
            fut.set(data)

