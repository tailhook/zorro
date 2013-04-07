from collections import deque
import logging

from greenlet import GreenletExit

from . import Future, Condition, gethub


log = logging.getLogger(__name__)


class PipeError(Exception):
    pass


class ShutdownException(Exception):
    pass


class BaseChannel(object):
    def __init__(self):
        self._alive = True
        self._pending = deque()
        self._cond = Condition()
        self._sender_alive = True
        self._receiver_alive = True

    def _start(self):
        hub = gethub()
        hub.do_spawnhelper(self._sender_wrapper)
        hub.do_spawnhelper(self._receiver_wrapper)

    def __bool__(self):
        return self._alive

    def _close_channel(self):
        pass

    def _sender_wrapper(self):
        try:
            self.sender()
        except (EOFError, ShutdownException, GreenletExit) as e:
            pass
        except Exception:
            log.exception("Error in %r's sender", self)
        finally:
            if self._alive:
                self._alive = False
                self._stop_producing()
            self._sender_alive = False
            if not self._receiver_alive:
                # both are down
                self._close_channel()

    def _receiver_wrapper(self):
        try:
            self.receiver()
        except (EOFError, ShutdownException, GreenletExit) as e:
            pass
        except Exception:
            log.exception("Error in %r's receiver", self)
        finally:
            if self._alive:
                self._alive = False
                self._stop_producing()
            self._receiver_alive = False
            if not self._sender_alive:
                # both are down
                self._close_channel()

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
        self._cond.notify()  # wake up consumer if it waits for messages

    def produce(self, value):
        if not self._alive:
            raise ShutdownException()
        val = self._cur_producing.append(value)
        num = self._producing[0][0]
        if num is None:
            del self._cur_producing[:]
            self._producing.popleft()[1].set(value)
        elif len(self._cur_producing) >= num:
            res = tuple(self._cur_producing)
            del self._cur_producing[:]
            self._producing.popleft()[1].set(res)
        else:
            return

    def request(self, input, num_output=None):
        if not self._alive:
            raise PipeError()
        val = Future()
        self._pending.append(input)
        self._producing.append((num_output, val))
        self._cond.notify()
        return val

    def push(self, input):
        """For requests which do not need an answer"""
        self._pending.append(input)
        self._cond.notify()


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
        if not self._alive:
            raise ShutdownException()
        fut = self.requests.pop(id, None)
        if fut is not None:
            fut.set(data)

