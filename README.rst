=====
Zorro
=====

This is mostly networking library. It implements main loop based on greenlets.
Mostly suitable for servers of some kind.

API's implemented so far:

* Zeromq
* Redis
* MySQL
* Mongo
* HTTP/HTTPS (client)
* DNS (async)

Features:

* Only for Python3 (works with 3.2, needs 3.3 for zorro.web)
* Full set of synchronisation primitives (incl. Futures, Lock, Conditions...)
* Pipelining for all network protocols
* Pure python implementation (still outperforms C implementations for many
  tasks because of pipelining)
* Basic web framework for zerogw
* Pluggable polling mechanisms

Usage::

    from zorro import Hub

    hub = Hub()
    @hub.run
    def main():
        # setup other coroutines here
        return

Basic zmq replier example. Each reply will get it's own microthread::

    from zorro import Hub, zmq

    def replier(preference,*other_multipart_args):
        if preference == b'binary':
            return b'hello'
        elif preference == b'unicode':
            return 'hello' # same as above, encoded in 'utf-8'
        elif preference == b'tuple':
            return 'hello', 'world' # two parts will be sent
        else:
            # exeption will be logged, but reply is not sent
            # so you must timeout on the other side
            # other requests will be ok (we use ZMQ_XREP actually)
            raise ValueError(preference)

    hub = Hub()
    @hub.run
    def main():
        sock = zmq.rep_socket(replier)
        sock.connect('tcp://somewhere')

Some advanced redis usage example::

    from zorro import Hub, redis, Future
    from functools import partial

    hub = Hub()
    redis = redis.Redis()

    def getkey(index):
        # Semi-parallel requests will be pipelined so it's quite fast
        a = redis.execute('INCR', 'test:{0}'.format(index-1), 1)
        redis.execute('DECR', 'test:{0}'.format(index+1), a)
        return int(redis.execute('GET', 'test:{0}'.format(index)))

    @hub.run
    def main():
        futures = [Future(partial(getkey, i)) for i in range(100)]
        print("TOTAL", sum(f.get() for f in futures))
