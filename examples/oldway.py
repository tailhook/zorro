import redis
import zmq


ctx = zmq.Context(1)
sock = ctx.socket(zmq.REP)
sock.connect('tcp://localhost:7004')
r = redis.Redis()


while True:
    arg, = sock.recv_multipart()
    for i in xrange(10):
        nvisits = r.incr(b'visits:' + arg)
    v = r.get(b'page:' + arg)
    sock.send_multipart([v + b' ' + str(nvisits).encode('utf-8')])
