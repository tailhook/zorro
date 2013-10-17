from zorro import Hub, redis, zmq
import logging; logging.basicConfig()


hub = Hub()
redis.plug(hub, host='localhost', db=0)


def reply(url):
    for i in range(10):
        nvisits = redis.redis().execute(b'INCR', b'visits:' + url)
    val = redis.redis().execute(b'GET', b'page:' + url)
    if val is None:
        return '404 Not Found'
    else:
        val += b' '
        val += str(nvisits).encode('utf-8')
        return bytes(val)


@hub.run
def main():
    zmq.rep_socket(reply).connect('tcp://localhost:7004')
