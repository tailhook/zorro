import json
import abc
from urllib.parse import urlparse, parse_qsl
from collections import namedtuple

import zmq

from . import redis
from .zmq import send_data
from .util import cached_property


import logging
log = logging.getLogger(__name__)


def blob(val):
    """Little helper function which turns data into bytes by either encoding
    or json.dumping them

    In production it can be a bad practice because you can't send json-dumped
    string (it will be just encoded). But for our example its fun that we
    can send pre-serialized data.

    And you always send json objects anyway, don't you?
    """
    if isinstance(val, (dict, list)):
        return json.dumps(val).encode('utf-8')
    elif isinstance(val, str):
        return val.encode('utf-8')
    else:
        return val


def cid(val):
    if hasattr(val, 'cid'):
        return val.cid
    assert isinstance(val, (bytes, bytearray)), ("Connection must be bytes "
        "or object having cid property")
    return val


class JSONWebsockOutput(object):

    def __init__(self, channel):
        self._channel = channel
        # we use private interface for performance
        self._sock = channel._sock

    def subscribe(self, conn, topic):
        self._do_send((b'subscribe', cid(conn), topic))

    def unsubscribe(self, conn, topic):
        self._do_send((b'unsubscribe', cid(conn), topic))

    def drop(self, topic):
        self._do_send((b'drop', topic))

    def send(self, conn, data):
        self._do_send((b'send', cid(conn), blob(data)))

    def publish(self, topic, data):
        self._do_send((b'publish', topic, blob(data)))

    def set_cookie(self, conn, cookie):
        self._do_send((b'set_cookie', cid(conn), cookie))

    def add_output(self, conn, prefix, name):
        self._do_send((b'add_output', cid(conn), prefix, name))

    def del_output(self, conn, prefix, name):
        self._do_send((b'del_output', cid(conn), prefix, name))

    def disconnect(self, conn):
        self._do_send((b'disconnect', cid(conn)))

    def _do_send(self, data):
        log.debug("Sending to zerogw: %r", data)
        # we use private interface for performance
        send_data(self._sock, data)


class RequestMixin(object):
    # TODO(tailhook) implement cookie utility
    # TODO(tailhook) implement get arguments parsing utility
    # TODO(tailhook) implement post body arguments parsing utility

    @cached_property
    def parsed_uri(self):
        return urlparse(self.uri.decode('ascii'))


class InternalRedirect(Exception, metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def update_request(self, request):
        pass


class PathRewrite(InternalRedirect):

    def __init__(self, new_path):
        self.new_path = new_path

    def update_request(self, request):
        request.current_path = self.new_path


class WebException(Exception):
    pass


class Forbidden(WebException):

    def default_response(self):
        return (b'403 Forbidden',
                b'Content-Type\0text/html\0',
                b'<!DOCTYPE html>'
                b'<html>'
                    b'<head>'
                        b'<title>403 Forbidden</title>'
                    b'</head>'
                    b'<body>'
                    b'<h1>403 Forbidden</h1>'
                    b'</body>'
                b'</html>'
                )


class InternalError(WebException):

    def default_response(self):
        return (b'500 Internal Server Error',
                b'Content-Type\0text/html\0',
                b'<!DOCTYPE html>'
                b'<html>'
                    b'<head>'
                        b'<title>500 Internal Server Error</title>'
                    b'</head>'
                    b'<body>'
                    b'<h1>500 Internal Server Error</h1>'
                    b'</body>'
                b'</html>'
                )

class NotFound(WebException):

    def default_response(self):
        return (b'404 Not Found',
                b'Content-Type\0text/html\0',
                b'<!DOCTYPE html>'
                b'<html>'
                    b'<head>'
                        b'<title>404 Page Not Found</title>'
                    b'</head>'
                    b'<body>'
                    b'<h1>404 Page Not Found</h1>'
                    b'</body>'
                b'</html>'
                )


class UnknownConvention(Exception):
    pass


class HTTPService(object):

    def __init__(self, *input_fields):
        self.Request = type('Request',
            (namedtuple('RequestBase', input_fields), RequestMixin), {})

    def _safe_dispatch(self, request):
        while True:
            try:
                result = self.dispatch(request)
            except InternalRedirect as e:
                e.update_request(request)
                continue
            except Exception as e:
                if not isinstance(e, WebException):
                    log.exception("Can't process request %r", request)
                    e = InternalError(e)
                try:
                    return self._error_page(e)
                except Exception:
                    log.exception("Can't make error page for %r", e)
                    return e.default_response()
            else:
                return result

    def _error_page(self, e):
        return e.default_response()

    def __call__(self, *args):
        request = self.Request(*args)
        result = self._safe_dispatch(request)
        if hasattr(result, 'zerogw_response'):
            return result.zerogw_response()
        else:
            return result

    def _call_convention(self, target, request):
        if target is None:
            raise NotFound()
        if hasattr(target, 'dispatch'):
            return target.dispatch(request, cpath[next_idx])
        convention = getattr(target, '__zorro_convention__', None)
        if convention == 'splitpath':
            return target(*cpath[(idx or 10000)+1:].split('/'),
                **dict(parse_qsl(request.parsed_uri.query)))
        elif convention == 'simple':
            return target(request.parsed_uri.path,
                **dict(parse_qsl(request.parsed_uri.query)))
        elif convention is None:
            raise Forbidden()
        else:
            raise UnknownConvention(
                "Unknown convention {!r}".format(convention))


def public_splitpath(fun):
    fun.__zorro_convention__ = 'splitpath'
    return fun


def public(fun):
    fun.__zorro_convention__ = 'simple'
    return fun


class TreeService(HTTPService):

    def __init__(self, *input_fields):
        super().__init__(*input_fields)
        self.children = {}

    def dispatch(self, request, prefix='/'):
        cpath = getattr(request, 'current_path', None)
        if cpath is None:
            cpath = request.current_path = request.parsed_uri.path

        assert cpath.startswith(prefix)
        plen = len(prefix)
        idx = cpath.find('/', plen)
        if idx < 0:
            idx = None
            next_idx = None
        else:
            next_idx = idx + 1
        part = cpath[plen:idx]
        if part in self.children:
            target = self.children[part]
        elif not part:
            target = getattr(self, 'index', None)
        elif not part.startswith('_'):
            raise Forbidden()
        else:
            target = getattr(self, part, None)

        convention = getattr(target, '__zorro_convention__', None)
        if convention == 'splitpath':
            return target(*cpath[(idx or 10000)+1:].split('/'),
                **dict(parse_qsl(request.parsed_uri.query)))
        else:
            return self._call_convention(meth, request)


class ParamService(HTTPService):

    def __init__(self, *input_fields):
        super().__init__(*input_fields)

    def dispatch(self, request):
        methname = request.parsed_uri.params or 'default'
        target = getattr(self, methname, None)
        return self._call_convention(target, request)

class MethodService(HTTPService):

    def __init__(self, *input_fields):
        super().__init__(*input_fields)

    def dispatch(self, request):
        methname = request.method
        target = getattr(self, methname, None)
        return self._call_convention(target, request)
