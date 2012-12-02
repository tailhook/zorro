import json
import abc
import logging

from .web import _compile_signature

log = logging.getLogger(__name__)
_RESOURCE_METHOD = object()
_RPC_METHOD = object()
_RESOURCE = object()


class ChildNotFound(Exception):
    """Raised by resolve_local to notify that there is not such child"""


class NiceError(Exception):
    """The error which can be sent to user"""


class WebsocketCall(object):
    MESSAGE = 'message'
    CONNECT = 'connect'
    DICONNECT = 'disconnect'
    HEARTBEAT = 'heartbeat'
    SYNC = 'sync'

    def __init__(self, msgs):
        cid = msgs[0]
        kind = msgs[1]
        meth = getattr(self, '_init_' + kind.decode('ascii'), None)
        if meth:
            meth(cid, *msgs[2:])

    def _init_message(self, cid, body):
        self.cid = cid
        self.args = json.loads(body.decode('utf-8'))
        self.meth = self.args.pop(0)
        self.kwargs = self.args.pop(0)
        self.request_id = self.kwargs.pop('#', None)
        self.kind = self.MESSAGE

    def _init_msgfrom(self, cid, cookie, body):
        self.cid = cid
        self.marker = cookie
        self.args = json.loads(body.decode('utf-8'))
        self.meth = self.args.pop(0)
        self.kwargs = self.args.pop(0)
        self.request_id = self.kwargs.pop('#', None)
        self.kind = self.MESSAGE

    def _init_connect(self, cid):
        self.cid = cid
        self.kind = self.CONNECT

    def _init_disconnect(self, cid, cookie=None):
        self.cid = cid
        self.marker = cookie
        self.kind = self.DISCONNECT

    def _init_heartbeat(self, server_id):
        self.server_id = server_id
        self.kind = self.HEARTBEAT

    def _init_sync(self, server_id, *users):
        self.server_id = server_id
        it = iter(users)
        self.users = dict(zip(it, it))
        self.kind = self.SYNC


class Websockets(object):

    def __init__(self, *, resources=()):
        self.resources = resources

    def _resolve(self, request):
        for i in self.resources:
            res = i.resolver_class(request)
            try:
                return res.resolve(i)
            except ChildNotFound as e:
                continue

    def _safe_dispatch(self, request):
        while True:
            try:
                result = self._resolve(request)
            except NiceError as e:
                return ['_error', e.client_data()]
            except Exception as e:
                log.exception("Can't process request %r", request)
                return ['_internal_error']
            else:
                return ['_result', result]

    def __call__(self, *args):
        call = WebsocketCall(args)
        if call.kind is WebsocketCall.MESSAGE:
            result = self._safe_dispatch(call)
            # TODO(pc) send result
        else:
            meth = getattr(self, 'handle_' + WebsocketCall.kind, None)
            if meth is not None:
                meth()


class Resolver(metaclass=abc.ABCMeta):

    def __init__(self, request):
        self.request = request
        self.args = request.args[:]
        self.kwargs = request.kwargs

    def child_fallback(self):
        raise NotFound()

    def resolve(self, root):
        self.resource = node = root
        for name in self.request.meth.split('.'):
            try:
                node = node.resolve_local(name)
            except ChildNotFound:
                node = self.child_fallback()
            kind = getattr(node, '_zweb', None)
            if kind is _RPC_METHOD:
                return _dispatch_page(node, node.__self__, self)
            elif kind is _RESOURCE_METHOD:
                node, self.args = _dispatch_resource(node, node.__self__, self)
                self.resource = node
                if not isinstance(self, node.resolver_class):
                    newres =  node.resolver_class(self.request)
                    return newres.resolve(node)
            elif kind is _RESOURCE:
                pass
            else:
                raise ChildNotFound()  # probably impossible but ...
        raise ChildNotFound()


def _dispatch_resource(fun, self, resolver):  # r: --v
    deco = getattr(fun, '_zweb_deco', None)
    if deco is not None:
        return deco(self, resolver,
            partial(fun._zweb_deco_callee, self, resolver),
            *resolver.args, **resolver.kwargs)  # r: kwargs
    else:
        try:
            args, tail, kw = fun._zweb_sig(resolver,
                *resolver.args, **resolver.kwargs)  # r: kwargs
        except (TypeError, ValueError) as e:
            log.debug("Signature mismatch %r %r",
                resolver.args, resolver.kwargs,  # r: kwargs
                exc_info=e)  # debug
            raise NotFound()  # r: NotFound
        else:
            return fun(*args, **kw), tail


def _dispatch_page(fun, self, resolver):  # r: --v
    deco = getattr(fun, '_zweb_deco', None)
    if deco is not None:
        result = deco(self, resolver,
            partial(fun._zweb_deco_callee, self, resolver),
            *resolver.args, **resolver.kwargs)  # r: kwargs
    else:
        try:
            args, tail, kw = fun._zweb_sig(resolver,
                *resolver.args, **resolver.kwargs)  # r: kwargs
        except (TypeError, ValueError) as e:
            log.debug("Signature mismatch %r %r",
                resolver.args, resolver.kwargs,  # r: kwargs
                exc_info=e)  # debug
            raise ChildNotFound()  # r: NotFound
        else:
            result = fun(*args, **kw)
    return result


class Resource(object):  # r: different resolver
    resolver_class = Resolver
    _zweb = _RESOURCE

    def resolve_local(self, name):
        if not name.isidentifier() or name.startswith('_'):
            raise ChildNotFound()
        target = getattr(self, name, None)
        if target is None:
            raise ChildNotFound()
        kind = getattr(target, '_zweb', None)
        if kind is not None:
            return target
        raise ChildNotFound()


def resource(fun):  # r:ok
    """Decorator to denote a method which returns resource to be traversed"""
    fun._zweb = _RESOURCE_METHOD
    fun._zweb_sig = _compile_signature(fun, partial=True)
    return fun


def method(fun):  # r:ok
    """Decorator to denote a method which returns some result to the user"""
    fun._zweb = _RPC_METHOD
    fun._zweb_sig = _compile_signature(fun, partial=False)
    return fun
