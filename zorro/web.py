import abc
import json
import logging
import inspect
from http.cookies import SimpleCookie
from urllib.parse import urlparse, parse_qsl
from itertools import zip_longest
from functools import partial

from .util import cached_property, marker_object


log = logging.getLogger(__name__)

_LEAF_METHOD = marker_object('LEAF_METHOD')
_LEAF_WSOCK_METHOD = marker_object('LEAF_WSOCK_METHOD')
_LEAF_HTTP_METHOD = marker_object('LEAF_HTTP_METHOD')
_RESOURCE_METHOD = marker_object('RESOURCE_METHOD')
_RES_WSOCK_METHOD = marker_object('RES_WSOCK_METHOD')
_RES_HTTP_METHOD = marker_object('RES_HTTP_METHOD')
_RESOURCE = marker_object('RESOURCE')
_INTERRUPT = marker_object('INTERRUPT')
_FORM_CTYPE = b'application/x-www-form-urlencoded'


class LegacyMultiDict(object):
    """Utilitary class which wrap dict to make it suitable for old utilities
    like wtforms"""

    def __init__(self, pairs=None):
        self._dic = {}
        if not pairs is None:
            self.update(pairs)

    def update(self, pairs):
        for k, v in pairs:
            if k not in self._dic:
                self._dic[k] = [v]
            else:
                self._dic[k].append(v)

    def getlist(self, k):
        return list(self._dic[k])

    def __contains__(self, k):
        return k in self._dic

    def __iter__(self):
        for k in self._dic:
            yield k

    def __len__(self):
        return len(self._dic)


class Request(object):
    __slots__ = ('__dict__',
        # the excpected property names and names of respective zerogw items
        'uri',           # !Uri
        'content_type',  # !Header Content-Type
        'cookie',        # !Header Cookie
        'body',          # !PostBody
        )

    @cached_property
    def parsed_uri(self):
        return urlparse(self.uri.decode('ascii'))

    @cached_property
    def form_arguments(self):
        arguments = {}
        if hasattr(self, 'uri'):
            arguments.update(parse_qsl(self.parsed_uri.query))
        body = getattr(self, 'body', None)
        if body and getattr(self, 'content_type', None) == _FORM_CTYPE:
            arguments.update(parse_qsl(self.body.decode('ascii')))
        return arguments

    @cached_property
    def legacy_arguments(self):
        arguments = LegacyMultiDict()
        if hasattr(self, 'uri'):
            arguments.update(parse_qsl(self.parsed_uri.query))
        body = getattr(self, 'body', None)
        if body and getattr(self, 'content_type', None) == _FORM_CTYPE:
            arguments.update(parse_qsl(self.body.decode('ascii')))
        return arguments

    @cached_property
    def cookies(self):
        cobj = SimpleCookie(self.cookie.decode('ascii', 'ignore'))
        return dict((k, cobj[k].value) for k in cobj)

    @classmethod
    def create(cls, resolver):
        return resolver.request


class WebException(Exception):
    """Base for all exceptions which render error code (and page) to client"""

    @abc.abstractmethod
    def default_response(self):
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


class MethodNotAllowed(WebException):

    def default_response(self):
        return (b'405 Method Not Allowed',
                b'Content-Type\0text/html\0',
                b'<!DOCTYPE html>'
                b'<html>'
                    b'<head>'
                        b'<title>405 Method Not Allowed</title>'
                    b'</head>'
                    b'<body>'
                    b'<h1>405 Method Not Allowed</h1>'
                    b'</body>'
                b'</html>'
                )


class Redirect(WebException):

    def __init__(self, location, status_code, status_text):
        self.statusline = '{:d} {}'.format(status_code, status_text)
        self.location = location

    def location_header(self):
        return b'Location\0' + self.location.encode('ascii') + b'\0'

    def headers(self):
        return (b'Content-Type\0text/html\0'
                + self.location_header())

    def default_response(self):
        return (self.statusline, self.headers(),
                '<!DOCTYPE html>'
                '<html>'
                    '<head>'
                        '<title>{0.statusline}</title>'
                    '</head>'
                    '<body>'
                    '<h1>{0.statusline}</h1>'
                    '<a href="{0.location}">Follow</a>'
                    '</body>'
                '</html>'.format(self)
                )


class CompletionRedirect(Redirect):
    """Temporary redirect which sends code 303

    With :param:`cookie` set it is often used for login forms. Without
    parameter set it is used to provide "success" page for various web forms
    and other non-idempotent actions
    """

    def __init__(self, location, cookie=None, *,
        status_code=303, status_text="See Other"):
        super().__init__(location,
            status_code=status_code, status_text=status_text)
        self.cookie = cookie

    def headers(self):
        sup = super().headers()
        if self.cookie is not None:
            sup += self.cookie.output(header='Set-Cookie\0',
                                      sep='\0').encode('ascii') + b'\0'
        return sup


class ChildNotFound(Exception):
    """Raised by resolve_local to notify that there is not such child"""


class NiceError(Exception):
    """Error that is safe to present to user"""


class BaseResolver(metaclass=abc.ABCMeta):

    _LEAF_METHODS = {_LEAF_METHOD}
    _RES_METHODS = {_RESOURCE_METHOD}
    resolver_class_attr = 'resolver_class'
    default_method = None

    def __init__(self, request, parent=None):
        self.request = request
        self.parent = parent
        self.resource = None

    @abc.abstractmethod
    def __next__(self):
        pass

    def __iter__(self):
        return self

    def child_fallback(self):
        raise NotFound()

    @abc.abstractmethod
    def set_args(self, args):
        pass

    def resolve(self, root):
        self.resource = node = root
        for name in self:
            # assert name is not None, "Wrong name from {!r}".format(self)
            try:
                node = node.resolve_local(name)
            except ChildNotFound:
                node = self.child_fallback()
            kind = getattr(node, '_zweb', None)
            if kind in self._LEAF_METHODS:
                return _dispatch_leaf(node, node.__self__, self)
            elif kind in self._RES_METHODS:
                newnode, tail = _dispatch_resource(node, node.__self__, self)
                if newnode is _INTERRUPT:
                    return tail  # tail is actual result in this case
                self.set_args(tail)
                self.resource = newnode
                res_class = getattr(newnode, self.resolver_class_attr, None)
                if res_class is None:
                    raise RuntimeError("Value {!r} returned from"
                        " {!r} is not a resource".format(newnode, node))
                node = newnode
                if not isinstance(self, res_class):
                    newres = res_class(self.request, self)
                    return newres.resolve(node)
            elif kind is _RESOURCE:
                self.resource = node
            else:
                log.debug("Wrong kind %r", kind)
                raise NotFound()  # probably impossible but ...


        if self.default_method is not None:
            meth = getattr(node, self.default_method, None)
            if(meth is not None
                and getattr(meth, '_zweb', None) in self._LEAF_METHODS):
                return _dispatch_leaf(node.index, node, self)

        raise NotFound()


class PathResolver(BaseResolver):

    _LEAF_METHODS = {_LEAF_METHOD, _LEAF_HTTP_METHOD}
    _RES_METHODS = {_RESOURCE_METHOD, _RES_HTTP_METHOD}
    resolver_class_attr = 'http_resolver_class'
    default_method = 'index'

    def __init__(self, request, parent=None):
        super().__init__(request, parent)
        path = request.parsed_uri.path.strip('/')
        if path:
            self.args = path.split('/')
        else:
            self.args = []
        self.kwargs = dict(request.form_arguments)

    def __next__(self):
        try:
            return self.args.pop(0)
        except IndexError:
            raise StopIteration()

    def set_args(self, args):
        self.args = list(args)


class MethodResolver(BaseResolver):

    _LEAF_METHODS = {_LEAF_METHOD, _LEAF_HTTP_METHOD}
    _RES_METHODS = {_RESOURCE_METHOD, _RES_HTTP_METHOD}
    resolver_class_attr = 'http_resolver_class'
    default_method = None

    def __init__(self, request, parent=None):
        super().__init__(request, parent)
        self.args = parent.args
        self.kwargs = dict(request.form_arguments)

    def __next__(self):
        return self.request.method.decode('ascii').upper()

    def child_fallback(self):
        raise MethodNotAllowed()

    def set_args(self, args):
        self.args = args


class WebsockResolver(BaseResolver):

    _LEAF_METHODS = {_LEAF_METHOD, _LEAF_WSOCK_METHOD}
    _RES_METHODS = {_RESOURCE_METHOD, _RES_WSOCK_METHOD}
    resolver_class_attr = 'websock_resolver_class'
    default_method = 'default'

    def __init__(self, request, parent=None):
        super().__init__(request, parent)
        self.parts = list(request.meth.split('.'))
        self.args = request.args
        self.kwargs = dict(request.kwargs)

    def __next__(self):
        try:
            return self.parts.pop(0)
        except IndexError:
            raise StopIteration()

    def set_args(self, args):
        self.args = args


class InternalRedirect(Exception, metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def update_request(self, request):
        pass


class PathRewrite(InternalRedirect):

    def __init__(self, new_path):
        self.new_path = new_path.encode('ascii')

    def update_request(self, request):
        request.uri = self.new_path
        del request.parsed_uri


class ResourceInterface(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def resolve_local(self, name):
        """Returns child resource or method or raises ChildNotFound"""


@ResourceInterface.register
class Resource(object):
    http_resolver_class = PathResolver
    websock_resolver_class = WebsockResolver
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


@ResourceInterface.register
class DictResource(dict):

    http_resolver_class = PathResolver
    websock_resolver_class = WebsockResolver
    _zweb = _RESOURCE


    def resolve_local(self, name):
        try:
            return self[name]
        except KeyError:
            raise ChildNotFound()


class Site(object):

    def __init__(self, *, request_class, resources=()):
        self.request_class = request_class
        self.resources = resources

    def _resolve(self, request):
        for i in self.resources:
            res = i.http_resolver_class(request)
            try:
                return res.resolve(i)
            except NotFound as e:
                continue
        else:
            raise NotFound()

    def _safe_dispatch(self, request):
        while True:
            try:
                result = self._resolve(request)
            except InternalRedirect as e:
                e.update_request(request)
                continue
            except Exception as e:
                if not isinstance(e, WebException):
                    log.exception("Can't process request %r", request)
                    e = InternalError(e)
                try:
                    return self.error_page(e)
                except Exception:
                    log.exception("Can't make error page for %r", e)
                    return e.default_response()
            else:
                return result

    def error_page(self, e):
        return e.default_response()

    def __call__(self, *args):
        request = self.request_class(*args)
        result = self._safe_dispatch(request)
        if hasattr(result, 'zerogw_response'):
            return result.zerogw_response()
        else:
            return result


def _dispatch_resource(fun, self, resolver):
    preproc = getattr(fun, '_zweb_pre', ())
    result = None
    for prefun in preproc:
        result = prefun(self, resolver, *resolver.args, **resolver.kwargs)
        if result is not None:
            break
    if result is None:
        deco = getattr(fun, '_zweb_deco', None)
        if deco is not None:
            return deco(self, resolver,
                partial(fun._zweb_deco_callee, self, resolver),
                *resolver.args, **resolver.kwargs)
        else:
            try:
                args, tail, kw = fun._zweb_sig(resolver,
                    *resolver.args, **resolver.kwargs)
            except (TypeError, ValueError) as e:
                log.debug("Signature mismatch %r %r",
                    resolver.args, resolver.kwargs,
                    exc_info=e)  # debug
                raise NotFound()
            else:
                return fun(*args, **kw), tail
    else:
        for proc in fun._zweb_post:
            result = proc(self, resolver, result)
        return _INTERRUPT, result


class ReprHack(str):
    __slots__ = ()
    def __repr__(self):
        return str(self)


def _compile_signature(fun, partial):
    sig = inspect.signature(fun)
    fun_params = [
        inspect.Parameter('resolver',
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD)]
    args = []
    kwargs = []
    vars = {
        '__empty__': object(),
        }
    lines = []
    annotations = {}
    self = True
    varkw = False
    varpos = False

    for name, param in sig.parameters.items():
        ann = param.annotation
        if param.default is not inspect.Parameter.empty:
            vars[name + '_def'] = param.default
            defname = ReprHack('__empty__')
        else:
            defname = inspect.Parameter.empty
        if ann is not inspect.Parameter.empty:
            if isinstance(ann, type) and issubclass(ann, Sticker):
                lines.append('  {0} = {0}_create(resolver)'.format(name))
                vars[name + '_create'] = ann.create
            else:
                lines.append('  if {0} is __empty__:'.format(name))
                lines.append('    {0} = {0}_def'.format(name))
                lines.append('  else:')
                if isinstance(ann, type) and ann.__module__ == 'builtins':
                    lines.append('    {0} = {1}({0})'.format(
                        name, ann.__name__))
                    fun_params.append(param.replace(
                        default=defname))
                else:
                    lines.append('    {0} = {0}_type({0})'.format(name))
                    vars[name + '_type'] = ann
                    fun_params.append(param.replace(
                        annotation=ReprHack(name + '_type'),
                        default=defname))
        elif not self:
            fun_params.append(param.replace(default=defname))

        if param.kind == inspect.Parameter.VAR_KEYWORD:
            varkw = True
        elif param.kind == inspect.Parameter.VAR_POSITIONAL:
            varpos = name
        if param.kind == inspect.Parameter.KEYWORD_ONLY:
            kwargs.append('{0!r}: {0}'.format(name))
        elif not self:
            args.append(name)
        if self:
            self = False
    if not varpos and partial:
        for i, p in enumerate(fun_params):
            if p.kind == inspect.Parameter.KEYWORD_ONLY:
                fun_params.insert(i, inspect.Parameter('__tail__',
                    kind=inspect.Parameter.VAR_POSITIONAL))
                break
        else:
            fun_params.append(inspect.Parameter('__tail__',
                kind=inspect.Parameter.VAR_POSITIONAL))
    if not varkw:
        fun_params.append(inspect.Parameter('__kw__',
            kind=inspect.Parameter.VAR_KEYWORD))
    funsig = inspect.Signature(fun_params)
    lines.insert(0, 'def __sig__{}:'.format(funsig))
    if len(args) == 1:
        args = args[0] + ','
    else:
        args = ', '.join(args)
    if varpos:
        lines.append('  return ({}) + {}, (), {{{}}}'.format(
            args, varpos,  ', '.join(kwargs)))
    elif partial:
        lines.append('  return ({}), __tail__, {{{}}}'.format(
            args, ', '.join(kwargs)))
    else:
        lines.append('  return ({}), (), {{{}}}'.format(
            args, ', '.join(kwargs)))
    code = compile('\n'.join(lines), '__sig__', 'exec')
    exec(code, vars)
    return vars['__sig__']

def resource(fun):
    """Decorator to denote a method which returns resource to be traversed"""
    fun._zweb = _RESOURCE_METHOD
    fun._zweb_sig = _compile_signature(fun, partial=True)
    return fun


def http_resource(fun):
    """Decorator to denote a method which returns HTTP-only resource"""
    resource(fun)
    fun._zweb = _RES_HTTP_METHOD
    return fun


def websock_resource(fun):
    """Decorator to denote a method which returns Websocket-only resource"""
    resource(fun)
    fun._zweb = _RES_WSOCK_METHOD
    return fun


def _dispatch_leaf(fun, self, resolver):
    preproc = getattr(fun, '_zweb_pre', ())
    result = None
    for prefun in preproc:
        result = prefun(self, resolver, *resolver.args, **resolver.kwargs)
        if result is not None:
            break
    if result is None:
        deco = getattr(fun, '_zweb_deco', None)
        if deco is not None:
            result = deco(self, resolver,
                partial(fun._zweb_deco_callee, self, resolver),
                *resolver.args, **resolver.kwargs)
        else:
            try:
                args, tail, kw = fun._zweb_sig(resolver,
                    *resolver.args, **resolver.kwargs)
            except (TypeError, ValueError) as e:
                log.debug("Signature mismatch %r %r",
                    resolver.args, resolver.kwargs,
                    exc_info=e)  # debug
                raise NotFound()
            else:
                result = fun(*args, **kw)
    for proc in fun._zweb_post:
        result = proc(self, resolver, result)
    return result


def endpoint(fun):
    """Decorator to denote a method which returns some result to the user"""
    if not hasattr(fun, '_zweb_post'):
        fun._zweb_post = []
    fun._zweb = _LEAF_METHOD
    fun._zweb_sig = _compile_signature(fun, partial=False)
    return fun


def page(fun):
    """Decorator to denote a method which works only for http"""
    endpoint(fun)
    fun._zweb = _LEAF_HTTP_METHOD
    return fun


def method(fun):
    """Decorator to denote a method which works only for websockets"""
    endpoint(fun)
    fun._zweb = _LEAF_WSOCK_METHOD
    return fun


def postprocessor(fun):
    """A decorator that accepts method's output and processes it

    Works only on leaf nodes. Typical use cases are:

    * turn dict of variables into a JSON
    * render a template from the dict.
    """
    if not hasattr(fun, '_zweb_post'):
        fun._zweb_post = []
    def wrapper(proc):
        fun._zweb_post.append(proc)
        return fun
    return wrapper


def preprocessor(fun):
    """A decorators that runs before request

    When preprocessor returns None, request is processed as always. If
    preprocessor return not None it's return value treated as return value
    of a leaf node (even if it's resource) including executing all
    postprocessors.

    Works both on resources and leaf nodes. Typical use casees are:

    * access checking
    * caching
    """
    if not hasattr(fun, '_zweb_pre'):
        fun._zweb_pre = []
    def wrapper(proc):
        fun._zweb_pre.append(proc)
        return fun
    return wrapper


def decorator(fun):
    def wrapper(parser):
        olddec = getattr(fun, '_zweb_deco', None)
        oldcallee = getattr(fun, '_zweb_deco_callee', None)
        if olddec is None:
            def callee(self, resolver, *args, **kw):
                try:
                    args, tail, kw = fun._zweb_sig(resolver, *args, **kw)
                except (TypeError, ValueError) as e:
                    log.debug("Signature mismatch %r %r", args, kw,
                        exc_info=e)  # debug
                    raise NotFound()
                else:
                    return fun(self, *args, **kw)
        else:
            def callee(self, resolver, *args, **kw):
                return olddec(self, resolver,
                    partial(oldcallee, self, resolver),
                    *args, **kw)

        fun._zweb_deco = parser
        fun._zweb_deco_callee = callee
        return fun
    return wrapper


class Sticker(metaclass=abc.ABCMeta):
    """
    An object which is automatically put into arguments in the view if
    specified in annotation
    """
    __superseded = {}

    @classmethod
    @abc.abstractmethod
    def create(cls, resolver):
        """Creates an object of this class based on resolver"""

    @classmethod
    def supersede(cls, sub):
        oldsup = cls.__superseeded.get(cls, None)
        if oldsup is not None:
            if issubclass(sub, oldsup):
                pass  # just supersede it again
            elif issubclass(oldsup, sub):
                return  # already superseeded by more specific subclass
            else:
                raise RuntimeError("{!r} is already superseeded by {!r}"
                    .format(cls, oldsup))

        super().register(sub)
        cls.__superseded[cls] = sub


class WebsockCall(object):
    MESSAGE = 'message'
    CONNECT = 'connect'
    DISCONNECT = 'disconnect'
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

    @classmethod
    def create(cls, resolver):
        return resolver.request


class Websockets(object):

    def __init__(self, *, resources=(), output=None):
        self.resources = resources
        self.output = output

    def _resolve(self, request):
        for i in self.resources:
            res = i.websock_resolver_class(request)
            try:
                return res.resolve(i)
            except NotFound as e:
                continue
        raise NotFound()

    def _safe_dispatch(self, request):
        try:
            result = self._resolve(request)
        except NiceError as e:
            return ['_error', e.client_data()]
        except NotFound as e:
            return ['_not_found', request.meth]
        except Exception as e:
            log.exception("Can't process request %r", request)
            return ['_internal_error']
        else:
            return ['_result', result]

    def __call__(self, *args):
        call = WebsockCall(args)
        if call.kind is WebsockCall.MESSAGE:
            result = self._safe_dispatch(call)
            if call.request_id is not None:
                result.insert(1, call.request_id)
                self.output.send(call, json.dumps(result))
        else:
            meth = getattr(self, 'handle_' + call.kind, None)
            if meth is not None:
                meth(call)


Sticker.register(Request)
Sticker.register(WebsockCall)
