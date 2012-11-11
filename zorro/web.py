import abc
import logging
from inspect import signature, Parameter
from urllib.parse import urlparse, parse_qsl

from .util import cached_property

log = logging.getLogger(__name__)


class LegacyMultiDict(object):
    """Utilitary class which wrap dict to make it suitable for old utilities
    like wtforms"""

    def __init__(self, dic):
        self._dic = dic

    def getlist(self, k):
        return [self._dic[k]]

    def __contains__(self, k):
        return k in self._dic

    def __iter__(self):
        for k in self._dic:
            yield k


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
        if body and getattr(self, 'content_type', None) == FORM_CTYPE:
            arguments.update(parse_qsl(self.body.decode('ascii')))
        return arguments

    @cached_property
    def legacy_arguments(self):
        return LegacyMultiDict(self.form_arguments)

    @cached_property
    def cookies(self):
        cobj = SimpleCookie(self.cookie.decode('ascii', 'ignore'))
        return dict((k, cobj[k].value) for k in cobj)


class WebException(Exception):
    """Base for all exceptions which render error code (and page) to client"""


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


class PathResolver(object):

    def __init__(self, request):
        self.request = request
        self.parts = request.parsed_uri.path.strip('/').split('/')
        self.path_iter = self.positional_args = iter(self.parts)
        self.keyword_args = self.request.form_arguments.copy()

    def resolve(self, root):
        node = root
        for i in self.path_iter:
            node = node.resolve_local(i)
            if hasattr(node, '_zweb_page'):
                return node._zweb_page(node.__self__, self)
            elif hasattr(node, '_zweb_resource'):
                node = node._zweb_resource(node.__self__, self)
        if hasattr(node, 'index') and hasattr(node.index, '_zweb_page'):
            return node.index._zweb_page(node, self)
        return node()


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


class Resource(object):
    resolver_class = PathResolver


    def resolve_local(self, name):
        if not name.isidentifier() or name.startswith('_'):
            raise NotFound()
        target = getattr(self, name, None)
        if target is None:
            raise NotFound()
        if getattr(target, '_zweb_page', None):
            return target
        if getattr(target, '_zweb_resource', None):
            return target
        raise NotFound()


class Site(object):

    def __init__(self, *, request_class, resources=()):
        self.request_class = request_class
        self.resources = resources

    def _resolve(self, request):
        for i in self.resources:
            res = i.resolver_class(request)
            try:
                return res.resolve(i)
            except NotFound:
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

def resource(fun):
    """Decorator to denote a method which returns resource to be traversed"""
    def decor(self, dispatcher):
        sig = signature(fun)
        pos = []
        kw = {}
        for i, (name, param) in enumerate(sig.parameters.items()):
            if i == 0:
                continue  # always self
            if param.kind == Parameter.POSITIONAL_ONLY:
                try:
                    pos.append(next(dispatcher.positional_args))
                except StopIteration:
                    if param.default is Parameter.empty:
                        log.info("No positional argument %r for %r", name, fun)
                        raise NotFound()
            elif param.kind == Parameter.POSITIONAL_OR_KEYWORD:
                try:
                    pos.append(next(dispatcher.positional_args))
                except StopIteration:
                    try:
                        kw[name] = dispatcher.keyword_args.pop(name)
                    except KeyError:
                        if param.default is Parameter.empty:
                            log.info("No argument %r for %r", name, fun)
                            raise NotFound()
            elif param.kind == Parameter.VAR_POSITIONAL:
                pos.extend(dispatcher.positional_args)
            elif param.kind == Parameter.KEYWORD_ONLY:
                try:
                    kw[name] = dispatcher.keyword_args.pop(name)
                except KeyError:
                    if param.default is Parameter.empty:
                        log.info("No keyword argument %r for %r", name, fun)
                        raise NotFound()
            elif param.kind == Parameter.VAR_KEYWORD:
                kw.update(dispatcher.keyword_args)
                dispatcher.keyword_args.clear()
            else:
                raise NotImplementedError(param.kind)
        try:
            bound = sig.bind(self, *pos, **kw)
        except TypeError:
            raise NotFound()
        for k, v in bound.arguments.items():
            an = sig.parameters[k].annotation
            if an is not Parameter.empty:
                try:
                    bound.arguments[k] = an(v)
                except (ValueError, TypeError):
                    raise NotFound()
        return fun(*bound.args, **bound.kwargs)
    fun._zweb_resource = decor
    return fun


def page(fun):
    """Decorator to denote a method which returns some result to the user"""
    def decor(self, dispatcher):
        sig = signature(fun)
        try:
            bound = sig.bind(self,
                *dispatcher.positional_args,
                **dispatcher.keyword_args)
        except TypeError as e:
            raise NotFound()
        for k, v in bound.arguments.items():
            an = sig.parameters[k].annotation
            if an is not Parameter.empty:
                try:
                    bound.arguments[k] = an(v)
                except (ValueError, TypeError):
                    raise NotFound()
        return fun(*bound.args, **bound.kwargs)
    fun._zweb_page = decor
    return fun


def postprocessor(fun):
    if not hasattr(fun, '_post_processors'):
        fun._post_processors = []
    def wrapper(proc):
        fun._post_processors.append(proc)
        return wrapper
    return fun

"""
def arguments(fun):
    lines = []
    call = []
    params = signature(fun).parameters
    for name, param in params.items()
        if param.kind = Parameter.VAR_KEYWORD:
            lines.append('{} = dict(resolver.keyword_args)')
            break
    for name, param in params.items():
        if ann.kind == VAR_POSITIONAL:
            lines.append('{} = list(resolver.positional_args)')
            call.append('*' + name)
            continue
        elif ann.kind == VAR_KEYWORD:
            # processed earlier
            call.append('**' + name)
            continue
        ann = param.annotation
        if ann is Parameter.empty
            raise TypeError("No annotation for {!r}".format(name))
        if ann.kind == Parameter.POSITIONAL_ONLY:
            lines.append('')
    fun._public_decorator = decor
    return fun
"""
