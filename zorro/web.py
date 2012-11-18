import abc
import logging
import inspect
from urllib.parse import urlparse, parse_qsl
from itertools import zip_longest
from collections import OrderedDict
from functools import partial

from .util import cached_property


log = logging.getLogger(__name__)
sentinel = object()

_PAGE_METHOD = object()
_RESOURCE_METHOD = object()
_RESOURCE = object()


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
            sup += ('Set-Cookie\0'
                    + self.cookie.output(header='').encode('ascii') + '\0')
        return sup


class ChildNotFound(Exception):
    """Raised by resolve_local to notify that there is not such child"""


class BaseResolver(metaclass=abc.ABCMeta):

    def __init__(self, request):
        self.request = request

    @abc.abstractmethod
    def __next__(self):
        pass

    def __iter__(self):
        return self

    def child_fallback(self):
        raise NotFound()

    def resolve(self, root):
        node = root
        for name in self:
            try:
                node = node.resolve_local(name)
            except ChildNotFound:
                node = self.child_fallback()
            kind = getattr(node, '_zweb', None)
            if kind is _PAGE_METHOD:
                return _dispatch_page(node, node.__self__, self)
            elif kind is _RESOURCE_METHOD:
                node, self.args = _dispatch_resource(node, node.__self__, self)
                if not isinstance(self, node.resolver_class):
                    newres =  node.resolver_class(self.request)
                    return newres.resolve(node)
            elif kind is _RESOURCE:
                pass
            else:
                raise NotFound()  # probably impossible but ...
        if(hasattr(node, 'index')
            and getattr(node.index, '_zweb', None) is _PAGE_METHOD):
            return _dispatch_page(node.index, node, self)
        raise NotFound()


class PathResolver(BaseResolver):

    def __init__(self, request):
        super().__init__(request)
        path = request.parsed_uri.path.strip('/')
        if path:
            self.args = path.split('/')
        else:
            self.args = ()

    def __next__(self):
        if not self.args:
            raise StopIteration()
        name, *args = self.args
        self.args = args
        return name


class MethodResolver(BaseResolver):

    def __init__(self, request):
        super().__init__(request)
        self.args = ()

    def __next__(self):
        return self.request.method.decode('ascii').upper()

    def child_fallback(self):
        raise MethodNotAllowed()


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


class Site(object):

    def __init__(self, *, request_class, resources=()):
        self.request_class = request_class
        self.resources = resources

    def _resolve(self, request):
        for i in self.resources:
            res = i.resolver_class(request)
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
    deco = getattr(fun, '_zweb_deco', None)
    if deco is not None:
        return deco(self, resolver,
            partial(fun._zweb_deco_callee, self, resolver),
            *resolver.args, **resolver.request.form_arguments)
    else:
        try:
            args, tail, kw = fun._zweb_sig(resolver,
                *resolver.args, **resolver.request.form_arguments)
        except (TypeError, ValueError) as e:
            log.debug("Signature mismatch %r %r",
                resolver.args, resolver.request.form_arguments,
                exc_info=e)  # debug
            raise NotFound()
        else:
            return fun(*args, **kw), tail


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


def _dispatch_page(fun, self, resolver):
    deco = getattr(fun, '_zweb_deco', None)
    if deco is not None:
        result = deco(self, resolver,
            partial(fun._zweb_deco_callee, self, resolver),
            *resolver.args, **resolver.request.form_arguments)
    else:
        try:
            args, tail, kw = fun._zweb_sig(resolver,
                *resolver.args, **resolver.request.form_arguments)
        except (TypeError, ValueError) as e:
            log.debug("Signature mismatch %r %r",
                resolver.args, resolver.request.form_arguments,
                exc_info=e)  # debug
            raise NotFound()
        else:
            result = fun(*args, **kw)
    for proc in fun._zweb_post:
        result = proc(self, resolver, result)
    return result


def page(fun):
    """Decorator to denote a method which returns some result to the user"""
    if not hasattr(fun, '_zweb_post'):
        fun._zweb_post = []
    fun._zweb = _PAGE_METHOD
    fun._zweb_sig = _compile_signature(fun, partial=False)
    return fun


def postprocessor(fun):
    if not hasattr(fun, '_zweb_post'):
        fun._zweb_post = []
    def wrapper(proc):
        fun._zweb_post.append(proc)
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

