from .util import cached_property


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


class Resource(object):


    def resolve_local(self, name):
        if not name.isidentifier() or name.startswith('_'):
            raise NotFound()
        target = getattr(self, name, None)
        if target is None:
            raise NotFound()
        if getattr(target, '_public_decorator', None):
            return target
        raise NotFound()


class Site(object):

    def __init__(self, *, request_class, resources=()):
        self.request_class = request_class
        self.resources = resources

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


def public(fun):
    fun._public_decorator = lambda self, *a, **kw: fun(self)
    return fun
