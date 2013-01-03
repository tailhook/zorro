"""Zorro's dependency injection framework

Usage
=====

Declare a class' dependencies::

    @has_dependencies
    class Hello(object):

        world = dependency(World, 'universe')

You must decorate class with ``@has_dependencies``. All dependencies has a
type(class), which is ``World`` in this case, and a name, which is ``universe``
in this case. This is for having multiple similar dependencies.

.. note::

    Name of dependency is unique among all services in single dependency
    injector and not tied to particular type. This is done to easier support
    subclassing of dependencies (and you can also register subclass with abc
    instead of subclassing directly)

Then at some initialisation code you create dependency injector, and set
apropriate services::

    inj = DependencyInjector()
    inj['universe'] = World()

Now you can create ``Hello`` instances and inject dependencies to them::

    hello = inj.inject(Hello())
    assert hello.world is inj['universe']

And you can propagate dependencies starting from existing instances::

    h2 = di(hello).inject(Hello())
    assert h2.world is hello.world

"""

def has_dependencies(cls):
    """Class decorator that declares dependencies"""
    deps = {}
    for i in dir(cls):
        val = getattr(cls, i)
        if isinstance(val, dependency):
            deps[i] = val
    cls.__zorro_depends__ = deps
    return cls


class dependency:
    """Property that represents single dependency"""

    def __init__(self, typ, name):
        self.type = typ
        self.name = name

    def __get__(self, inst, owner):
        if inst is None:
            return self
        raise RuntimeError("Dependency {!r} is not configured".format(self))

    def __repr__(self):
        return "<dependency {!r}:{!r}>".format(self.type, self.name)


class DependencyInjector(object):
    """Main dependency injector class

    Set all dependencies with::

        >>> inj = DependencyInjector()
        >>> inj['name'] = value
        >>> obj = inj.inject(DependentClass())

    Propagate dependencies with::

        >>> di(obj).inject(NewObject())
    """

    def __init__(self):
        self._provides = {}

    def inject(self, inst, **renames):
        """Injects dependencies and propagates dependency injector"""
        if renames:
            di = self.clone(**renames)
        else:
            di = self
        pro = di._provides
        inst.__zorro_di__ = di
        deps = getattr(inst, '__zorro_depends__', None)
        if deps:
            for attr, dep in deps.items():
                val = pro[dep.name]
                if not isinstance(val, dep.type):
                    raise RuntimeError("Wrong provider for {!r}".format(val))
                setattr(inst, attr, val)
        meth = getattr(inst, '__zorro_di_done__', None)
        if meth is not None:
            meth()
        return inst

    def inject_subset(self, inst, *names, **renames):
        """Injects only specified dependencies, and does not propagate others
        """
        # TODO(tailhook) may be optimize if names == _provides.keys()
        di = self.__class__()
        mypro = self._provides
        pro = di._provides
        for name in names:
            pro[name] = mypro[name]
        for name, alias in renames.items():
            pro[name] = mypro[alias]
        inst.__zorro_di__ = di
        deps = getattr(inst, '__zorro_depends__', None)
        if deps:
            for attr, dep in deps.items():
                if dep.name not in pro:
                    continue
                val = pro[dep.name]
                if not isinstance(val, dep.type):
                    raise RuntimeError("Wrong provider for {!r}".format(val))
                setattr(inst, attr, val)
        meth = getattr(inst, '__zorro_di_done__', None)
        if meth is not None:
            meth()
        return inst

    def inject_restricted(self, inst):
        """Injects dependencies, and propagates only this instance's
        dependencies
        """
        # TODO(tailhook) may be optimize if names == _provides.keys()
        di = self.__class__()
        mypro = self._provides
        pro = di._provides
        for name in names:
            pro[name] = mypro[name]
        for name, alias in renames.items():
            pro[name] = mypro[alias]
        inst.__zorro_di__ = di
        deps = getattr(inst, '__zorro_depends__', None)
        if deps:
            for attr, dep in deps.items():
                val = pro[dep.name]
                if not isinstance(val, dep.type):
                    raise RuntimeError("Wrong provider for {!r}".format(val))
                setattr(inst, attr, val)
        meth = getattr(inst, '__zorro_di_done__', None)
        if meth is not None:
            meth()
        return inst

    def __setitem__(self, name, value):
        if name in self._provides:
            raise RuntimeError("Two providers for {!r}".format(name))
        self._provides[name] = value

    def __getitem__(self, name):
        return self._provides[name]

    def __contains__(self, name):
        return name in self._provides

    def clone(self, **renames):
        di = self.__class__()
        mypro = self._provides
        pro = di._provides
        pro.update(mypro)
        for name, alias in renames.items():
            pro[name] = mypro[alias]
        return di


def dependencies(cls):
    """Returns dict of dependencies of a class declared with
    ``@has_dependencies``
    """
    return getattr(cls, '__zorro_depends__', {})


def di(obj):
    """Returns dependency injector used to construct class

    This instance is useful to propagate dependencies
    """
    try:
        return obj.__zorro_di__
    except AttributeError:
        raise RuntimeError("No dependency injector found")
