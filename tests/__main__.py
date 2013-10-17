
import unittest
import pkgutil
import tests
import logging


logging.basicConfig(filename='tests.log', filemode='w', level=logging.DEBUG)
suite = unittest.TestSuite()
loader = unittest.TestLoader()
for _loader, name, _ispkg in pkgutil.iter_modules(tests.__path__):
    if not name.startswith('__'):
        suite.addTest(loader.loadTestsFromName('tests.'+name))
unittest.TextTestRunner(verbosity=2).run(suite)
