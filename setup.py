import unittest
from setuptools import setup
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    """
    Provide a Test runner to be used from setup.py to run unit tests
    """

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ""

    def run_tests(self):
        tests = unittest.TestLoader().discover("tests", pattern="test*.py")
        unittest.TextTestRunner(verbosity=2).run(tests)


setup(
    **{
        "name": "drain",
        "version": "0.0.1",
        "url": "http://github.com/codepr/drain",
        "license": "MIT",
        "author": "Andrea Giacomo Baldan",
        "author_email": "a.g.baldan@gmail.com",
        "description": ("A typed drain for all kind of dark matters"),
        "packages": ["drain"],
        "platforms": "any",
        "python_requires": ">=3.8",
        "cmdclass": {"test": PyTest},
    }
)
