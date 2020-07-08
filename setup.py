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
        "name": "streamario",
        "version": "0.0.1",
        "url": "http://github.com/codepr/streamario",
        "license": "MIT",
        "author": "Andrea Giacomo Baldan",
        "author_email": "a.g.baldan@gmail.com",
        "description": ("A stream for Mario"),
        "packages": ["streamario"],
        "platforms": "any",
        "python_requires": ">=3.8",
        "cmdclass": {"test": PyTest},
    }
)
