from setuptools import setup, find_packages

setup(
    name="drain",
    version="0.0.1",
    url="http://github.com/codepr/drain",
    license="MIT",
    author="Andrea Giacomo Baldan",
    author_email="a.g.baldan@gmail.com",
    description="A sewer of drains. A typed stream app for all kind of dark matters",
    packages=find_packages(exclude=["tests"]),
    platforms="any",
    python_requires=">=3.7",
    test_suite="tests",
)
