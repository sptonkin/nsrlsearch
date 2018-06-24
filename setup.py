from setuptools import setup

import re
import platform
import os
import sys


def load_version(filename='nsrlsearch/version.py'):
    """Parse a __version__ number from a source file"""
    with open(filename) as source:
        text = source.read()
        for line in text.splitlines():
            match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", line)
            if match:
                return match.group(1)

    msg = "Unable to find version number in {}".format(filename)
    raise RuntimeError(msg)


setup(
    name="nsrlsearch",
    version=load_version(),
    packages=['nsrlsearch'],
    package_data={'nsrlsearch': ['nsrlsearch/default_config.cfg']},
    include_package_data=True,
    zip_safe=False,
    author="Stephen Tonkin",
    author_email="sptonkin@outlook.com",
    url="https://github.com/sptonkin/nsrlsearch",
    description="Ingest and query NIST NSRL CSV files in Elasticsearch.",
    long_description=open('README.rst').read(),
    license="Apache Software License",
    install_requires=["elasticsearch>=5.4,<6",
                      "begins>=0.9",
                      "bottle>=0.12",
                      "requests>=2.0",
                      "gunicorn>=19.7",
                      "six",
                      "isoparser>=0.3",
                      "backports.csv"],
    platforms=['linux'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Other Audience',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Security',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    test_suite="tests",
    tests_require=["WebTest>=2.0"],
    entry_points={
        'console_scripts': {
            'nsrlsearch = nsrlsearch.cli:main.start'
        }
    }
)
