#!/usr/bin/env python
# setup.py
# -*- coding: utf-8 -*-
# pylint: disable=invalid-name

"""Install script for framework-amqp.py"""

# Copyright (C) 2019-2023:
# Vladimir Berezenko : qmaster 2thousand at gmail com

# This software is licensed under the terms of the MIT license.

import os
import sys

from setuptools import setup, find_packages

if sys.version_info.major < 3:
    print('This code is intended to use with python 3 only')
    sys.exit(1)

__here__ = os.path.abspath(os.path.dirname(__file__))

NAME = 'asyncframework-amqp'
PACKAGES = find_packages(exclude=['tests'])
DESCRIPTION = 'Async framework amqp addon.'
URL = 'https://github.com/Q-Master/framework-amqp.py'

REQUIRES = """
    asyncframework
    aio_pika
"""
LINKS = """
    git+https://github.com/Q-Master/framework.py.git#egg=asyncframework
"""

VERSION = '0.1.0'


LONG_DESCRIPTION = """***asyncframework-amqp*** - async framework amqp addon
===

`asyncframework` is my own view of easy async python applications and serivces development.

The main idea of this project is to give easiness of creation of various async services without
the needs to go deep into creating and managing multiprocess services, network interconnections
and so on.

"""

CLASSIFIERS = [
    # Details at http://pypi.python.org/pypi?:action=list_classifiers
    'Development Status :: 5',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
    'Programming Language :: Python :: 3.11',
    'Operating System :: OS Independent',
    'Topic :: Software Development :: Libraries',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

AUTHOR = 'Vladimir Berezenko'

AUTHOR_EMAIL = 'qmaster2000@gmail.com'

KEYWORDS = "async, application, developer, service".split(', ')

project = dict(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    packages=PACKAGES,
    install_requires=[i.strip() for i in REQUIRES.splitlines() if i.strip()],
    dependency_links=[i.strip() for i in LINKS.splitlines() if i.strip()],
    python_requires='>=3.7',
    classifiers=CLASSIFIERS,
    keywords=KEYWORDS,
    license='MIT',
)

if __name__ == '__main__':
    setup(**project)