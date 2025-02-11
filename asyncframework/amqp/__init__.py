# -*- coding: utf-8 -*-

__version__ = '0.1.21'

__title__ = 'asyncframework-amqp'
__description__ = 'Async framework amqp addon.'
__url__ = 'https://github.com/Q-Master/framework-amqp.py'
__uri__ = __url__
__doc__ = f"{__description__} <{__uri__}>"

__author__ = 'Vladimir Berezenko'
__email__ = 'qmaster2000@gmail.com'

__license__ = 'MIT'
__copyright__ = 'Copyright 2019-2023 Vladimir Berezenko'


from .connection import *
from .pool import *
from .event_dispatcher import *
