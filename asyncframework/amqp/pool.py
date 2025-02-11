# -*- coding: utf-8 -*-
from typing import Iterable, List
import aio_pika
from random import randint, seed
from asyncio import AbstractEventLoop
from asyncframework.log.log import get_logger
from asyncframework.net.pool import StaticPool


__all__ = ['AMQPPool']


class AMQPPool(StaticPool[aio_pika.abc.AbstractRobustConnection]):
    """AMQP connection pool class"""
    log = get_logger('AMQPPool')
    _hosts: List[str]

    def __init__(self, hosts: Iterable[str], pool_size=1):
        super().__init__(pool_size=pool_size)
        self._hosts = list(hosts)
        seed()

    async def create(self, ioloop: AbstractEventLoop, *args, **kwargs) -> aio_pika.abc.AbstractRobustConnection:
        if len(self._hosts) == 1:
            idx = 0
        else:
            idx = randint(0, len(self._hosts)-1)
        try:
            _connection = await aio_pika.connect_robust(self._hosts[idx], loop=ioloop)
            self.log.info(f'Connected to amqp-host {self._hosts[idx]}')
        except RuntimeError as e:
            self.log.error(f'Connection to host {self._hosts[idx]} failed ({e})')
            raise
        return _connection

    async def destroy(self, elem: aio_pika.Connection):
        self.log.info(f'Closing the connection {elem.url}')
        await elem.close()
