# -*- coding: utf-8 -*-
import aio_pika
from typing import Optional, Union
from asyncio import AbstractEventLoop
from asyncframework.net.connection_base import ConnectionBase
from asyncframework.log.log import get_logger
from .pool import AMQPPool


class AMQPConnection(ConnectionBase):
    log = get_logger('AMQPConnection')
    __exchange: aio_pika.abc.AbstractExchange
    __queue: Optional[aio_pika.abc.AbstractQueue] = None
    __connection_pool: AMQPPool
    __receive_routing_key: str
    __send_routing_key: str
    __exchange_key: str
    __exchange_durable: bool
    __exchange_declare: bool
    __exchange_type: aio_pika.ExchangeType
    __queue_name: Optional[str]
    __queue_durable: bool
    __queue_autodelete: bool
    __queue_exclusive: bool
    __prefetch_count: Optional[int]

    def __init__(
            self,
            pool: AMQPPool,
            exchange_key: str,
            receive_routing_key: str,
            send_routing_key: str,
            *args,
            queue_name: Optional[str] = None,
            exchange_durable: bool = True,
            exchange_type: aio_pika.ExchangeType = aio_pika.ExchangeType.DIRECT,
            exchange_declare: bool = True,
            queue_durable: bool = True,
            queue_autodelete: bool = False,
            queue_exclusive: bool = False,
            prefetch_count: Optional[int] = None,
            **kwargs
    ) -> None:
        """Constructor

        Args:
            pool (AMQPPool): the connection pool
            exchange_key (str): the exchange key
            receive_routing_key (str): the routing key which this connection will listen
            send_routing_key (str): the routing key this connection writes to
            queue_name (Optional[str], optional): the queue name for connection. Defaults to None.
            exchange_durable (bool, optional): if exchange should be durable. Defaults to True.
            exchange_type (aio_pika.ExchangeType, optional): the exchange type. Defaults to aio_pika.ExchangeType.DIRECT.
            queue_durable (bool, optional): if queue should be durable. Defaults to True.
            queue_autodelete (bool, optional): if queue should be deleted after all consumers disconnected. Defaults to False.
            prefetch_count (Optional[int], optional): the prefetch count. Defaults to None.
        """
        super().__init__(*args, **kwargs)
        self.__connection_pool = pool
        self.__receive_routing_key = receive_routing_key
        self.__send_routing_key: str = send_routing_key
        self.__exchange_key = exchange_key
        self.__exchange_durable = exchange_durable
        self.__exchange_type = exchange_type
        self.__exchange_declare = exchange_declare
        self.__queue_name = queue_name
        self.__queue_durable = queue_durable
        self.__queue_autodelete = queue_autodelete
        self.__queue_exclusive = queue_exclusive
        self.__prefetch_count = prefetch_count

    async def connect(self, ioloop: Optional[AbstractEventLoop], *args, **kwargs):
        """Connect to the AMQP

        Args:
            ioloop (Optional[AbstractEventLoop]): event loop
        """
        super().connect(*args, **kwargs)
        try:
            _connection: aio_pika.Connection = await self.__connection_pool.acquire(loop=ioloop)
            channel = await _connection.channel()
            if self.__prefetch_count:
                await channel.set_qos(prefetch_count=self.__prefetch_count)
            channel.return_callbacks.add(self._amqp_message_returned)
            if self.__exchange_declare:
                self.log.debug(f'Declaring exchange "{self.__exchange_key}" with type {str(self.__exchange_type)}')
                self.__exchange = await channel.declare_exchange(
                    self.__exchange_key,
                    type=self.__exchange_type,
                    durable=self.__exchange_durable,
                )
            else:
                self.__exchange = await channel.get_exchange(self.__exchange_key)
            self.log.debug(f'Declaring queue "{self.__queue_name}"')
            self.__queue = await channel.declare_queue(
                self.__queue_name, durable=self.__queue_durable, auto_delete=self.__queue_autodelete, exclusive=self.__queue_exclusive
            )
            if not self.__receive_routing_key:
                self.__receive_routing_key = self.__queue.name
            self.log.debug(f'Binding queue "{self.__queue.name}" to exchange "{self.__exchange_key}" with routing key {self.__receive_routing_key}')
            await self.__queue.bind(self.__exchange, routing_key=self.__receive_routing_key)
            await self.__queue.consume(self._amqp_message_received)
        except RuntimeError as e:
            self.log.error(f'Failed to connect(%s)', e)
        if not _connection or _connection.is_closed:
            exc = RuntimeError('Connection cancelled')
            await self.on_connection_lost(exc)
            raise exc
        await self.on_connection_made(_connection.transport)
        self.log.info('Connected OK')

    async def close(self):
        if self.__queue:
            await self.__queue.unbind(self.__exchange, routing_key=self.__receive_routing_key)
        super().close()
        self.log.info('Connection closed')

    async def _amqp_message_received(self, msg: aio_pika.abc.AbstractIncomingMessage):
        async with msg.process():
            body = msg.body.decode('utf-8')
            self.log.debug(f'Got envelope reply_to: {msg.reply_to}, body: {body}')
            await self.on_message_received(body, routing_key=msg.reply_to)

    async def _amqp_message_returned(self, msg: aio_pika.message.ReturnedMessage):
        self.log.debug(f'Message returned "{msg}"')
        async with msg.process():
            body = msg.body.decode('utf-8')
            await self.on_message_returned(body)

    async def write(self, msg: str, *args, routing_key: Optional[str] = None, **kwargs):
        self.log.debug(f'Sending envelope with routing_key "{routing_key}", msg: "{msg}"')
        routing_key = routing_key or self.__send_routing_key
        if not routing_key:
            raise RuntimeError(f'Send routing key not set')
        await self.__exchange.publish(
            aio_pika.Message(
                msg.encode('utf-8'),
                content_encoding='utf-8',
                reply_to=self.__receive_routing_key,
                *args,
                **kwargs
            ),
            routing_key=routing_key,
        )
