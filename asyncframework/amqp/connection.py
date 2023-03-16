# -*- coding: utf-8 -*-
import aio_pika
from typing import Optional
from asyncio import AbstractEventLoop
from asyncframework.net.connection_base import ConnectionBase
from asyncframework.log.log import get_logger
from .pool import AMQPPool


class AMQPConnection(ConnectionBase):
    log = get_logger('AMQPConnection')
    _exchange: Optional[aio_pika.abc.AbstractExchange] = None
    _queue: Optional[aio_pika.abc.AbstractQueue] = None
    _connection_pool: AMQPPool
    _receive_routing_key: str
    _send_routing_key: str
    _exchange_key: str
    _exchange_durable: bool
    _exchange_type: aio_pika.ExchangeType
    _queue_name: Optional[str]
    _queue_durable: bool
    _queue_autodelete: bool
    _prefetch_count: Optional[int]

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
            queue_durable: bool = True,
            queue_autodelete: bool = False,
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
        self._connection_pool = pool
        self._receive_routing_key = receive_routing_key
        self._send_routing_key: str = send_routing_key
        self._exchange_key = exchange_key
        self._exchange_durable = exchange_durable
        self._exchange_type = exchange_type
        self._queue_name = queue_name
        self._queue_durable = queue_durable
        self._queue_autodelete = queue_autodelete
        self._prefetch_count = prefetch_count

    async def connect(self, ioloop: Optional[AbstractEventLoop], *args, **kwargs):
        """Connect to the AMQP

        Args:
            ioloop (Optional[AbstractEventLoop]): event loop
        """
        super().connect(*args, **kwargs)
        try:
            _connection: aio_pika.Connection = await self._connection_pool.acquire(loop=ioloop)
            channel = await _connection.channel()
            if self._prefetch_count:
                await channel.set_qos(prefetch_count=self._prefetch_count)
            channel.return_callbacks.add(self._amqp_message_returned)

            self.log.debug(f'Declaring exchange "{self._exchange_key}" with type {str(self._exchange_type)}')
            self._exchange = await channel.declare_exchange(
                self._exchange_key,
                type=self._exchange_type,
                durable=self._exchange_durable,
            )
            self.log.debug(f'Declaring queue "{self._queue_name}"')
            self._queue = await channel.declare_queue(
                self._queue_name, durable=self._queue_durable, auto_delete=self._queue_autodelete
            )
            self.log.debug(f'Binding queue "{self._queue.name}" to exchange "{self._exchange_key}" with routing key {self._receive_routing_key}')
            await self._queue.bind(self._exchange, routing_key=self._receive_routing_key)
            await self._queue.consume(self._amqp_message_received)
        except RuntimeError as e:
            self.log.error(f'Failed to connect(%s)', e)
        if not _connection or _connection.is_closed:
            exc = RuntimeError('Connection cancelled')
            await self.on_connection_lost(exc)
            raise exc
        await self.on_connection_made(_connection.transport)
        self.log.info('Connected OK')

    async def close(self):
        if self._queue:
            await self._queue.unbind(self._exchange, routing_key=self._receive_routing_key)
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
        routing_key = routing_key or self._send_routing_key
        if not routing_key:
            raise RuntimeError(f'Send routing key not set')
        if self._exchange:
            await self._exchange.publish(
                aio_pika.Message(
                    msg.encode('utf-8'),
                    content_encoding='utf-8',
                    reply_to=self._receive_routing_key,
                    *args,
                    **kwargs
                ),
                routing_key=routing_key,
            )
        else:
            raise RuntimeError(f'No exchange "{self._exchange_key}" found')
