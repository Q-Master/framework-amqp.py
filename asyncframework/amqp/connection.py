# -*- coding: utf-8 -*-
from typing import Optional, Union, List, Tuple
import aio_pika
from asyncio import AbstractEventLoop
from asyncframework.net.connection_base import ConnectionBase
from asyncframework.log.log import get_logger
from .pool import AMQPPool


class AMQPConnection(ConnectionBase):
    log = get_logger('AMQPConnection')
    __channel: aio_pika.abc.AbstractChannel
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
    __queue_name_as_rkey: bool
    __consume_exclusive: bool
    __consume_noack: bool
    __consumer_tag: Optional[str]
    __queue_name_as_consumer_tag: bool
    __prefetch_count: Optional[int]
    __additional_binds: List[Tuple[aio_pika.abc.ExchangeParamType, str]]

    @property
    def exchange(self) -> str:
        return self.__exchange_key or ''

    @property
    def routing_key(self) -> str:
        return self.__receive_routing_key or ''

    @property
    def queue_name(self) -> Optional[str]:
        return self.__queue.name if self.__queue else None

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
            queue_name_as_rkey: bool = False,
            consume_exclusive: bool = False,
            consume_noack: bool = False,
            consumer_tag: Optional[str] = None,
            queue_name_as_consumer_tag: bool = False,
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
        self.__connection = None
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
        self.__queue_name_as_rkey = queue_name_as_rkey
        self.__consume_exclusive = consume_exclusive
        self.__consume_noack = consume_noack
        self.__consumer_tag = consumer_tag
        self.__queue_name_as_consumer_tag = queue_name_as_consumer_tag
        self.__prefetch_count = prefetch_count
        if self.__exchange_key.startswith('amq.') or not self.__exchange_key:
            self.__exchange_declare = False
        self.__additional_binds = []

    async def connect(self, ioloop: Optional[AbstractEventLoop], *args, **kwargs) -> None:
        """Connect to the AMQP

        Args:
            ioloop (Optional[AbstractEventLoop]): event loop
        """
        super().connect(*args, **kwargs)
        try:
            self.__connection: aio_pika.Connection = await self.__connection_pool.acquire(ioloop)
            self.__channel = await self.__connection.channel()
            if self.__prefetch_count:
                await self.__channel.set_qos(prefetch_count=self.__prefetch_count)
            self.__channel.return_callbacks.add(self._amqp_message_returned)
            if self.__exchange_declare:
                self.log.debug(f'Declaring exchange "{self.__exchange_key}" with type {str(self.__exchange_type)}')
                self.__exchange = await self.__channel.declare_exchange(
                    self.__exchange_key,
                    type=self.__exchange_type,
                    durable=self.__exchange_durable,
                )
            else:
                if self.__exchange_key:
                    self.__exchange = await self.__channel.get_exchange(self.__exchange_key)
                else:
                    self.__exchange = self.__channel.default_exchange
            self.log.debug(f'Declaring queue "{self.__queue_name}"')
            self.__queue = await self.__channel.declare_queue(
                self.__queue_name, durable=self.__queue_durable, auto_delete=self.__queue_autodelete, exclusive=self.__queue_exclusive
            )
            if self.__queue_name_as_rkey:
                self.__receive_routing_key = self.__queue.name
            if self.__queue_name_as_consumer_tag:
                self.__consumer_tag = self.__queue.name
            if self.__exchange_key:
                self.log.debug(f'Binding queue "{self.__queue.name}" to exchange "{self.__exchange_key}" with routing key {self.__receive_routing_key}')
                await self.__queue.bind(self.__exchange, routing_key=self.__receive_routing_key)
            self.__consumer_tag = await self.__queue.consume(self._amqp_message_received, consumer_tag=self.__consumer_tag, exclusive=self.__consume_exclusive, no_ack=self.__consume_noack)
        except RuntimeError as e:
            self.log.error(f'Failed to connect(%s)', e)
        if not self.__connection or self.__connection.is_closed:
            exc = RuntimeError('Connection cancelled')
            await self.on_connection_lost(exc)
            raise exc
        await self.on_connection_made(self.__connection.transport)
        self.log.info('Connected OK')

    async def close(self) -> None:
        if self.__queue:
            await self.__queue.cancel(self.__consumer_tag)
            if self.__exchange_key:
                await self.__queue.unbind(self.__exchange, routing_key=self.__receive_routing_key)
        if self.__connection:
            await self.__connection.close()
        super().close()
        self.log.info('Connection closed')

    async def add_bind(self, exchange: aio_pika.abc.ExchangeParamType, routing_key: str) -> None:
        if (exchange, routing_key) not in self.__additional_binds:
            if self.__queue:
                self.__additional_binds.append((exchange, routing_key))
                await self.__queue.bind(exchange, routing_key)
        else:
            self.log.error(f'Bind to {exchange}/{routing_key} already exists')

    async def remove_bind(self, exchange: aio_pika.abc.ExchangeParamType, routing_key: str) -> None:
        if (exchange, routing_key) in self.__additional_binds:
            if self.__queue:
                self.__additional_binds.remove((exchange, routing_key))
                await self.__queue.unbind(exchange, routing_key)
        else:
            self.log.error(f'Bind to {exchange}/{routing_key} does not exist')

    async def write(self, msg: str, *args, routing_key: Optional[str] = None, mandatory: bool = False, immediate: bool = False, **kwargs) -> None:
        self.log.debug(f'Sending envelope with routing_key "{routing_key}", msg: "{msg}"')
        routing_key = routing_key or self.__send_routing_key
        if not routing_key:
            raise RuntimeError(f'Send routing key not set')
        await self.__exchange.publish(
            aio_pika.Message(
                msg.encode('utf-8'),
                *args,
                content_encoding='utf-8',
                reply_to=self.__receive_routing_key,
                **kwargs
            ),
            mandatory=mandatory,
            immediate=immediate,
            routing_key=routing_key,
        )
    
    async def write_to_exchange(self, 
        exchange: Union[aio_pika.abc.AbstractExchange, str], msg: str, *args, 
        routing_key: Optional[str] = None, mandatory: bool = False, immediate: bool = False, **kwargs
        ) -> None:
        if isinstance(exchange, str):
            exchange = await self.__channel.get_exchange(exchange)
        self.log.debug(f'Sending envelope with routing_key "{routing_key}", msg: "{msg}" to {exchange.name}')
        routing_key = routing_key or self.__send_routing_key
        if not routing_key:
            raise RuntimeError(f'Send routing key not set')
        await exchange.publish(
            aio_pika.Message(
                msg.encode('utf-8'),
                *args,
                reply_to=self.__queue.name if self.__queue else None,
                content_encoding='utf-8',
                **kwargs
            ),
            mandatory=mandatory,
            immediate=immediate,
            routing_key=routing_key,
        )

    async def get_exchange(self, exchange_name: str) -> aio_pika.abc.AbstractExchange:
        return await self.__channel.get_exchange(exchange_name)

    async def _amqp_message_received(self, msg: aio_pika.abc.AbstractIncomingMessage) -> None:
        async with msg.process():
            body = msg.body.decode('utf-8')
            self.log.debug(f'Got envelope reply_to: {msg.reply_to}, body: {body}')
            await self.on_message_received(
                body, 
                reply_to=msg.reply_to, 
                headers=msg.headers, 
                app_id=msg.app_id, 
                correlation_id=msg.correlation_id,
                content_type=msg.content_type,
                msg_type=msg.type
            )

    async def _amqp_message_returned(self, msg: aio_pika.message.ReturnedMessage) -> None:
        self.log.debug(f'Message returned "{msg}"')
        async with msg.process():
            body = msg.body.decode('utf-8')
            await self.on_message_returned(
                body, 
                headers=msg.headers, 
                app_id=msg.app_id, 
                correlation_id=msg.correlation_id,
                content_type=msg.content_type,
                msg_type=msg.type
            )

