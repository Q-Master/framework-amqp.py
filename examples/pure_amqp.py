# -*- coding: utf-8 -*-
import asyncio
from uuid import uuid4
from asyncframework.app import main
from asyncframework.app.script import Script, Service
from asyncframework.log import get_logger, init_logging
from asyncframework.rpc import RPC, rpc_method
from asyncframework.rpc.types import RPCException
from asyncframework.util.datetime import time
from asyncframework.util.format_exception import format_exception
from asyncframework.amqp import AMQPPool, AMQPConnection

AMQP_URLS = ['amqp://127.0.0.1:5672/example']

class Example(Script):
    log = get_logger('Example')
    message_processed: asyncio.Future

    class Receiver(Service):
        log = get_logger('Example.Receiver')

        def __init__(self, ex: 'Example') -> None:
            super().__init__(linear=False)
            self._ex = ex
            self._pool = AMQPPool(AMQP_URLS, 1)
            self._connector = AMQPConnection(
                self._pool, '',
                receive_routing_key='example',
                send_routing_key='',
                queue_name='example',
                queue_durable=False,
                queue_exclusive=True, 
                consume_exclusive=False,
                queue_autodelete=True
            )
            self._connector.add_callbacks(
                on_message_received=self._on_message_received,
                on_connection_lost=self._on_connection_lost,
                on_message_returned=self._on_message_returned
            )

        async def _on_message_received(self, msg: str, *args, **kwargs):
            self.log.info(f'Сообщение получено: {msg}, ARGS:{args}, KWARGS:{kwargs}')

        async def _on_connection_lost(self, exc: Exception):
            self.log.error(f'Сервисное AMQP соединение потеряно по причине: {format_exception(exc)}')
            raise exc

        async def _on_message_returned(self, msg: str, **kwargs):
            self.log.warning(f'Сообщение не доставлено: {msg} {kwargs}')
        
        async def __start__(self):
            self.log.info('Starting example AMQP Listener')
            await self._connector.connect(self.ioloop)
            await self._connector.consume()

        async def __stop__(self):
            self.log.info('Stopping example AMQP Listener')
            await self._connector.close()
            await self._pool.stop()

    def __init__(self, config):
        init_logging(True, log_level='INFO')
        super().__init__('')
        self._receiver = Example.Receiver(self)
        self._pool = AMQPPool(AMQP_URLS, 1)
        self._connector = AMQPConnection(
            self._pool, '',
            receive_routing_key='',
            send_routing_key='',
            queue_name_as_rkey=True,
            queue_exclusive=True, 
            consume_exclusive=True,
            queue_autodelete=True
        )
        self._connector.add_callbacks(
            on_message_received=self._on_message_received,
            on_connection_lost=self._on_connection_lost,
            on_message_returned=self._on_message_returned
        )

    async def _on_message_received(self, msg: str, **kwargs):
        self.log.info(f'Сообщение получено: {msg}, KWARGS:{kwargs}')
    
    async def _on_connection_lost(self, exc: Exception):
        self.log.error(f'Сервисное AMQP соединение потеряно по причине: {format_exception(exc)}')
        raise exc

    async def _on_message_returned(self, msg: str, **kwargs):
        self.log.warning(f'Сообщение не доставлено: {msg}, KWARGS: {kwargs}')
        self.message_processed.set_result(None)

    async def __start__(self):
        self.log.info('Starting example AMQP script')
        self.message_processed = asyncio.Future()
        await self._receiver.start(self.ioloop)
        await self._connector.connect(self.ioloop)
        await self._connector.consume()
        self.message_processed = asyncio.Future()

    async def __stop__(self):
        self.log.info('Stopping example AMQP script')
        await self.message_processed
        await self._connector.close()
        await self._pool.stop()
        await self._receiver.stop()

    async def __body__(self):
        start_time = time.time()
        self.log.info('Sending actual request')
        await self.publish_event('example')
        self.log.info('Sent request')
        self.log.info('Sending request should cause return')
        await self.publish_event('notexample')
        self.log.info('Sent request')
        self.log.info(f'Messages processed ok {time.time() - start_time}')

    async def publish_event(self, rkey: str):
        await self._connector.write(
            'test text', 
            correlation_id=uuid4().hex,
            routing_key=rkey,
            mandatory=True,
            headers={}
        )

if __name__ == '__main__':
    main(Example, None)

