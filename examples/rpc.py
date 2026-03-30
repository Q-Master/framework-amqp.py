# -*- coding: utf-8 -*-
import asyncio
from asyncframework.app import main
from asyncframework.app.script import Script, Service
from asyncframework.log import get_logger, init_logging
from asyncframework.rpc import RPC, rpc_method
from asyncframework.util.datetime import time
from asyncframework.amqp import AMQPPool, AMQPConnection


class Example(Script):
    log = get_logger('Example')
    message_processed: asyncio.Future

    class Receiver(Service):
        log = get_logger('Example.Receiver')

        def __init__(self, ex: 'Example') -> None:
            super().__init__(linear=False)
            self._ex = ex
            self._pool = AMQPPool(['amqp://127.0.0.1:5672/example'], 1)
            self._connector = AMQPConnection(
                self._pool, '',
                receive_routing_key='example',
                send_routing_key='',
                queue_name='example',
                queue_durable=True,
                queue_exclusive=False, 
                consume_exclusive=False
            )
            self._rpc = RPC[Example.Receiver](
                self,
                self._connector
            )
        
        async def __start__(self):
            self.log.info('Starting example AMQP Listener')
            await self._connector.connect(self.ioloop)

        async def __stop__(self):
            self.log.info('Stopping example AMQP Listener')
            await self._connector.close()
            await self._pool.stop()

        @rpc_method()
        async def process_request(self, **kwargs):
            self.log.info('Received request')
            self._ex.message_processed.set_result(None)
            self.log.info('Request processed')
            return True


    def __init__(self, config):
        init_logging(True, log_level='INFO')
        super().__init__('')
        self._receiver = Example.Receiver(self)
        self._pool = AMQPPool(['amqp://127.0.0.1:5672/example'], 1)
        self._connector = AMQPConnection(
            self._pool, '',
            receive_routing_key='',
            send_routing_key='example',
            queue_name_as_rkey=True,
            queue_exclusive=True,
            consume_exclusive=True,
            queue_autodelete=True
        )
        self._rpc = RPC(
            self,
            self._connector,
            dont_receive=True
        )

    async def __start__(self):
        self.log.info('Starting example AMQP script')
        self.message_processed = asyncio.Future()
        await self._receiver.start(self.ioloop)
        await self._connector.connect(self.ioloop)
        self.message_processed = asyncio.Future()

    async def __stop__(self):
        self.log.info('Stopping example AMQP script')
        await self._connector.close()
        await self._pool.stop()
        await self._receiver.stop()

    async def __body__(self):
        start_time = time.time()
        self.log.info('Sending request')
        await self._rpc.call('process_request', response_required=False, headers={'rpc': 1})
        self.log.info('Sent request')
        await self.message_processed
        self.log.info(f'Message processed ok {time.time() - start_time}')
    

if __name__ == '__main__':
    main(Example, None)

