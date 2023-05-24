# -*- coding:utf-8 -*-
from typing import Optional, Dict, Callable, Any, List
from asyncio import gather, Future
from packets import Packet, Field, string_t, any_t
from asyncframework.log import get_logger
from asyncframework.app import Service
from asyncframework.aio import is_async
from .pool import AMQPPool
from .connection import AMQPConnection


__all__ = ['AMQPEventDispatcher', 'AMQPEvent']


class AMQPEvent(Packet):
    event_name = Field(string_t, 'e')
    data = Field(any_t, 'd')


class AsyncSync:
    sync: List[Callable[[Any], None]] = []
    not_sync: List[Callable[[Any], Future[None]]] = []

    def __init__(self) -> None:
        self.sync = []
        self.not_sync = []
    
    def __contains__(self, value) -> bool:
        return value in self.not_sync or value in self.sync
    
    def append(self, value):
        if is_async(value):
            self.not_sync.append(value)
        else:
            self.sync.append(value)
    
    def remove(self, value):
        if is_async(value):
            self.not_sync.remove(value)
        else:
            self.sync.remove(value)


class AMQPEventDispatcher(Service):
    log = get_logger('AMQPEvent')
    __conn: Optional[AMQPConnection] = None
    __signals: Dict[str, AsyncSync]
    __exchange: str
    __routing_key: str

    def __init__(self, pool: AMQPPool, exchange: str, routing_key: str) -> None:
        super().__init__()
        self.__conn = AMQPConnection(pool, exchange, routing_key, routing_key, exchange_declare=False, queue_durable=False, queue_autodelete=True, queue_exclusive=True)
        self.__conn.add_callbacks(
            on_message_received=self._on_message,
            on_message_returned=self._on_message_returned
        )
        self.__signals = {}
        self.__exchange = exchange
        self.__routing_key = routing_key
    
    def listen(self, signal: str, callback: Callable[[Any], None]):
        self.__signals.setdefault(signal, AsyncSync()).append(callback)

    def unlisten(self, signal: str, callback: Callable[[Any], None]):
        l = self.__signals.get(signal, AsyncSync())
        if callback in l:
            l.remove(callback)

    async def broadcast(self, signal: str, data: Optional[Any] = None):
        if not self.__conn:
            raise RuntimeError('Not connected to AMQP')
        event = AMQPEvent(event_name=signal, data=data)
        await self.__conn.write(event.dumps())

    async def __start__(self):
        await self.__conn.connect(self.ioloop)

    async def __stop__(self):
        await self.__conn.close()

    async def _on_message(self, body: str, routing_key: Optional[str] = None):
        try:
            event = AMQPEvent.loads(body)
            l = self.__signals.get(event.event_name, None)
            if l:
                if l.not_sync:
                    await gather(c(event.data) for c in l.not_sync)
                for c in l.sync:
                    c(event.data)
        except Exception as e:
            self.log.error(f'Unable to dispatch the message {body}')

    def _on_message_returned(self, body: str):
        self.log.error(f'Message "{body}" to "{self.__exchange}" with "{self.__routing_key}" cant be delivered')
