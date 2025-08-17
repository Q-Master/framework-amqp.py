# -*- coding:utf-8 -*-
from typing import Optional, Dict, Callable, Any, List
from asyncio import gather, Future
from aio_pika.abc import HeadersType
from packets import Packet, makeField
from packets.processors import string_t, any_t
from asyncframework.log import get_logger
from asyncframework.app import Service
from asyncframework.aio import is_async
from .connection import AMQPConnection


__all__ = ['AMQPEventDispatcher', 'AMQPEvent']


class AMQPEvent(Packet):
    event_name: str = makeField(string_t, 'e', required=True)
    data: Optional[Any] = makeField(any_t, 'd')
    headers: Optional[HeadersType] = None
    app_id: Optional[str] = None


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
    __conn: AMQPConnection
    __signals: Dict[str, AsyncSync]

    def __init__(self, conn: AMQPConnection) -> None:
        super().__init__()
        self.__conn = conn
        self.__conn.add_callbacks(
            on_message_received=self._on_message,
            on_message_returned=self._on_message_returned
        )
        self.__signals = {}
    
    def listen(self, signal: str, callback: Callable[[Any], None]):
        self.__signals.setdefault(signal, AsyncSync()).append(callback)

    def unlisten(self, signal: str, callback: Callable[[Any], None]):
        l = self.__signals.get(signal, AsyncSync())
        if callback in l:
            l.remove(callback)

    async def broadcast(self, signal: str, data: Optional[Any] = None, headers: Optional[HeadersType] = None):
        if not self.__conn:
            raise RuntimeError('Not connected to AMQP')
        event = AMQPEvent(event_name=signal, data=data)
        await self.__conn.write(event.dumps(), headers=headers or {})

    async def __start__(self):
        await self.__conn.connect(self.ioloop)

    async def __stop__(self):
        await self.__conn.close()

    async def _on_message(self, body: str, reply_to: Optional[str] = None, headers: Optional[HeadersType] = None, app_id: Optional[str] = None):
        try:
            event = AMQPEvent.loads(body)
            event.headers = headers
            event.app_id = app_id
            l = self.__signals.get(event.event_name, None)
            if l:
                if l.not_sync:
                    await gather(*[c(event.data) for c in l.not_sync])
                for c in l.sync:
                    c(event.data)
        except Exception as e:
            self.log.error(f'Unable to dispatch the message {body}')

    def _on_message_returned(self, body: str, headers: Optional[HeadersType] = None, app_id: Optional[str] = None):
        self.log.error(f'Message "{body}" to "{self.__conn.exchange}" with "{self.__conn.routing_key}" cant be delivered')
