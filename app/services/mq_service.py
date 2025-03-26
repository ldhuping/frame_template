import logging
from typing import Optional, Iterator, Callable
from asyncio.queues import Queue
from asyncio.locks import Lock

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection
from aio_pika import Message


class ChannelContext:
    def __init__(self, channel_pool: "ChannelPool"):
        self.pool = channel_pool

    async def __aenter__(self) -> AbstractChannel:
        self.channel = await self.pool.get_channel()
        return self.channel

    async def __aexit__(self, exc_type, exc_value, exec_traceback):
        await self.pool._pools.put(self.channel)

class ChannelPool:

    def __init__(self, url: str, max_size=100):
        self.url: str = url
        self._max_size = max_size  # 最大管道数
        self._now_size = max_size  # 可用管道数
        self._lock = Lock()
        self._pools = Queue(self._max_size)
        self._connection: Optional[AbstractConnection] = None

    @property
    def is_closed(self) -> bool:
        return self._connection is None or self._connection.is_closed

    async def get_connection(self) -> AbstractConnection:
        if self.is_closed:
            logging.info("创建MQ连接...")
            self._connection = await aio_pika.connect_robust(self.url, timeout=30)
        return self._connection

    async def close(self):
        if not self.is_closed:
            await self._connection.close()

    async def get_channel(self, callback:Callable=None) -> AbstractChannel:
        async with self._lock:
            if self.is_closed:
                self._now_size = self._max_size
                self._pools = Queue(self._max_size)
                await self.get_connection()
            logging.info(f"{callback} 获取MQ Channel, 当前可用Channel:{self._now_size}")
            if self._pools.empty() and self._now_size:  # 可以创建管道并且管道池为空
                self._now_size -= 1
                return await self._connection.channel()
        return await self._pools.get()

    async def publish(self, exchange, routing_key, body: bytes, headers: dict=None):
        async with ChannelContext(self) as channel:
            exchange = await channel.declare_exchange(exchange, type="direct", durable=True)
            await (await channel.declare_queue(routing_key, durable=True)).bind(exchange, routing_key)
            await exchange.publish(Message(body=body, headers=headers),routing_key=routing_key)

    async def publish_dl(self, exchange, routing_key, body: bytes, headers, expiration=10):
        async with ChannelContext(self) as channel:
            exchange = await channel.declare_exchange(exchange, type="direct", durable=True)
            k = f"{routing_key}_dl"
            await (await channel.declare_queue(k, durable=True, arguments={
                'x-dead-letter-exchange': exchange.name,
                'x-dead-letter-routing-key': routing_key})).bind(exchange, k)
            # 声明队列并绑定routing_key
            # await (await channel.declare_queue(routing_key, durable=True)).bind(exchange, routing_key)
            await exchange.publish(Message(body=body, delivery_mode=2, headers=headers, expiration=expiration), routing_key=k)

    async def publish_batch(self, exchange, routing_key, body_iterator: Iterator[bytes]):
        async with ChannelContext(self) as channel:
            exchange = await channel.declare_exchange(exchange, type="direct", durable=True)
            for i in body_iterator:
                await exchange.publish(Message(body=i), routing_key=routing_key)

    async def publish_q(self, exchange, routing_key, queue: Queue[bytes]):
        async with ChannelContext(self) as channel:
            exchange = await channel.declare_exchange(exchange, type="direct", durable=True)
            while not queue.empty():
                await exchange.publish(Message(body=await queue.get()), routing_key=routing_key)

    async def run_consumer(self, exchange, queue, callback):
        """启动一个消费者"""
        channel = await self.get_channel(callback.__name__)
        exchange = await channel.declare_exchange(exchange, type="direct", durable=True)
        mq_queue = await channel.declare_queue(queue, durable=True)
        await mq_queue.bind(exchange, queue)
        await mq_queue.consume(callback)
