import asyncio
from loguru import logger
import uuid
from redis.asyncio import Redis


class LockContext:

    def __init__(self, redis: "RedisService", key: str, ex=60, interval=0):
        """
        :param key
        :param ex: 锁多少秒过期
        :param interval: 轮询续锁间隔秒数，0 默认不开启
        """
        self.redis = redis
        self.key = key
        self.key_value = uuid.uuid4().hex
        self.ex = ex
        self.interval = interval
        self.expire_lock_task = None

    async def expire_lock(self):
        while self.interval:
            result = await self.redis.expire_lock(self.key, self.key_value, self.ex)
            logger.info(f"{self.key}续锁: 成功" if result == 1 else "失败")
            await asyncio.sleep(self.interval)

    async def __aenter__(self):
        await self.redis.get_lock(self.key, self.key_value, self.ex)
        self.expire_lock_task = asyncio.create_task(self.expire_lock())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # 取消续锁任务
        if self.expire_lock_task:
            self.expire_lock_task.cancel()
        await self.redis.release_lock(self.key, self.key_value)


class TaskLock:
    key_value = "running"

    def __init__(self, redis: "RedisService", key: str, ex=60, interval=0):
        self.redis = redis
        self.key = key
        self.ex = ex
        self.interval = interval
        self.expire_lock_task = None
        #  24小时内，是否已执行
        self.is_executed = False

    async def __aenter__(self):
        async with self.redis.redis.client() as conn:
            while True:
                if await conn.execute_command("SET", self.key, self.key_value, "EX", self.ex, "NX"):
                    logger.info("{}: 抢锁成功", self.key)
                    self.expire_lock_task = asyncio.create_task(self.expire_lock())
                    break
                if await conn.execute_command("GET", self.key) == b"succeed":
                    logger.info("{}: 已执行跳过抢锁", self.key)
                    self.is_executed = True
                    break
                await asyncio.sleep(2)
        return self

    async def expire_lock(self):
        while self.interval:
            result = await self.redis.expire_lock(self.key, self.key_value, self.ex)
            logger.info(f"{self.key}续锁: 成功" if result == 1 else "失败")
            await asyncio.sleep(self.interval)

    async def __aexit__(self, exc_type, exc, tb):
        # 取消续锁任务
        if self.expire_lock_task:
            self.expire_lock_task.cancel()
        if exc is None and not self.is_executed:  # 正常完成且指定时间内未执行过，设置锁过期时间为1天
            logger.info("{}: 设置锁过期时间为1天", self.key)
            await self.redis.set(self.key, "succeed", ex=86400)


class RedisService:

    def __init__(self, uri: str):
        self.redis = Redis.from_url(uri)

    async def get(self, key):
        async with self.redis.client() as conn:
            return await conn.execute_command("GET", key)

    async def set(self, key, value, ex=None):
        if ex:
            args = ("EX", ex)
        else:
            args = tuple()
        async with self.redis.client() as conn:
            await conn.execute_command("SET", key, value, *args)

    async def delete(self, key: str):
        async with self.redis.client() as conn:
            await conn.execute_command("DEL", key)

    async def expire(self, key: str, time: int):
        async with self.redis.client() as conn:
            await conn.execute_command("EXPIRE", key, time)

    async def hget(self, name: str, key: str):
        async with self.redis.client() as conn:
            return await conn.execute_command("HGET", name, key)

    async def hset(self, name: str, key: str, value):
        async with self.redis.client() as conn:
            await conn.execute_command("HSET", name, key, value)

    async def eval(self, script, key, *args):
        async with self.redis.client() as conn:
            await conn.execute_command("EVAL", script, len(key), *key, *args)

    def lock(self, key: str, ex=60, interval=0):
        """
        :param key
        :param ex: 锁多少秒过期
        :param interval: 轮询续锁间隔秒数，0 默认不开启
        """
        return LockContext(self, key, ex, interval)

    def task_lock(self, key: str, ex=60, interval=0):
        """
        :param key
        :param ex: 锁多少秒过期
        :param interval: 轮询续锁间隔秒数，0 默认不开启
        """
        return TaskLock(self, key, ex, interval)

    async def get_lock(self, key: str, value: str, ex=60, wait=5):
        async with self.redis.client() as conn:
            while not await conn.execute_command("SET", key, value, "EX", ex, "NX"):
                await asyncio.sleep(wait)

    async def release_lock(self, key: str, value: str):
        script = "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
        async with self.redis.client() as conn:
            await conn.execute_command("EVAL", script, 1, key, value)

    async def expire_lock(self, key: str, value: str, time: int):
        script = "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('EXPIRE',KEYS[1],ARGV[2]) else return 0 end"
        async with self.redis.client() as conn:
            return await conn.execute_command("EVAL", script, 1, key, value, time)
