from abc import ABC, abstractmethod

from redis import connection as redis_connection
from redis.exceptions import ConnectionError
import backoff as backoff

from config import BackoffConf

backoff_conf = BackoffConf()


class AbstractBaseStorage(ABC):
    @abstractmethod
    def set(self, key, value):
        """
            устанавливает значение
        :return:
        """
    @abstractmethod
    def get(sekf, key) -> str:
        """
            возвращает значение
        :return:
        """


class RedisStorage(AbstractBaseStorage):
    def __init__(self, redis_conn: redis_connection):
        self.redis_conn = redis_conn

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError,),
        **backoff_conf.dict()
    )
    def get(self, values):
        self.redis_conn.get(values)

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError,),
        **backoff_conf.dict()
    )
    def set(self, key, values):
        self.redis_conn.set(key, values)


class State:
    def __init__(self, storage: AbstractBaseStorage):
        self.storage = storage

    def set_state(self, key, val):
        return self.storage.set(key, val)

    def get_state(self, key):
        return self.storage.get(key)
