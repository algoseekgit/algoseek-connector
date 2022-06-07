import os
from typing import Callable, Optional, Any

from clickhouse_driver import Client
from clickhouse_driver.errors import Error


class ExecutionError(Exception):
    pass


class Session(object):

    def __init__(
            self,
            host: Optional[str] = None,
            user: Optional[str] = None,
            password: Optional[str] = None,
            port: Optional[int] = None,
            secure: bool = False,
            **kwargs: Any
            ):
        default_port = 9000
        self._host = host or os.getenv('AS_DATABASE_HOST')
        self._port = port or os.getenv('AS_DATABASE_PORT') or default_port
        self._user = user or os.getenv('AS_DATABASE_USER')
        self._password = password or os.getenv('AS_DATABASE_PASSWORD')
        self._secure = secure

        self.client = Client(
            self._host,
            user=self._user,
            password=self._password,
            port=self._port,
            secure=self._secure,
            **kwargs
        )

    def ping(self):
        return self.execute('SELECT 1')

    def close(self):
        self.client.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _wrap_exception(self, func: Callable, query: str, **kwargs: Any):
        try:
            return func(query, **kwargs)
        except Error as e:
            message = e.message.split('Stack trace:')[0].split(': ', 2)[-1]
            raise ExecutionError(message)

    def execute(self, query: str, **kwargs: Any):
        return self._wrap_exception(self.client.execute, query, **kwargs)

    def query_dataframe(self, query: str, **kwargs: Any):
        return self._wrap_exception(self.client.query_dataframe, query, **kwargs)

    def __hash__(self) -> int:
        return hash(f"{self._host}{self._user}{self._password}")
