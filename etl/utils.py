import psycopg2
from psycopg2 import OperationalError
from config import logger
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from elasticsearch import Elasticsearch, ConnectionError
from functools import wraps
from time import sleep
from config import dsn, elastic_host


@contextmanager
def pg_conn_context(dsn: dict):
    """Контекстный менеджер для создания подключения к базам данных SQLite и Postgres"""
    try:
        pg_conn = psycopg2.connect(**dsn, cursor_factory=RealDictCursor)
        yield pg_conn
    except psycopg2.OperationalError as err:
        logger.error(f'Postgres connection context manager caught an error: {err}')
    finally:
        pg_conn.close()

@contextmanager
def elastic_client_context(*args):
    """Контекстный менеджер для создания клиента подключения к Elasticsearch"""
    try:
        client = Elasticsearch(*args)
        yield client
    except ConnectionError as err:
        logger.error(f'Elastic client context manager caught an error: {err}')
    finally:
        client.transport.close()

def pg_backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)
        
    Формула:
        t = start_sleep_time * (factor ^ n), если t < border_sleep_time
        t = border_sleep_time, иначе
    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :param n: количество повторов
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(self, *args, **kwargs): 
            conn_obj_name = '_conn_inst'
            n = 0
            while True:
                try:
                    if hasattr(self, conn_obj_name):
                        if n > 0:
                            self.__dict__[conn_obj_name].close()
                            self.__dict__[conn_obj_name] = psycopg2.connect(**dsn)
                    else:
                        logger.warn(f'ES_BACKOFF: object {self} doesn\'t contain object "{conn_obj_name}"')
                    return func(self, *args, **kwargs)
                except OperationalError as err:
                    t = start_sleep_time * (factor ** n)
                    if t > border_sleep_time:
                        t = border_sleep_time
                    n += 1
                    logger.error(f' BACKOFF: {err}, reconnecting in {t}s')
                    sleep(t)
        return inner
    return func_wrapper

def es_backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(self, *args, **kwargs): 
            conn_obj_name = '_client'
            n = 0
            while True:
                try:
                    if hasattr(self, conn_obj_name):
                        if n > 0:
                            self.__dict__[conn_obj_name].transport.close()
                            self.__dict__[conn_obj_name] = Elasticsearch(elastic_host)
                    else:
                        logger.warn(f'ES_BACKOFF: object {self} doesn\'t contain object "{conn_obj_name}"')
                    return func(self, *args, **kwargs)
                except ConnectionError as err:
                    t = start_sleep_time * (factor ** n)
                    if t > border_sleep_time:
                        t = border_sleep_time
                    n += 1
                    logger.error(f' BACKOFF: {err}, reconnecting in {t}s')
                    sleep(t)
        return inner
    return func_wrapper