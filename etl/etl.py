import abc
from psycopg2.extensions import connection as _connection
from elasticsearch import Elasticsearch
from typing import Callable
from loader import BaseLoader, Loader
from inspector import DBInspector
from config import logger
from extractor import BaseExtractor, PersonExtractor, FilmworkExtractor, GenreExtractor
from transformer import BaseTransformer, Transformer
from state import State


class BaseETLComponent(abc.ABC):
    
    @property
    @abc.abstractmethod
    def db_inspector(self):
        return self._db_inspector
    
    @property
    @abc.abstractmethod
    def extractor(self) -> BaseExtractor:
        pass

    @property
    @abc.abstractmethod
    def loader(self) -> BaseLoader:
        pass
    
    @property
    @abc.abstractmethod
    def transformer(self) -> BaseTransformer:
        pass

    @abc.abstractmethod
    def perform_etl(self) -> None:
        pass


import threading
import time

class ETLComponent(BaseETLComponent):

    @property
    def db_inspector(self):
        return self._db_inspector

    @property
    def extractor(self):
        return self._extractor

    @property
    def transformer(self):
        return self._transformer

    @property
    def loader(self):
        return self._loader
    def __init__(self, db_conn: _connection, client: Elasticsearch, state: State, fetch_size: int, fetch_timeout: int):
        self._conn_inst = db_conn
        self._client = client
        self._state = state
        self._fetch_size = fetch_size
        self._fetch_timeout = fetch_timeout
        self._db_inspector = DBInspector(self._conn_inst, self._state)
        self._person_extractor = PersonExtractor(self._conn_inst, self._state, self._fetch_size)
        self._genre_extractor = GenreExtractor(self._conn_inst, self._state, self._fetch_size)
        self._filmwork_extractor = FilmworkExtractor(self._conn_inst, self._state, self._fetch_size)
        self._transformer = Transformer()
        self._loader = Loader(self._client, 'movies', self._fetch_size)

    def _process_entity(self, inspector_method: Callable, extractor: BaseExtractor, transformer: BaseTransformer, loader: BaseLoader) -> None:
        while True:
            if inspector_method():
                data = extractor.extract_data()
                logger.debug(data)
                models = transformer.transform(data)
                loader.load_data(models)
            time.sleep(self._fetch_timeout)

    def perform_etl(self) -> None:
        threads = [
            threading.Thread(target=self._process_entity, args=(self.db_inspector.inspect_filmwork, self._filmwork_extractor, self._transformer, self._loader)),
            threading.Thread(target=self._process_entity, args=(self.db_inspector.inspect_genre, self._genre_extractor, self._transformer, self._loader)),
            threading.Thread(target=self._process_entity, args=(self.db_inspector.inspect_person, self._person_extractor, self._transformer, self._loader)),
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
