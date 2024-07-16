import abc
from typing import Iterable
from utils import es_backoff
from pydantic import BaseModel
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class BaseLoader(abc.ABC):
    @property
    @abc.abstractmethod
    def __init__(self, conn_inst: any, models: list[BaseModel]):
        pass

    @abc.abstractmethod
    def load_data(self) -> None:
        pass


class ComponentLoader(BaseLoader):
    def __init__(self, client: Elasticsearch, index: str, fetch_size: int):
        self._client = client
        self._index = index
        self._fetch_size = fetch_size
    
    def _get_bulk_data(self, models: list[BaseModel]):
        bulk_data = []
        for model in models:
            bulk_data.append(
                {
                    "_index": self._index,
                    "_id": str(model.id),
                    "_source": model.model_dump()
                }
            )
        return bulk_data
    
    @es_backoff()
    def _bulk(self, bulk_data_batch: Iterable):
        bulk(self._client, bulk_data_batch)


    def load_data(self, models: list[BaseModel]) -> None:
        bulk_data = self._get_bulk_data(models)
        n = 1
        while len(bulk_data_batch := bulk_data[self._fetch_size*(n-1):self._fetch_size*n]) > 0:     
            self._bulk(bulk_data_batch)
            n += 1
        
        
class Loader(ComponentLoader):
    def __init__(self, client: Elasticsearch, index: str, fetch_size: int):
        super().__init__(client, index, fetch_size)