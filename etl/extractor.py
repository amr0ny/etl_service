import abc
from psycopg2.extensions import connection as _connection
from contextlib import closing
from state import State
from datetime import datetime
from utils import pg_backoff
from config import logger


class BaseExtractor(abc.ABC):   
    @abc.abstractmethod
    def extract_data(self) -> dict[str, any]:
        pass

    @abc.abstractmethod
    def _enrich_data(self, data) -> dict[str, any]:
        pass

    @abc.abstractmethod
    def _merge_data(self, data) -> dict[str, any]:
        pass

    @abc.abstractmethod
    def _get_time_modified(self):
        pass


class ExtractorComponent(BaseExtractor):

    def __init__(self, conn_inst: _connection, state: State, fetch_size: int):
        self._conn_inst = conn_inst
        self._state = state
        self._fetch_size = fetch_size

    def _fetch(self, query: str) -> list:
        with closing(self._conn_inst.cursor()) as cursor:
            cursor.execute(query)
            data = list()
            while len(batch := cursor.fetchmany(self._fetch_size)):
                data.extend(batch)
        return data
    
    @pg_backoff()
    def _merge_data(self, data: list[dict[str, any]]) -> list[dict[str, any]]:
        filmwork_ids_str = ','.join([f"'{row['id']}'" for row in data])
        query = f'SELECT fw.id as film_work_id, fw.title, fw.description, fw.rating, fw.type, fw.created, fw.modified, array_agg(DISTINCT g.name) as genres FROM content.film_work fw LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id LEFT JOIN content.genre g ON g.id = gfw.genre_id WHERE fw.id IN ({filmwork_ids_str}) GROUP BY fw.id'        
        data = self._fetch(query)
        logger.debug(f'film_work - merge_data: recieved rows: {len(data)}')
        query = f"SELECT fw.id as film_work_id, COALESCE (json_agg(DISTINCT jsonb_build_object('person_role', pfw.role,'person_id', p.id,'person_name', p.full_name)) FILTER (WHERE p.id is not null),'[]') as persons FROM content.film_work fw LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id LEFT JOIN content.person p ON p.id = pfw.person_id WHERE fw.id IN ({filmwork_ids_str}) GROUP BY fw.id"
        persons_data = self._fetch(query)
        persons_data_dict = {row['film_work_id']: row['persons'] for row in persons_data}
        for row in data:
            row['persons'] = persons_data_dict[row['film_work_id']]
        logger.debug(f'merge_data: recieved rows: {len(data)}')
        return data


class PersonExtractor(ExtractorComponent):

    time_modified_key_name = 'person_time_modified'

    def _get_time_modified(self):
        time_modified = self._state.get_state(self.time_modified_key_name)
        if time_modified is None:
            time_modified = datetime.min
        return time_modified
    
    def __init__(self, conn_inst: _connection, state: State, fetch_size: int):
        super().__init__(conn_inst, state, fetch_size)

    @pg_backoff()
    def extract_data(self) -> list[dict[str, any]]:
        time_modified = self._get_time_modified()
        query = f'SELECT id, modified FROM content.person WHERE modified > \'{time_modified}\' ORDER BY modified'
        data = self._fetch(query)
        logger.debug(f'person – extract_data: recieved rows: {len(data)}')
        enriched_data = self._enrich_data(data)
        filmwork_data = self._merge_data(enriched_data)
        self._state.set_state(self.time_modified_key_name, str(data[-1]['modified']))
        return filmwork_data
    
    @pg_backoff()
    def _enrich_data(self, data: list[dict[str, any]]) -> list[dict[str, any]]:
        person_ids_str = ','.join([f"'{row['id']}'" for row in data])
        query = f'SELECT DISTINCT fw.id, fw.modified FROM content.film_work fw LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id WHERE pfw.person_id IN ({person_ids_str}) ORDER BY fw.modified'
        data = self._fetch(query)
        logger.debug(f'person – enrich_data: recieved rows: {len(data)}')
        return data
    
    def _merge_data(self, data: list[dict[str, any]]) -> list[dict[str, any]]:
        return super()._merge_data(data)


class FilmworkExtractor(ExtractorComponent):

    time_modified_key_name = 'filmwork_time_modified'

    def _get_time_modified(self):
        time_modified = self._state.get_state(self.time_modified_key_name)
        if time_modified is None:
            time_modified = datetime(1970, 1, 1)
        return time_modified
    
    def __init__(self, conn_inst: _connection, state: State, fetch_size: int):
        super().__init__(conn_inst, state, fetch_size)

    @pg_backoff()
    def extract_data(self) -> list[dict[str, any]]:
        time_modified = self._get_time_modified()
        query = f'SELECT fw.id, fw.modified FROM content.film_work fw WHERE modified > \'{time_modified}\' ORDER BY modified'
        data = self._fetch(query)
        logger.debug(f'film_work - extract_data: recieved rows: {len(data)}')
        filmwork_data = self._merge_data(data)
        self._state.set_state(self.time_modified_key_name, str(data[-1]['modified']))
        return filmwork_data
    
    def _enrich_data(self, data: list[dict[str, any]]) -> list[dict[str, any]]:
        return super()._enrich_data(data)
    
    

class GenreExtractor(ExtractorComponent):

    time_modified_key_name = 'genre_time_modified'

    def _get_time_modified(self):
        time_modified = self._state.get_state(self.time_modified_key_name)
        if time_modified is None:
            time_modified = datetime(1970, 1, 1)
        return time_modified
    
    def __init__(self, conn_inst: _connection, state: State, fetch_size: int):
        super().__init__(conn_inst, state, fetch_size)

    @pg_backoff()
    def extract_data(self) -> list[dict[str, any]]:
        time_modified = self._get_time_modified()
        query = f'SELECT g.id, g.modified FROM content.genre g WHERE modified > \'{time_modified}\' ORDER BY modified'
        data = self._fetch(query)
        logger.debug(f'genre - extract_data: recieved rows: {len(data)}')
        enriched_data = self._enrich_data(data)
        filmwork_data = self._merge_data(enriched_data)
        self._state.set_state(self.time_modified_key_name, str(data[-1]['modified']))
        return filmwork_data
    
    @pg_backoff()
    def _enrich_data(self, data: list[dict[str, any]]) -> list[dict[str, any]]:
        genre_ids_str = ','.join([f"'{row['id']}'" for row in data])
        query = f'SELECT DISTINCT fw.id, fw.modified FROM content.film_work fw LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id WHERE gfw.genre_id IN ({genre_ids_str}) ORDER BY fw.modified'
        data = self._fetch(query)
        logger.debug(f'genre – enrich_data: recieved rows: {len(data)}')
        return data
    
    def _merge_data(self, data: list[dict[str, any]]) -> list[dict[str, any]]:
        return super()._merge_data(data)