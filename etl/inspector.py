from contextlib import closing
import abc
from datetime import datetime
from config import logger, target_schema
from utils import pg_backoff
from psycopg2.extensions import connection as _connection
from state import State
class BaseInspector(abc.ABC):

    @abc.abstractmethod
    def inspect(self) -> int:
        pass

    def _get_time_modified(self):
        pass

class InspectorComponent(BaseInspector):
    
    def __init__(self, conn_inst: _connection, state: State, table_name: str, time_modified_key_name: str):
        self._conn_inst = conn_inst
        self._state = state
        self._table_name = table_name
        self.time_modified_key_name = time_modified_key_name;

    def _get_time_modified(self):
        time_modified = self._state.get_state(self.time_modified_key_name)
        if time_modified is None:
            time_modified = datetime(1970, 1, 1)
        return time_modified
    
    @pg_backoff()
    def inspect(self) -> int:
        time_modified = self._get_time_modified()
        query = f'SELECT COUNT(id) FROM "{target_schema}"."{self._table_name}" WHERE modified > \'{time_modified}\''
        with closing(self._conn_inst.cursor()) as cursor:
            cursor.execute(query)
            val = cursor.fetchone()['count']
        logger.debug(f'new rows in "{target_schema}"."{self._table_name}\" since \'{time_modified}\': {val}')
        return val
    

class PersonInspector():
    def __init__(self, conn_inst: _connection, state: State):
        person_time_modified = 'person_time_modified'
        table_name = 'person'
        super().__init__(conn_inst, state, table_name, person_time_modified)

class GenreInspector():
    def __init__(self, conn_inst: _connection, state: State):
        time_modified_key_name = 'genre_time_modified'
        table_name = 'genre'
        super().__init__(conn_inst, state, table_name, time_modified_key_name)

class FilmworkInspector():
    def __init__(self, conn_inst: _connection, state: State):
        time_modified_key_name = 'filmwork_time_modified'
        table_name = 'film_work'
        super().__init__(conn_inst, state, table_name, time_modified_key_name)
        
class DBInspector():
    def __init__(self, conn_inst: _connection, state: State):
        self._conn_inst = conn_inst
        self._state = state
        self._filmwork_inspector = FilmworkInspector(self._conn_inst, self._state)
        self._genre_inspector = GenreInspector(self._conn_inst, self._state)
        self._person_inspector = PersonInspector(self._conn_inst, self._state)

    def inspect_filmwork(self) -> bool:
        filmwork_upd = self._filmwork_inspector.inspect()
        return filmwork_upd > 0
    
    def inspect_genre(self) -> bool:
        genre_upd = self._genre_inspector.inspect()
        return genre_upd > 0
    
    def inspect_person(self):
        person_upd = self._genre_inspector.inspect()
        return person_upd.inspect() > 0
    