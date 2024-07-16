from storage import BaseStorage
from state import State
from etl import ETLComponent

class ETLMaster:
    
    #
    # Perhaps it is good idea to delete all the self attributes
    #
    def __init__(self, conn_inst, client, storage: BaseStorage, fetch_size: int, fetch_timeout):
        self._conn_inst = conn_inst
        self._client = client
        self._storage = storage
        self._state = State(self._storage)
        self._fetch_size = fetch_size
        self._fetch_timeout = fetch_timeout
        self._etl = ETLComponent(self._conn_inst, self._client, self._state, self._fetch_size, self._fetch_timeout)
    def run_etl(self):
        self._etl.perform_etl()