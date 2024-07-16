from psycopg2.extensions import connection as _connection
from elasticsearch import Elasticsearch
from utils import elastic_client_context, pg_conn_context
from config import dsn, elastic_host, storage_path, fetch_size, fetch_timeout
from storage import JsonFileStorage
from etl_master import ETLMaster

def load_data(pg_conn: _connection, client: Elasticsearch):
    storage = JsonFileStorage(storage_path)
    etl_master = ETLMaster(pg_conn, client, storage, fetch_size, fetch_timeout)
    etl_master.run_etl()

if __name__ == '__main__':
    with pg_conn_context(dsn) as pg_conn, elastic_client_context(elastic_host) as client:
        load_data(pg_conn, client)