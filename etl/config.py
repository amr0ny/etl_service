import os
from logger import Logger
from dotenv import load_dotenv

load_dotenv()

#database configuration
dsn = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'password': os.getenv('POSTGRES_PASSWORD')
}
target_schema = os.getenv('TARGET_SCHEMA')
fetch_size = int(os.getenv('FETCH_SIZE', 1000))
fetch_timeout = int(os.getenv('FETCH_TIMEOUT'))

#elasticsearch configuration
elastic_host = os.getenv('ELASTIC_HOST')
elastic_user = os.getenv('ELASTIC_USER')
elastic_password = os.getenv('ELASTIC_PASSWORD')

#logs and storage configuration
logs_path = os.getenv('LOGS_PATH', './logs/etl.log')
logging_level = os.getenv('LOGGING_LEVEL', 'DEBUG')
logger = Logger(logs_path, logging_level)

storage_path = os.getenv('STORAGE_PATH')