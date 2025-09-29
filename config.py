import os
from dotenv import load_dotenv

load_dotenv()  # Загружаем переменные окружения из .env

MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DB'),
}

MONGO_CONFIG = {
    'uri': os.getenv('MONGO_URI'),
    'db_name': os.getenv('MONGO_DB'),
    'collection_name': os.getenv('MONGO_COLLECTION'),
}
