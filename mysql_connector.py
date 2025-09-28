
import pymysql
from config import MYSQL_CONFIG

def connect_mysql():
    try:
        connection = pymysql.connect(
            host=MYSQL_CONFIG['host'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            database=MYSQL_CONFIG['database'],
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except pymysql.MySQLError as e:
        print(f"Ошибка подключения к MySQL: {e}")
        return None