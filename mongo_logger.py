from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime
from config import MONGO_CONFIG


def connect_mongo():
    """Подключение к MongoDB по URI."""
    try:
        client = MongoClient(MONGO_CONFIG['uri'], serverSelectionTimeoutMS=5000)
        db = client[MONGO_CONFIG['db_name']]
        return db
    except PyMongoError as e:
        print(f" Ошибка подключения к MongoDB: {e}")
        return None


def log_query(mongo_db, query_type, parameters, result_count, duration):
    """
    Запись запроса в MongoDB.
    :param mongo_db: подключение к базе
    :param query_type: тип запроса (title, rating, genre_year, etc.)
    :param parameters: параметры поиска (dict)
    :param result_count: количество найденных фильмов
    :param duration: время выполнения запроса в секундах
    """
    if mongo_db is not None:
        try:
            logs_collection = mongo_db[MONGO_CONFIG['collection_name']]
            logs_collection.insert_one({
                "type": query_type,
                "parameters": parameters,
                "result_count": result_count,
                "duration_sec": round(duration, 4),
                "timestamp": datetime.utcnow()
            })
        except Exception as e:
            print(f" Ошибка при логировании в MongoDB: {e}")


def show_most_popular_queries(mongo_db):
    """Вывод самых популярных типов запросов."""
    print("\n Топ популярных типов запросов:")
    if mongo_db is None:
        print("MongoDB не подключена.")
        return

    try:
        logs_collection = mongo_db[MONGO_CONFIG['collection_name']]
        pipeline = [
            {"$group": {"_id": "$type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        results = logs_collection.aggregate(pipeline)
        for r in results:
            print(f"- {r['_id']}: {r['count']} запросов")
    except Exception as e:
        print(f" Ошибка при получении статистики: {e}")


def show_last_unique_queries(mongo_db):
    """Вывод последних 10 уникальных запросов."""
    print("\n Последние уникальные запросы:")
    if mongo_db is None:
        print("MongoDB не подключена.")
        return

    try:
        logs_collection = mongo_db[MONGO_CONFIG['collection_name']]
        cursor = logs_collection.find().sort("timestamp", -1).limit(10)
        for log in cursor:
            t = log.get('type', '???')
            p = log.get('parameters', {})
            r = log.get('result_count', 0)
            ts = log.get('timestamp').strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] {t.upper()} — {p} → {r} результатов")
    except Exception as e:
        print(f" Ошибка при выводе последних запросов: {e}")
