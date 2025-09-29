from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime
import pytz
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
            print(f"Ошибка при логировании в MongoDB: {e}")


import pytz

def show_most_popular_queries(db):
    print("\nТоп 10 популярных запросов:")

    pipeline = [
        {"$group": {
            "_id": {"type": "$type", "parameters": "$parameters"},
            "count": {"$sum": 1},
            "avg_duration": {"$avg": "$duration_sec"},
            "total_results": {"$sum": "$result_count"},
            "last_time": {"$max": "$timestamp"}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]

    try:
        results = list(db[MONGO_CONFIG['collection_name']].aggregate(pipeline))

        if not results:
            print("Нет данных для отображения.")
            return

        local_tz = pytz.timezone("Europe/Moscow")

        for i, entry in enumerate(results, start=1):
            query_type = entry['_id']['type']
            params = entry['_id']['parameters']
            count = entry['count']
            avg_duration = round(entry['avg_duration'], 3)
            total_results = entry['total_results']
            utc_time = entry['last_time']
            local_time = utc_time.replace(tzinfo=pytz.utc).astimezone(local_tz)
            formatted_time = local_time.strftime('%d-%m-%Y %H:%M:%S')

            print(f"\n{i}. Тип: {query_type}")
            print(f"   Параметры: {params}")
            print(f"   Количество запросов: {count}")
            print(f"   Общее количество результатов: {total_results}")
            print(f"   Среднее время выполнения: {avg_duration:.3f} сек")
            print(f"   Последний раз: {formatted_time}")

            # Дополнительная информация
            summary = []
            if 'keyword' in params:
                summary.append(f"Ключевое слово: {params['keyword']}")
            if 'genre' in params:
                summary.append(f"Жанр: {params['genre']}")
            if 'year_from' in params and 'year_to' in params:
                summary.append(f"Годы: {params['year_from']}-{params['year_to']}")
            if 'rating' in params:
                summary.append(f"Рейтинг: {params['rating']}")

            if summary:
                print("   Доп. информация:", "; ".join(summary))

    except Exception as e:
        print(f"Ошибка при получении популярных запросов: {e}")

def show_last_unique_queries(mongo_db):
    print("\nПоследние уникальные запросы:")

    if mongo_db is None:
        print("Нет подключения к MongoDB.")
        return

    try:
        logs_collection = mongo_db[MONGO_CONFIG['collection_name']]

        pipeline = [
            {"$sort": {"timestamp": -1}},
            {"$group": {
                "_id": {"type": "$type", "parameters": "$parameters"},
                "timestamp": {"$first": "$timestamp"},
                "result_count": {"$first": "$result_count"}
            }},
            {"$sort": {"timestamp": -1}},
            {"$limit": 10}
        ]

        results = list(logs_collection.aggregate(pipeline))

        if not results:
            print("Нет данных.")
            return

        local_tz = pytz.timezone("Europe/Moscow")

        for i, log in enumerate(results, 1):
            query_type = log["_id"]["type"]
            parameters = log["_id"]["parameters"]
            timestamp_utc = log["timestamp"]
            result_count = log.get("result_count", 0)

            timestamp_local = timestamp_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)
            formatted_time = timestamp_local.strftime("%d-%m-%Y %H:%M:%S")

            print(f"{i}. [{formatted_time}] {query_type.upper()} — {parameters} → {result_count} результатов")

    except Exception as e:
        print(f"Ошибка при выводе последних уникальных запросов: {e}")

