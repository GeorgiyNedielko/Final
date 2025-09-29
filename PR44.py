import pymysql
from config import MYSQL_CONFIG
from mongo_logger import connect_mongo, log_query, show_most_popular_queries, show_last_unique_queries
import time

from formatter import print_films, select_film, show_film_details  # импорт из formatter.py


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


def search_by_title(cursor, mongo_db):
    while True:
        keyword = input("Введите ключевое слово для поиска в названии фильма (или 'b' для возврата): ").strip()
        if keyword.lower() in ('b', 'back', 'q'):
            return
        if not keyword:
            print("Пустой ввод. Попробуйте снова.")
            continue

        start_time = time.time()

        query = """
            SELECT f.film_id, f.title, f.release_year, f.rating, c.name AS genre
            FROM film f
            JOIN film_category fc ON f.film_id = fc.film_id
            JOIN category c ON fc.category_id = c.category_id
            WHERE f.title LIKE %s
            ORDER BY f.release_year, f.title;
        """
        cursor.execute(query, (f"%{keyword}%",))
        results = cursor.fetchall()

        duration = time.time() - start_time

        try:
            if mongo_db is not None:
                log_query(mongo_db, "title", {"keyword": keyword}, len(results), duration)
        except Exception as e:
            print(f"Ошибка логирования запроса в MongoDB: {e}")

        if results:
            print(f"\nНайдено {len(results)} фильмов по запросу '{keyword}':")
            print_films(results, group_by='year')
            select_film(cursor, results)
        else:
            print(f"Ничего не найдено по запросу '{keyword}'.")

        again = input("\nВыполнить ещё поиск в этом меню? (y — да, любая другая — возврат): ").strip().lower()
        if again != 'y':
            return


def search_by_genre_and_year(cursor, mongo_db):
    cursor.execute("SELECT DISTINCT name FROM category ORDER BY name;")
    genres = [row['name'] for row in cursor.fetchall()]

    print("\nДоступные жанры:")
    print(', '.join(genres))

    cursor.execute("SELECT MIN(release_year) AS min_year, MAX(release_year) AS max_year FROM film;")
    year_range = cursor.fetchone()
    min_year, max_year = year_range['min_year'], year_range['max_year']

    print(f"\nДоступный диапазон лет: от {min_year} до {max_year}\n")

    while True:
        genre = input("Введите жанр (или 'b' для возврата): ").strip()
        if genre.lower() in ('b', 'back', 'q'):
            return

        year_from = input("Введите начальный год: ").strip()
        year_to = input("Введите конечный год: ").strip()

        if not genre or not year_from or not year_to:
            print("Жанр и оба года обязательны. Попробуйте снова.\n")
            continue

        if not (year_from.isdigit() and year_to.isdigit()):
            print("Годы должны быть числовыми. Попробуйте снова.\n")
            continue

        year_from, year_to = int(year_from), int(year_to)

        if year_from > year_to:
            print("Начальный год не может быть больше конечного. Попробуйте снова.\n")
            continue

        if year_from < min_year or year_to > max_year:
            print(f"Годы должны быть в диапазоне от {min_year} до {max_year}. Попробуйте снова.\n")
            continue

        start_time = time.time()

        query = """
                SELECT f.film_id, f.title, f.release_year, f.rating, c.name AS genre
                FROM film f
                         JOIN film_category fc ON f.film_id = fc.film_id
                         JOIN category c ON fc.category_id = c.category_id
                WHERE c.name LIKE %s
                  AND f.release_year BETWEEN %s AND %s
                ORDER BY f.release_year, f.title;
                """
        cursor.execute(query, (f"%{genre}%", year_from, year_to))
        results = cursor.fetchall()

        duration = time.time() - start_time

        try:
            if mongo_db is not None:
                log_query(mongo_db, "genre_year", {"genre": genre, "year_from": year_from, "year_to": year_to},
                          len(results), duration)
        except Exception as e:
            print(f"Ошибка логирования запроса в MongoDB: {e}")

        if results:
            print_films(results, group_by='year')
            select_film(cursor, results)
        else:
            print("Ничего не найдено по заданным параметрам.")

        again = input("\nИскать снова? (y — да, любая другая — возврат в меню): ").strip().lower()
        if again != 'y':
            return


def show_films_with_pagination(cursor, mongo_db):
    page_size = 10
    page = 0

    while True:
        offset = page * page_size
        start_time = time.time()

        query = """
            SELECT f.film_id, f.title, f.release_year, f.rating, c.name AS genre
            FROM film f
            JOIN film_category fc ON f.film_id = fc.film_id
            JOIN category c ON fc.category_id = c.category_id
            ORDER BY f.release_year, f.title
            LIMIT %s OFFSET %s;
        """
        cursor.execute(query, (page_size, offset))
        results = cursor.fetchall()

        duration = time.time() - start_time

        try:
            if mongo_db is not None:
                log_query(mongo_db, "pagination", {"page_size": page_size, "page": page + 1}, len(results), duration)
        except Exception as e:
            print(f"Ошибка логирования запроса в MongoDB: {e}")

        if not results:
            print("Достигнут конец списка фильмов.")
            return

        print_films(results, group_by='year', start_index=offset + 1)

        print("\nНавигация:")
        print("Введите номер фильма для просмотра деталей.")
        print("n - следующая страница")
        print("p - предыдущая страница")
        print("b - назад в главное меню")

        action = input("Выберите действие: ").strip().lower()

        if action == 'n':
            page += 1
        elif action == 'p':
            if page > 0:
                page -= 1
            else:
                print("Это первая страница.")
        elif action == 'b':
            return
        elif action.isdigit():
            idx = int(action) - (offset + 1)
            if 0 <= idx < len(results):
                film_id = results[idx]['film_id']
                try:
                    show_film_details(cursor, film_id)
                except Exception as e:
                    print(f"Ошибка при показе деталей фильма: {e}")
                # После показа деталей — НЕ выводим список повторно, просто ждем следующее действие
                input("\nНажмите Enter для продолжения...")
            else:
                print("Неверный номер фильма.")
        else:
            print("Некорректный ввод. Попробуйте снова.")


def search_by_rating(cursor, mongo_db):
    print("\nДоступные рейтинги: G, PG, PG-13, R, NC-17")
    valid_ratings = {'G', 'PG', 'PG-13', 'R', 'NC-17'}

    while True:
        rating = input("Введите рейтинг (или 'b' для возврата): ").strip().upper()
        if rating.lower() in ('b', 'back', 'q'):
            return

        if rating not in valid_ratings:
            print("Недопустимый рейтинг. Попробуйте снова.")
            continue

        start_time = time.time()

        query = """
            SELECT f.film_id, f.title, f.release_year, f.rating, c.name AS genre
            FROM film f
            JOIN film_category fc ON f.film_id = fc.film_id
            JOIN category c ON fc.category_id = c.category_id
            WHERE f.rating = %s
            ORDER BY f.release_year, f.title;
        """
        cursor.execute(query, (rating,))
        results = cursor.fetchall()

        duration = time.time() - start_time

        try:
            if mongo_db is not None:
                log_query(mongo_db, "rating", {"rating": rating}, len(results), duration)
        except Exception as e:
            print(f"Ошибка логирования запроса в MongoDB: {e}")

        if results:
            print_films(results, group_by='year')
            select_film(cursor, results)
        else:
            print(f"Фильмы с рейтингом {rating} не найден")

        again = input("\nИскать снова? (y — да, любая другая — возврат): ").strip().lower()
        if again != 'y':
            return


def main():
    mysql_conn = connect_mysql()
    if mysql_conn is None:
        print("Не удалось подключиться к MySQL. Завершение работы.")
        return

    mongo_db = connect_mongo()
    if mongo_db is None:
        print("Не удалось подключиться к MongoDB. Логирование не будет работать.")
        # Явно указываем None, чтобы избежать ошибок при проверках
        mongo_db = None

    try:
        with mysql_conn:
            with mysql_conn.cursor() as cursor:
                while True:
                    print("\nМеню:")
                    print("1. Поиск фильма по названию")
                    print("2. Поиск фильма по жанру и диапазону годов")
                    print("3. Вывести все фильмы с постраничным просмотром")
                    print("4. Поиск фильма по рейтингу")
                    print("5. Показать 10 самых популярных запросов")
                    print("6. Показать 10 последних уникальных запросов")
                    print("0. Выход")

                    choice = input("Выберите действие: ").strip()

                    if choice == '1':
                        search_by_title(cursor, mongo_db)
                    elif choice == '2':
                        search_by_genre_and_year(cursor, mongo_db)
                    elif choice == '3':
                        show_films_with_pagination(cursor, mongo_db)
                    elif choice == '4':
                        search_by_rating(cursor, mongo_db)
                    elif choice == '5':
                        if mongo_db is not None:
                            try:
                                show_most_popular_queries(mongo_db)
                            except Exception as e:
                                print(f"Ошибка при получении популярных запросов: {e}")
                        else:
                            print("Нет подключения к MongoDB для отображения статистики.")
                    elif choice == '6':
                        if mongo_db is not None:
                            try:
                                show_last_unique_queries(mongo_db)
                            except Exception as e:
                                print(f"Ошибка при получении последних уникальных запросов: {e}")
                        else:
                            print("Нет подключения к MongoDB для отображения статистики.")
                    elif choice == '0':
                        print("Выход из программы.")
                        break
                    else:
                        print("Некорректный выбор. Попробуйте снова.")
    except Exception as e:
        print(f"Произошла ошибка в работе программы: {e}")




if __name__ == "__main__":
    main()
