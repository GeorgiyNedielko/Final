
def print_films(results, group_by='year', start_index=1):
    """
    Печатает список фильмов в таблице, сгруппированной по году или жанру.
    """
    if not results:
        print("Ничего не найдено.")
        return

    last_index = start_index + len(results) - 1
    idx_width = max(2, len(str(last_index)))

    max_title = max((len(str(r.get('title') or '')) for r in results), default=0)
    max_genre = max((len(str(r.get('genre') or '')) for r in results), default=0)
    max_rating = max((len(str(r.get('rating') or '')) for r in results), default=0)

    title_w = max(10, max_title)
    genre_w = max(6, max_genre)
    rating_w = max(3, max_rating)

    fmt = f"{{idx:>{idx_width}}}. {{title:<{title_w}}}  — жанр: {{genre:<{genre_w}}}  — рейтинг: {{rating:<{rating_w}}}"

    print(f" №  {'Название фильма':<{title_w}}  Жанр{' ' * (genre_w - 4)}  Рейтинг")
    print('-' * (idx_width + 3 + title_w + genre_w + rating_w + 12))

    def print_grouped(key):
        current = None
        for offset, film in enumerate(results):
            i = start_index + offset
            if film.get(key) != current:
                current = film.get(key)
                print(f"\n{key.capitalize()}: {current}")
            print(fmt.format(idx=i, title=film.get('title', ''), genre=film.get('genre', ''), rating=film.get('rating', '')))

    if group_by in ('year', 'genre'):
        print_grouped('release_year' if group_by == 'year' else 'genre')
    else:
        for offset, film in enumerate(results):
            i = start_index + offset
            print(fmt.format(idx=i, title=film.get('title', ''), genre=film.get('genre', ''), rating=film.get('rating', '')))


def select_film(cursor, results, offset=0):
    """
    Позволяет пользователю выбрать фильм по номеру или названию и показать его детали.
    """
    if not results:
        return

    id_map = {str(offset + idx + 1): film['film_id'] for idx, film in enumerate(results)}
    title_map = {film['title'].lower(): film['film_id'] for film in results}

    while True:
        choice = input("\nВведите номер или название фильма для просмотра деталей (Enter — выход): ").strip()
        if not choice:
            return

        film_id = id_map.get(choice) or title_map.get(choice.lower())
        if film_id:
            show_film_details(cursor, film_id)
            return  # не возвращаемся к списку
        else:
            print("Фильм не найден. Попробуйте снова.")


def show_film_details(cursor, film_id):
    """
    Выводит подробности по ID фильма.
    """
    query = """
        SELECT f.title, f.description, f.release_year, f.rating, c.name AS genre
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        WHERE f.film_id = %s;
    """
    cursor.execute(query, (film_id,))
    film = cursor.fetchone()

    if not film:
        print("Фильм не найден.")
        return

    print(f"\nНазвание: {film['title']} ({film['release_year']})")
    print('-' * 50)
    print(f"Описание: {film['description']}")
    print(f"Жанр: {film['genre']}")
    print(f"Рейтинг: {film['rating']}")
    print(f"Год: {film['release_year']}")