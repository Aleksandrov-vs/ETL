import os
import sqlite3
import sys
from contextlib import contextmanager, closing
from pathlib import Path

import dotenv
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from sqlite_to_postgres.models import Genre, Person, Filmwork, \
    PersonFilmwork, GenreFilmwork

dotenv.load_dotenv()


@contextmanager
def sqlite_conn_context(db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()

class Checker:
    sqlite_connect: sqlite3.connect
    pg_connect: psycopg2.connect

    def __init__(self, sqlite_connect: sqlite3.Connection,
                 pg_connect: _connection) -> None:
        self.sqlite_connect = sqlite_connect
        self.sqlite_connect.row_factory = sqlite3.Row
        self.pg_connect = pg_connect
        self.pg_connect = pg_connect
        with pg_connect.cursor() as pg_curs:
            pg_curs.execute('SET search_path TO content,public;')

    def check_count_records(self, table_name):
        query = f"SELECT count(*) FROM {table_name};"

        sqlite_curs = self.sqlite_connect.cursor()
        sqlite_curs.execute(query)
        sqlite_count_rows = sqlite_curs.fetchone()[0]

        pg_curs = self.pg_connect.cursor()
        pg_curs.execute(query)
        pg_count_rows = pg_curs.fetchone()[0]
        pg_curs.close()

        assert pg_count_rows == sqlite_count_rows, \
            f'разное количество строк в таблицах\n' \
            f'таблица: {table_name}\n' \
            f'кол-во строк в postgres: {pg_count_rows}\n' \
            f'кол-во строк в sqlite: {sqlite_count_rows}'

    def get_rows_from_table(self, table_name):
        query = f"SELECT * FROM {table_name};"
        pg_cursor = self.pg_connect.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        pg_cursor.execute(query)
        while True:
            result = pg_cursor.fetchone()
            if result is not None:
                yield result
            else:
                return

    def check_genre_data(self):
        for row in self.get_rows_from_table('genre'):
            genre_in_pg = Genre(id=row['id'], name=row['name'],
                                description=row['description'],
                                created_at=row['created'],
                                updated_at=row['modified'])
            sqlite_cursor = self.sqlite_connect.cursor()
            sqlite_cursor.execute(
                'SELECT * FROM genre WHERE id=?;', (genre_in_pg.id,)
            )
            genre_in_sqlite = Genre(**sqlite_cursor.fetchone())
            assert genre_in_sqlite == genre_in_pg, \
                f'значение из таблицы  genre не совпадают\n' \
                f'значение в postgres: {genre_in_pg}\n' \
                f'значение в sqlite: {genre_in_sqlite}\n'

    def check_person_data(self):
        for row in self.get_rows_from_table('person'):
            person_in_pg = Person(id=row['id'], full_name=row['full_name'],
                                  created_at=row['created'],
                                  updated_at=row['modified'])
            sqlite_cursor = self.sqlite_connect.cursor()
            sqlite_cursor.execute(
                'SELECT * FROM person WHERE id=?;', (person_in_pg.id,)
            )
            person_in_sqlite = Person(**sqlite_cursor.fetchone())
            assert person_in_sqlite == person_in_pg, \
                f'значение из таблицы  person не совпадают\n' \
                f'значение в postgres: {person_in_pg}\n' \
                f'значение в sqlite: {person_in_sqlite}\n'

    def check_filmwork_data(self):
        for row in self.get_rows_from_table('film_work'):
            filmwork_in_pg = Filmwork(id=row['id'], title=row['title'],
                                      description=row['description'],
                                      creation_date=row['creation_date'],
                                      rating=row['rating'],
                                      type=row['type'],
                                      created_at=row['created'],
                                      updated_at=row['modified']
                                      )
            sqlite_cursor = self.sqlite_connect.cursor()
            sqlite_cursor.execute(
                'SELECT * FROM film_work WHERE id=?;',
                (filmwork_in_pg.id,)
            )
            filmwork_in_sqlite = Filmwork(**sqlite_cursor.fetchone())
            sqlite_cursor.close()
            assert filmwork_in_sqlite == filmwork_in_pg, \
                f'значение из таблицы  film_work не совпадают\n' \
                f'значение в postgres: {filmwork_in_pg}\n' \
                f'значение в sqlite: {filmwork_in_sqlite}\n'

    def check_genre_filmwork_data(self):
        for row in self.get_rows_from_table('genre_film_work'):
            genre_filmwork_in_pg = GenreFilmwork(
                id=row['id'],
                genre_id=row['genre_id'],
                film_work_id=row['film_work_id'],
                created_at=row['created']
            )
            sqlite_cursor = self.sqlite_connect.cursor()
            sqlite_cursor.execute(
                'SELECT * FROM genre_film_work WHERE id=?;',
                (genre_filmwork_in_pg.id,)
            )
            genre_filmwork_in_sqlite = GenreFilmwork(
                **sqlite_cursor.fetchone()
            )
            assert genre_filmwork_in_sqlite == genre_filmwork_in_pg, \
                f'значение из таблицы  genre_film_work не совпадают\n' \
                f'значение в postgres: {genre_filmwork_in_pg}\n' \
                f'значение в sqlite: {genre_filmwork_in_sqlite}\n'

    def check_person_filmwork_data(self):
        for row in self.get_rows_from_table('person_film_work'):
            person_filmwork_in_pg = PersonFilmwork(
                id=row['id'],
                person_id=row['person_id'],
                film_work_id=row['film_work_id'],
                role=row['role'],
                created_at=row['created']
            )
            sqlite_cursor = self.sqlite_connect.cursor()
            sqlite_cursor.execute(
                'SELECT * FROM person_film_work WHERE id=?;',
                (person_filmwork_in_pg.id,)
            )
            person_filmwork_in_sqlite = PersonFilmwork(
                **sqlite_cursor.fetchone()
            )
            assert person_filmwork_in_sqlite == person_filmwork_in_pg, \
                f'значение из таблицы  person_film_work не совпадают\n' \
                f'значение в postgres: {person_filmwork_in_pg}\n' \
                f'значение в sqlite: {person_filmwork_in_sqlite}\n'
            sqlite_cursor.close()

    def check_data_compliance(self, table_name):
        if table_name == 'genre':
            self.check_genre_data()
        if table_name == 'person':
            self.check_person_data()
        if table_name == 'film_work':
            self.check_filmwork_data()
        if table_name == 'person_film_work':
            self.check_person_filmwork_data()
        if table_name == 'genre_film_work':
            self.check_genre_filmwork_data()

    def check(self):
        table_names = [
            'genre',
            'film_work',
            'person',
            'person_film_work',
            'genre_film_work',
        ]
        for table_name in table_names:
            self.check_count_records(table_name)
            self.check_data_compliance(table_name)


def test_load_data():
    dsl = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT')
    }

    sqlite_path = Path(sys.path[0]).joinpath('sqlite_to_postgres/db.sqlite')
    with sqlite_conn_context(sqlite_path) as sqlite_conn, \
            closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
        checker = Checker(sqlite_conn, pg_conn)
        checker.check()
