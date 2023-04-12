import sqlite3
from contextlib import contextmanager, closing

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from data_extractor import SQLiteExtractor
from data_saver import PostgresSaver
from config import PostgresSettings

postgres_settings = PostgresSettings()


@contextmanager
def sqlite_conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    for rows in sqlite_extractor.extract_movies():
        postgres_saver.save_data(rows)


def create_schema(pg_conn: _connection):

    cur = pg_conn.cursor()
    try:
        with open('movies_database.ddl', 'r') as f:
            sql = f.read()
        cur.execute(sql)
    except Exception as e:
        print(e)
    finally:
        cur.close()


if __name__ == '__main__':
    with sqlite_conn_context('db.sqlite') as sqlite_conn, \
            closing(psycopg2.connect(**postgres_settings.dict(), cursor_factory=DictCursor)) as pg_conn:

        create_schema(pg_conn)

        load_from_sqlite(sqlite_conn, pg_conn)
        pg_conn.commit()
        print('all data loaded')
