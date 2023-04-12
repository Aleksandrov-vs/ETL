from typing import List
from contextlib import closing

import sqlite3
import dotenv
import os

from models import Genre, Filmwork, Person, PersonFilmwork, GenreFilmwork
from dataclasses import dataclass

dotenv.load_dotenv()

COUNT_ROWS_FOR_LOADING = int(os.getenv('COUNT_ROWS_FOR_LOADING'))


class SQLiteExtractor:
    data = dict
    conn: sqlite3.Connection

    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self.conn.row_factory = sqlite3.Row

    def validation(self, table_name, new_rows) -> List[dataclass]:
        model_table_name_mapping = {
            "genre": Genre,
            'person': Person,
            'film_work': Filmwork,
            'genre_film_work': GenreFilmwork,
            'person_film_work': PersonFilmwork
        }
        model = model_table_name_mapping.get(table_name)
        return list(map(lambda x: model(**x), new_rows))

    def extract_rows(self, table_name: str):
        with closing(self.conn.cursor()) as curs:
            query = f"SELECT * FROM {table_name}"
            curs.execute(query)
            while True:
                new_rows = curs.fetchmany(COUNT_ROWS_FOR_LOADING)
                new_valid_rows = self.validation(table_name, new_rows)
                if len(new_valid_rows) > 0:
                    yield new_valid_rows
                else:
                    return None

    def extract_data(self) -> List[dataclass]:
        table_names = [
            'genre',
            'film_work',
            'person',
            'genre_film_work',
            'person_film_work'
        ]
        for table_name in table_names:
            for extracted_rows in self.extract_rows(table_name):
                if extracted_rows is not None:
                    yield extracted_rows

        return

    def extract_movies(self):
        for new_rows in self.extract_data():
            if len(new_rows) == 0:
                return None
            else:
                yield new_rows
