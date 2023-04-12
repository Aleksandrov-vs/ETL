"""Класс для сохранения данных в postgres"""
from dataclasses import asdict
from typing import List, Union

from psycopg2 import sql
from psycopg2.extensions import connection as _connection
from models import Genre, GenreFilmwork, PersonFilmwork, Person, Filmwork

AnyModelType = Union[Genre, GenreFilmwork, PersonFilmwork, Person, Filmwork]


class PostgresSaver:
    def __init__(self, pg_connect: _connection):
        self.pg_conn = pg_connect

    def insert_to_database(self, query: str, values: list):
        curs = self.pg_conn.cursor()
        try:
            result = curs.execute(query, values)
        except Exception as e:
            print(e)
            raise e
        finally:
            curs.close()

    def create_data_for_query(self, model: AnyModelType):
        values_list = []
        column_name_list = []
        model_table_name_mapping = {
            "genre": Genre,
            'person': Person,
            'film_work': Filmwork,
            'genre_film_work': GenreFilmwork,
            'person_film_work': PersonFilmwork
        }
        for key, value in model_table_name_mapping.items():
            if isinstance(model, value):
                table_name = key

        for key, value in asdict(model).items():
            if key == 'created_at':
                key = 'created'
            if key == 'updated_at':
                key = 'modified'
            if key == 'file_path':
                continue
            column_name_list.append(key)
            values_list.append(value)
        return table_name, values_list, column_name_list

    def create_insert_query(self, model: AnyModelType):
        table_name, values_list, column_list = self.create_data_for_query(model)
        values_type = ', '.join(['%s' for i in range(len(values_list))])
        query = sql.SQL(
            "INSERT INTO {table_name} ({column_name})" +
            f" VALUES({values_type}) " +
            "ON CONFLICT (id) DO NOTHING;"
        ).format(
            table_name=sql.Identifier('content', table_name),
            column_name=sql.SQL(', ').join(map(sql.Identifier, column_list)),
        )
        return query, values_list

    def save_data(self, rows: List[AnyModelType]) -> None:
        for row in rows:
            query, values = self.create_insert_query(row)
            self.insert_to_database(query, values)
