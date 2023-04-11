import datetime
import os
from contextlib import closing
from typing import List, Union
from uuid import UUID

import backoff as backoff
import dotenv
import psycopg2
from psycopg2 import OperationalError, sql
from psycopg2.extras import RealDictConnection, RealDictRow
from psycopg2.sql import Composed
from redis import connection as redis_connection

dotenv.load_dotenv()

dsl = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT'),
}

BACKOFF_CONF = {
    'max_time': int(os.getenv('MAX_TIME_BACKOFF')),
    'max_tries': int(os.getenv('MAX_TRIES_BACKOFF'))
}


class BasePostgresExtractor:

    def __init__(self, redis_conn: redis_connection):
        self.redis_conn = redis_conn

        # максимальное значение modified в пачке данных которые будут загружены в elastic
        self.max_modified_in_bundle = None
        # максимальное значение modified в данных которое уже загружено в elastic
        self.last_modified_in_elastic = None
        self.state_key_name = ''

    def create_state(self, table_name: str):
        """
        если в redis нет нужных state-> создаем
        дальше добавляем атрибут с значением ко всем, кто наследуется
        :return:
        """
        last_modif = self.redis_conn.get(self.state_key_name)

        if last_modif is None:
            self.last_modified_in_elastic = self.get_earliest_time(
                table_name
            )
            self.redis_conn.set(
                self.state_key_name,
                str(self.last_modified_in_elastic)
            )
        else:
            self.last_modified_in_elastic: Union[datetime, str] = datetime.\
                datetime.strptime(
                last_modif.decode("utf-8"),
                '%Y-%m-%d %H:%M:%S.%f%z'
            )

    @backoff.on_exception(
        backoff.expo,
        (OperationalError, ),
        **BACKOFF_CONF
    )
    def bundle_extract_rows(
            self, query: str, values: List, count_row: int = 10
    ):
        """

        :param query: sql запрос
        :param values: значения, которые надо подставить
        :param count_row: количество строк в одной пачке
        :return:
        """
        with closing(
            psycopg2.connect(**dsl, connection_factory=RealDictConnection)
        ) as pg_conn:
            curs = pg_conn.cursor()
            try:
                curs.execute(query, values)
                while True:
                    rows: list[RealDictRow] = curs.fetchmany(count_row)
                    if len(rows) > 0:
                        yield rows
                    else:
                        return None
            except Exception as e:
                raise e
            finally:
                curs.close()

    @backoff.on_exception(
        backoff.expo,
        (OperationalError,),
        **BACKOFF_CONF
    )
    def run_sql_query(self, query: Union[str, Composed], values: list):
        """

        :param query: sql запрос
        :param values: значения, которые надо подставить
        :return:
        """
        with closing(
            psycopg2.connect(**dsl, connection_factory=RealDictConnection)
        ) as pg_conn:
            curs = pg_conn.cursor()
            try:
                curs.execute(query, values)
                result: list[RealDictRow] = curs.fetchall()
                return result
            except Exception as e:
                raise e
            finally:
                curs.close()

    def get_new_rows(self, table_name: str, limit_value: int = 100) \
            -> Union[List[RealDictRow], None]:
        """
        достаем пачку данных из таблицы, отсортированной по полю modified
        :param table_name: название таблицы из которой извлекаем строки
        :param limit_value: ограничение
        :return: возвращаем None, если новых данных нет
        """
        query = sql.SQL(
            """
            SELECT id, modified
            FROM {table_name}
            WHERE modified > %s
            ORDER BY modified
            limit {limit_value};
            """
        ).format(
            table_name=sql.Identifier('content', table_name),
            limit_value=sql.Literal(limit_value)
        )
        new_rows = self.run_sql_query(query, [self.last_modified_in_elastic])
        if len(new_rows) > 0:
            self.max_modified_in_bundle = max(
                list(
                    map(lambda g: g['modified'], new_rows)
                )
            )
            return new_rows
        else:
            return None

    def get_film_work_ids_for_update_base(
            self, new_entitys: List[RealDictRow],
            table_name: str, m2m_table_name: str,
            field_to_join: str
            ) -> List[UUID]:
        """

        :param new_entitys: набор строк из таблицы (например из таблицы person)
        :param table_name: название таблицы
        :param m2m_table_name: название таблицы для join
        :param field_to_join: поле для join
        :return:
        """
        new_ids = list(map(lambda p: p['id'], new_entitys))
        query = sql.SQL(
            """
                SELECT m2m.film_work_id, ent.modified
                FROM {table_name} ent
                LEFT JOIN {m2m_table_name} m2m ON m2m.{field_to_join} = ent.id
                WHERE ent.id IN ({new_id})
                ORDER BY ent.modified;
            """
        ).format(
            table_name=sql.Identifier('content', table_name),
            m2m_table_name=sql.Identifier('content', m2m_table_name),
            field_to_join=sql.Identifier(field_to_join),
            new_id=sql.SQL(',').join(sql.Literal(u) for u in new_ids)
        )

        rows = self.run_sql_query(query, [])
        return list(map(lambda row: row['film_work_id'], rows))

    def get_earliest_time(self, table_name: str):
        query = sql.SQL(
            "SELECT modified from {table_name} order by modified limit 1;"
        ).format(
            table_name=sql.Identifier('content', table_name),
        )
        return self.run_sql_query(query, [])[0]['modified']

    def get_film_works_data(self, film_work_ids: List[UUID]):
        placeholders = ','.join(['%s'] * len(film_work_ids))
        query = f"""
            SELECT
               fw.id,
               fw.title,
               fw.description,
               fw.rating,
               fw.type,
               fw.created,
               fw.modified,
               COALESCE (
                   json_agg(
                       DISTINCT jsonb_build_object(
                           'person_role', pfw.role,
                           'person_id', p.id,
                           'person_name', p.full_name
                       )
                   ) FILTER (WHERE p.id is not null),
                   '[]'
               ) as persons,
               array_agg(DISTINCT g.name) as genres
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN ({placeholders})
            GROUP BY fw.id
            ORDER BY fw.modified;
        """
        return self.bundle_extract_rows(query, film_work_ids)

    def update_state(self):
        self.redis_conn.set(
            self.state_key_name,
            str(self.max_modified_in_bundle)
        )
        self.last_modified_in_elastic = self.max_modified_in_bundle
