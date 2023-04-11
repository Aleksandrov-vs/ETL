from typing import List, Union
from uuid import UUID

from psycopg2.extras import RealDictRow
from redis import connection as redis_connection

from .base import BasePostgresExtractor


class GenreExtractor(BasePostgresExtractor):

    def __init__(self, redis_conn: redis_connection):
        super().__init__(redis_conn)
        self.state_key_name = 'g_modified'

        super().create_state()

    def get_new_genres(self) -> Union[List[RealDictRow]]:
        return super().get_new_rows('genre', limit_value=10)

    def get_film_work_ids_for_update(self, new_genre) -> List[UUID]:
        return super().get_film_work_ids_for_update_base(
            new_genre, 'genre',
            'genre_film_work', 'genre_id'
        )

    def extract_genres(self):
        new_genres: List[RealDictRow] = self.get_new_genres()
        if new_genres is None:
            return [None]
        film_works_ids_for_update = self.get_film_work_ids_for_update(
            new_genres
        )
        print(film_works_ids_for_update)
        return self.get_film_works_data(film_works_ids_for_update)
