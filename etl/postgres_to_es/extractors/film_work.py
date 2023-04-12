
from typing import List
from uuid import UUID

import dotenv
from redis import connection as redis_connection

from .base import BasePostgresExtractor
from storages import State


dotenv.load_dotenv()


class FilmworkExtractor(BasePostgresExtractor):

    def __init__(self, state: State):
        super().__init__(state)
        self.state_key_name = 'fw_modified'

        super().create_state('film_work')

    def get_film_work_ids_for_update(self) -> List[UUID] | None:
        new_film_work = super().get_new_rows('film_work', 100)
        if new_film_work is None:
            return None
        else:
            new_film_work_ids = list(map(lambda fw: fw['id'], new_film_work))
            return new_film_work_ids

    def extract_filmwork(self):
        film_works_ids_for_update = self.get_film_work_ids_for_update()
        if film_works_ids_for_update is None:
            return [None]
        return self.get_film_works_data(film_works_ids_for_update)
