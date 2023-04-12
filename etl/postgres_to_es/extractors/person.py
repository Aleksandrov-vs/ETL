from typing import List
from uuid import UUID

import dotenv
from psycopg2.extras import RealDictRow

from storages import State

from .base import BasePostgresExtractor

dotenv.load_dotenv()


class PersonExtract(BasePostgresExtractor):

    def __init__(self, state: State):
        super().__init__(state)
        self.state_key_name = 'p_modified'
        super().create_state('person')

    def get_new_persons(self):
        return super().get_new_rows('person', limit_value=100)

    def get_film_work_ids_for_update(self, new_persons: List[RealDictRow])\
            -> List[UUID]:
        return super().get_film_work_ids_for_update_base(
            new_persons, 'person',
            'person_film_work', 'person_id'
        )

    def extract_persons(self):
        new_persons: List[RealDictRow] = self.get_new_persons()
        if new_persons is None:
            return [None]
        film_works_ids_for_update = self.get_film_work_ids_for_update(
            new_persons
        )
        return self.get_film_works_data(film_works_ids_for_update)
