from typing import Dict, List

from models import Actor, Filmwork, Writer
from psycopg2.extras import RealDictRow


class DataTransform:

    def transform_persons(self, persons: List[Dict]):
        writer_list: List[Writer] = []
        actor_list: List[Actor] = []
        director_list: List[str] = []

        for p in persons:
            if p['person_role'] == 'writer':
                writer_list.append(
                    Writer(id=p['person_id'], name=p['person_name'])
                )
            if p['person_role'] == 'actor':
                actor_list.append(
                    Actor(id=p['person_id'], name=p['person_name'])
                )
            if p['person_role'] == 'director':
                director_list.append(p['person_name'])

        return writer_list, actor_list, director_list

    def transform_data(self, data: List[RealDictRow]) -> List[Filmwork]:
        film_works = []
        for row in data:
            writer_list, actor_list, director_list = self.transform_persons(
                row.pop('persons')
            )
            genres = row.pop('genres')
            film_work = Filmwork(
                **row, actors=actor_list,
                writers=writer_list,
                director=director_list,
                genre=genres
            )
            film_works.append(film_work)
        return film_works
