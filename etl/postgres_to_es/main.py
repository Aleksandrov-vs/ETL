import os
import time
from contextlib import closing

from config import RedisSettings

import redis
from extractors.film_work import FilmworkExtractor
from extractors.genre import GenreExtractor
from extractors.person import PersonExtract
from loader import ElasticsearchLoader
from transformer import DataTransform
from storages import State


redis_settings = RedisSettings()

if __name__ == '__main__':
    with closing(redis.Redis(**redis_settings.dict())) as redis_conn:
        state = State()
        person_extr = PersonExtract(state)
        genre_extr = GenreExtractor(state)
        film_work_extr = FilmworkExtractor(state)

        data_transform = DataTransform()
        loader = ElasticsearchLoader()

        while True:
            for data in person_extr.extract_persons():
                if data is None:
                    break
                documents = data_transform.transform_data(data)
                loader.bulk_load_movies(documents)
                person_extr.update_modified_state()
            time.sleep(5)

            for data in genre_extr.extract_genres():
                if data is None:
                    break
                documents = data_transform.transform_data(data)
                loader.bulk_load_movies(documents)
                genre_extr.update_modified_state()
            time.sleep(5)

            for data in film_work_extr.extract_filmwork():
                if data is None:
                    break
                documents = data_transform.transform_data(data)
                loader.bulk_load_movies(documents)
                film_work_extr.update_modified_state()
            time.sleep(5)
