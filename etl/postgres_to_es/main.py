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


redis_settings = RedisSettings()

if __name__ == '__main__':
    with closing(redis.Redis(**redis_settings.dict())) as redis_conn:
        person_extr = PersonExtract(redis_conn)
        genre_extr = GenreExtractor(redis_conn)
        film_work_extr = FilmworkExtractor(redis_conn)

        data_transform = DataTransform()
        loader = ElasticsearchLoader()

        while True:
            for data in person_extr.extract_persons():
                if data is None:
                    break
                documents = data_transform.transform_data(data)
                loader.bulk_load_movies(documents)
                person_extr.update_state()
            time.sleep(5)

            for data in genre_extr.extract_genres():
                if data is None:
                    break
                documents = data_transform.transform_data(data)
                loader.bulk_load_movies(documents)
                genre_extr.update_state()
            time.sleep(5)

            for data in film_work_extr.extract_filmwork():
                if data is None:
                    break
                documents = data_transform.transform_data(data)
                loader.bulk_load_movies(documents)
                film_work_extr.update_state()
            time.sleep(5)
