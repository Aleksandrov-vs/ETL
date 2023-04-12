import logging
import time
from contextlib import closing

import redis

from config import RedisSettings
from extractors.film_work import FilmworkExtractor
from extractors.genre import GenreExtractor
from extractors.person import PersonExtract
from loader import ElasticsearchLoader
from storages import RedisStorage, State
from transformer import DataTransform

redis_settings = RedisSettings()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


if __name__ == '__main__':
    with closing(redis.Redis(**redis_settings.dict())) as redis_conn:
        redis_storage = RedisStorage(redis_conn)
        state = State(redis_storage)
        person_extr = PersonExtract(state)
        genre_extr = GenreExtractor(state)
        film_work_extr = FilmworkExtractor(state)

        data_transform = DataTransform()
        loader = ElasticsearchLoader()

        while True:
            for data in person_extr.extract_persons():
                if data is None:
                    logging.info('новых персон нет')
                    break
                logging.info(f'в базе появилось {len(data)} новых персон')
                documents = data_transform.transform_data(data)
                loader.bulk_load_movies(documents)
                person_extr.update_modified_state()
            time.sleep(5)

            for data in genre_extr.extract_genres():
                if data is None:
                    logging.info('новых жанров нет')
                    break
                logging.info(f'в базе появилось {len(data)} новых жанров')
                documents = data_transform.transform_data(data)
                loader.bulk_load_movies(documents)
                genre_extr.update_modified_state()
            time.sleep(5)

            for data in film_work_extr.extract_filmwork():
                if data is None:
                    logging.info('новых фильмов нет')
                    break
                logging.info(f'в базе появилось {len(data)} новых фильмов')
                documents = data_transform.transform_data(data)
                loader.bulk_load_movies(documents)
                film_work_extr.update_modified_state()
            time.sleep(5)
