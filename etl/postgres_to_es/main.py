import os
from contextlib import closing

import redis
from extractors.film_work import FilmworkExtractor
from extractors.genre import GenreExtractor
from extractors.person import PersonExtract
from load_dotenv import load_dotenv
from loader import ElasticsearchLoader
from transformer import DataTransform

load_dotenv()

REDIS_CONF = {
    'host': os.getenv('REDIS_HOST'),
    'port': os.getenv('REDIS_PORT'),
    'password': os.getenv('REDIS_PASSWORD')
}

if __name__ == '__main__':
    with closing(redis.Redis(**REDIS_CONF)) as redis_conn:
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

            for data in genre_extr.extract_genres():
                if data is None:
                    break
                documents = data_transform.transform_data(data)
                loader.bulk_load_movies(documents)
                genre_extr.update_state()

            for data in film_work_extr.extract_filmwork():
                if data is None:
                    break
                documents = data_transform.transform_data(data)
                loader.bulk_load_movies(documents)
                film_work_extr.update_state()
            # time.sleep(1000)
