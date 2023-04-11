import pprint
import os
from typing import List

import backoff as backoff
from elastic_transport import ConnectionError, ConnectionTimeout
from elasticsearch import Elasticsearch, helpers
from load_dotenv import load_dotenv

from models import Filmwork


load_dotenv()

BACKOFF_CONF = {
    'max_time': int(os.getenv('MAX_TIME_BACKOFF')),
    'max_tries': int(os.getenv('MAX_TRIES_BACKOFF'))
}
ELASTIC_URL = os.getenv('ELASTIC_URL')
ELASTIC_PORT = os.getenv('ELASTIC_PORT')


class ElasticsearchLoader:

    def __init__(self):
        self.es = Elasticsearch(f'{ELASTIC_URL}:{ELASTIC_PORT}')

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, ConnectionTimeout),
        **BACKOFF_CONF
    )
    def insert_to_elastic(self, actions):
        return helpers.bulk(self.es, actions, raise_on_error=False)

    def bulk_load_movies(self, film_works:  List[Filmwork]):

        actions = []
        for fw in film_works:

            action = {
                "_index": "movies",
                "_id": fw.id,
            }
            action.update(fw.dict(exclude={'modified', 'created', 'type'}))
            actions.append(action)
        pprint.pp(actions)
        success, failed = self.insert_to_elastic(actions)
