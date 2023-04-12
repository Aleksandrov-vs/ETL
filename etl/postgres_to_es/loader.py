
from typing import List

from models import Filmwork
from config import ElasticSettings, BackoffConf

import backoff as backoff
from elastic_transport import ConnectionError, ConnectionTimeout
from elasticsearch import Elasticsearch, helpers

elastic_settings = ElasticSettings()
backoff_conf = BackoffConf()


class ElasticsearchLoader:

    def __init__(self):
        self.es = Elasticsearch(f'{elastic_settings.url}:{elastic_settings.port}')

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, ConnectionTimeout),
        **backoff_conf.dict()
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
        success, failed = self.insert_to_elastic(actions)
