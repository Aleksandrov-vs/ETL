import uuid
from dataclasses import dataclass
from datetime import datetime


@dataclass
class DataTimeMixin:
    created_at: datetime
    updated_at: datetime

    def __post_init__(self):
        if not isinstance(self.created_at, datetime):
            self.created_at = datetime.strptime(
                self.created_at+'00', '%Y-%m-%d %H:%M:%S.%f%z'
            )

        if not isinstance(self.updated_at, datetime):
            self.updated_at = datetime.strptime(
                self.updated_at+'00', '%Y-%m-%d %H:%M:%S.%f%z'
            )


@dataclass
class UUIDMixin:
    id: uuid


@dataclass
class Filmwork(UUIDMixin, DataTimeMixin):
    title: str
    description: str
    creation_date: datetime
    rating: float
    type: str
    file_path: str = None


@dataclass
class Genre(UUIDMixin, DataTimeMixin):
    name: str
    description: str


@dataclass
class GenreFilmwork(UUIDMixin):
    genre_id: uuid
    film_work_id: uuid
    created_at: datetime

    def __post_init__(self):
        if not isinstance(self.created_at, datetime):
            self.created_at = datetime.strptime(
                self.created_at + '00', '%Y-%m-%d %H:%M:%S.%f%z'
            )


@dataclass
class Person(UUIDMixin, DataTimeMixin):
    full_name: str


@dataclass
class PersonFilmwork(UUIDMixin):
    person_id: uuid
    film_work_id: uuid
    role: str
    created_at: datetime

    def __post_init__(self):
        if not isinstance(self.created_at, datetime):
            self.created_at = datetime.strptime(
                self.created_at + '00', '%Y-%m-%d %H:%M:%S.%f%z'
            )
