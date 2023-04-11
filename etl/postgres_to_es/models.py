from datetime import datetime
from typing import List
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, root_validator, validator


class DateTimeMixin(BaseModel):
    created: datetime
    modified: datetime

    @validator('created')
    def check_createdt(cls, created):
        if not isinstance(created, datetime):
            created = datetime.strptime(
                created + '00', '%Y-%m-%d %H:%M:%S.%f%z'
            )
        return created

    @validator('modified')
    def check_updated_at(cls, modified):
        if not isinstance(modified, datetime):
            modified = datetime.strptime(
                modified + '00', '%Y-%m-%d %H:%M:%S.%f%z'
            )
        return modified


class UUIDMixin(BaseModel):
    id: UUID = Field(default_factory=uuid4)


class Actor(UUIDMixin):
    name: str


class Writer(Actor):
    pass


class Filmwork(UUIDMixin, DateTimeMixin):
    title: str
    description: str = None
    type: str
    actors: List[Actor] = []
    writers: List[Writer] = []
    director: List[str] = []
    genre: List[str]

    actors_names: List[str] = []
    writers_names: List[str] = []
    imdb_rating: float = Field(alias="rating", default=None)

    class Config:
        validate_assignment = True

    @validator('description')
    def check_description(cls, description):
        if description is None:
            return ''
        return description

    @validator('imdb_rating')
    def check_imdb_rating(cls, imdb_rating):
        if imdb_rating is None:
            return ''
        return imdb_rating

    @root_validator
    def calculate_actors_names(cls, values):
        values["actors_names"] = list(map(
            lambda act: act.name, values.get("actors")
        ))
        values["writers_names"] = list(map(
            lambda wrt: wrt.name, values.get("writers")
        ))

        return values
