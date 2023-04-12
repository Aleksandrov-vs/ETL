from pydantic import BaseSettings
from pydantic.fields import Field
from load_dotenv import load_dotenv

load_dotenv()


class RedisSettings(BaseSettings):
    host: str = Field(env="REDIS_HOST")
    port: int = Field(env="REDIS_PORT")
    password: str = Field(env="REDIS_PASSWORD")

    class Config:
        env_file = ".env"


class PostgresSettings(BaseSettings):
    host: str = Field(env="POSTGRES_HOST")
    port: int = Field(env="POSTGRES_PORT")
    dbname: str = Field(env="POSTGRES_DB")
    user: str = Field(env="POSTGRES_USER")
    password: str = Field(env="POSTGRES_PASSWORD")

    class Config:
        env_file = ".env"


class ElasticSettings(BaseSettings):
    url: str = Field(env="ELASTIC_URL")
    port: int = Field(env="ELASTIC_PORT")

    class Config:
        env_file = ".env"


class BackoffConf(BaseSettings):
    max_time: int = Field(env="MAX_TIME_BACKOFF")
    max_tries: int = Field(env="MAX_TRIES_BACKOFF")

    class Config:
        env_file = ".env"
