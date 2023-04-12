from pydantic import BaseSettings
from pydantic.fields import Field
from dotenv import load_dotenv

load_dotenv()


class PostgresSettings(BaseSettings):
    host: str = Field(env="POSTGRES_HOST")
    port: int = Field(env="POSTGRES_PORT")
    dbname: str = Field(env="POSTGRES_DB")
    user: str = Field(env="POSTGRES_USER")
    password: str = Field(env="POSTGRES_PASSWORD")

    class Config:
        env_file = ".env"
