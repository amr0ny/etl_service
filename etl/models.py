from uuid import UUID
from pydantic import BaseModel, PositiveFloat


class Genre(BaseModel):
    name: str


class Person(BaseModel):
    id: UUID
    name: str


class Filmwork(BaseModel):
    id: UUID
    title: str
    description: str | None
    imdb_rating: PositiveFloat | None
    genres: list[str]
    directors_names: list[str]
    actors_names: list[str]
    writers_names: list[str]
    directors: list[Person]
    actors: list[Person]
    writers: list[Person]