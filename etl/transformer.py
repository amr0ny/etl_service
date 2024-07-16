import abc
from pydantic import BaseModel
from models import Filmwork, Genre, Person
from config import logger

class BaseTransformer(abc.ABC):

    @abc.abstractmethod
    def transform(self, data: list[dict[str, any]]) -> list[BaseModel]:
        pass


class ComponentTransformer(BaseTransformer):
    def __init__(self):
        pass

    def transform(self, data: list[dict[str, any]]) -> list[BaseModel]:
        models = []
        for item in data:
            genres = [Genre(name=genre) for genre in item['genres']]
            model_dict = {
                'id': item['film_work_id'],
                'title': item['title'],
                'description': item['description'],
                'imdb_rating': item['rating'],
                'genres': [genre.name for genre in genres],
                'actors': [Person(id=person['person_id'], name=person['person_name']) for person in item['persons'] if person['person_role'] == 'actor'],
                'directors': [Person(id=person['person_id'], name=person['person_name']) for person in item['persons'] if person['person_role'] == 'director'],
                'writers': [Person(id=person['person_id'], name=person['person_name']) for person in item['persons'] if person['person_role'] == 'writer'],
                'directors_names': [person['person_name'] for person in item['persons'] if person['person_role'] == 'director'],
                'actors_names': [person['person_name'] for person in item['persons'] if person['person_role'] == 'actor'],
                'writers_names': [person['person_name'] for person in item['persons'] if person['person_role'] == 'writer'],
            }
            model = Filmwork(**model_dict)
            models.append(model)
        logger.debug(models)
        return models

class Transformer(ComponentTransformer):
    pass
