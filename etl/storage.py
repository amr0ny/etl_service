import abc
import json

class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    def save_state(self, state: dict[str, any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> dict[str, any]:
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

        
    def save_state(self, state: dict[str, any]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, 'w') as json_file:
            json.dump(state, json_file)
           
    def retrieve_state(self) -> dict[str, any]:
        """Получить состояние из хранилища."""
        state = {}
        try:
            with open(self.file_path, 'r') as json_file:
                state = json.load(json_file)
        finally:
            return state
