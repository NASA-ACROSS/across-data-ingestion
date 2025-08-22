from abc import ABC, abstractmethod
from datetime import datetime


class KeyManager(ABC):
    @abstractmethod
    def get(self) -> str:
        return ""

    @abstractmethod
    def rotate(self) -> None:
        return

    @abstractmethod
    def expiration(self) -> datetime:
        return datetime.now()
