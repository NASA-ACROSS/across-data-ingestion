from abc import ABC, abstractmethod


class CredentialStorage(ABC):
    @abstractmethod
    def id(self, force: bool = False) -> str: ...

    @abstractmethod
    def secret(self, force: bool = False) -> str: ...

    @abstractmethod
    def update_key(self, key: str) -> None: ...
