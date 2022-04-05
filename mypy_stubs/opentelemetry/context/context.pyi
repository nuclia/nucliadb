import abc
import typing
from abc import ABC, abstractmethod

class Context(typing.Dict[str, object]):
    def __setitem__(self, key: str, value: object) -> None: ...

class _RuntimeContext(ABC, metaclass=abc.ABCMeta):
    @abstractmethod
    def attach(self, context: Context) -> object: ...
    @abstractmethod
    def get_current(self) -> Context: ...
    @abstractmethod
    def detach(self, token: object) -> None: ...
