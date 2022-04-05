import abc
import typing
from opentelemetry.context.context import Context as Context
from typing import Any

CarrierT: Any
CarrierValT: Any

class Getter(abc.ABC, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get(self, carrier: CarrierT, key: str) -> typing.Optional[typing.List[str]]: ...
    @abc.abstractmethod
    def keys(self, carrier: CarrierT) -> typing.List[str]: ...

class Setter(abc.ABC, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def set(self, carrier: CarrierT, key: str, value: str) -> None: ...

class DefaultGetter(Getter):
    def get(self, carrier: typing.Mapping[str, CarrierValT], key: str) -> typing.Optional[typing.List[str]]: ...
    def keys(self, carrier: typing.Dict[str, CarrierValT]) -> typing.List[str]: ...

default_getter: Any

class DefaultSetter(Setter):
    def set(self, carrier: typing.MutableMapping[str, CarrierValT], key: str, value: CarrierValT) -> None: ...

default_setter: Any

class TextMapPropagator(abc.ABC, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def extract(self, carrier: CarrierT, context: typing.Optional[Context] = ..., getter: Getter = ...) -> Context: ...
    @abc.abstractmethod
    def inject(self, carrier: CarrierT, context: typing.Optional[Context] = ..., setter: Setter = ...) -> None: ...
    @property
    @abc.abstractmethod
    def fields(self) -> typing.Set[str]: ...
