import abc
import typing
from abc import ABC, abstractmethod
from opentelemetry.context.context import Context as Context
from opentelemetry import textmap as textmap
from typing import Any

def get_global_response_propagator(): ...
def set_global_response_propagator(propagator) -> None: ...

class Setter(ABC, metaclass=abc.ABCMeta):
    @abstractmethod
    def set(self, carrier, key, value): ...

class DictHeaderSetter(Setter):
    def set(self, carrier, key, value) -> None: ...

class FuncSetter(Setter):
    def __init__(self, func) -> None: ...
    def set(self, carrier, key, value) -> None: ...

default_setter: Any

class ResponsePropagator(ABC, metaclass=abc.ABCMeta):
    @abstractmethod
    def inject(
        self,
        carrier: textmap.CarrierT,
        context: typing.Optional[Context] = ...,
        setter: textmap.Setter = ...,
    ) -> None: ...

class TraceResponsePropagator(ResponsePropagator):
    def inject(
        self,
        carrier: textmap.CarrierT,
        context: typing.Optional[Context] = ...,
        setter: textmap.Setter = ...,
    ) -> None: ...
