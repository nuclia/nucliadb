import typing
from opentelemetry.textmap import (
    CarrierT as CarrierT,
    Getter as Getter,
    Setter as Setter,
    TextMapPropagator,
)

class B3MultiFormat(TextMapPropagator):
    SINGLE_HEADER_KEY: str
    TRACE_ID_KEY: str
    SPAN_ID_KEY: str
    SAMPLED_KEY: str
    FLAGS_KEY: str
    def extract(
        self,
        carrier: CarrierT,
        context: typing.Optional[typing.Any] = ...,
        getter: Getter = ...,
    ) -> typing.Any: ...
    def inject(
        self,
        carrier: CarrierT,
        context: typing.Optional[typing.Any] = ...,
        setter: Setter = ...,
    ) -> None: ...
    @property
    def fields(self) -> typing.Set[str]: ...

class B3SingleFormat(B3MultiFormat):
    def inject(
        self,
        carrier: CarrierT,
        context: typing.Optional[typing.Any] = ...,
        setter: Setter = ...,
    ) -> None: ...
    @property
    def fields(self) -> typing.Set[str]: ...

class B3Format(B3MultiFormat):
    def __init__(self, *args, **kwargs) -> None: ...
