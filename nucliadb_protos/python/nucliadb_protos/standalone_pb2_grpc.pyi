"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import abc
import collections.abc
import grpc
import grpc.aio
import nucliadb_protos.standalone_pb2
import typing

_T = typing.TypeVar('_T')

class _MaybeAsyncIterator(collections.abc.AsyncIterator[_T], collections.abc.Iterator[_T], metaclass=abc.ABCMeta):
    ...

class _ServicerContext(grpc.ServicerContext, grpc.aio.ServicerContext):  # type: ignore
    ...

class StandaloneClusterServiceStub:
    def __init__(self, channel: typing.Union[grpc.Channel, grpc.aio.Channel]) -> None: ...
    NodeAction: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.standalone_pb2.NodeActionRequest,
        nucliadb_protos.standalone_pb2.NodeActionResponse,
    ]
    NodeInfo: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.standalone_pb2.NodeInfoRequest,
        nucliadb_protos.standalone_pb2.NodeInfoResponse,
    ]

class StandaloneClusterServiceAsyncStub:
    NodeAction: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.standalone_pb2.NodeActionRequest,
        nucliadb_protos.standalone_pb2.NodeActionResponse,
    ]
    NodeInfo: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.standalone_pb2.NodeInfoRequest,
        nucliadb_protos.standalone_pb2.NodeInfoResponse,
    ]

class StandaloneClusterServiceServicer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def NodeAction(
        self,
        request: nucliadb_protos.standalone_pb2.NodeActionRequest,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.standalone_pb2.NodeActionResponse, collections.abc.Awaitable[nucliadb_protos.standalone_pb2.NodeActionResponse]]: ...
    @abc.abstractmethod
    def NodeInfo(
        self,
        request: nucliadb_protos.standalone_pb2.NodeInfoRequest,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.standalone_pb2.NodeInfoResponse, collections.abc.Awaitable[nucliadb_protos.standalone_pb2.NodeInfoResponse]]: ...

def add_StandaloneClusterServiceServicer_to_server(servicer: StandaloneClusterServiceServicer, server: typing.Union[grpc.Server, grpc.aio.Server]) -> None: ...
