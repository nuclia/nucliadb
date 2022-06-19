"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import abc
import grpc
import nucliadb_telemetry.tests.grpc.hellostreamingworld_pb2
import typing

class MultiGreeterStub:
    """The greeting service definition."""
    def __init__(self, channel: grpc.Channel) -> None: ...
    sayHello: grpc.UnaryStreamMultiCallable[
        nucliadb_telemetry.tests.grpc.hellostreamingworld_pb2.HelloRequest,
        nucliadb_telemetry.tests.grpc.hellostreamingworld_pb2.HelloReply]
    """Sends multiple greetings"""


class MultiGreeterServicer(metaclass=abc.ABCMeta):
    """The greeting service definition."""
    @abc.abstractmethod
    def sayHello(self,
        request: nucliadb_telemetry.tests.grpc.hellostreamingworld_pb2.HelloRequest,
        context: grpc.ServicerContext,
    ) -> typing.Iterator[nucliadb_telemetry.tests.grpc.hellostreamingworld_pb2.HelloReply]:
        """Sends multiple greetings"""
        pass


def add_MultiGreeterServicer_to_server(servicer: MultiGreeterServicer, server: grpc.Server) -> None: ...
