# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from nucliadb_protos import noderesources_pb2 as nucliadb__protos_dot_noderesources__pb2
from nucliadb_protos import nodesidecar_pb2 as nucliadb__protos_dot_nodesidecar__pb2


class NodeSidecarStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetCount = channel.unary_unary(
                '/nodesidecar.NodeSidecar/GetCount',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodesidecar__pb2.Counter.FromString,
                )


class NodeSidecarServicer(object):
    """Missing associated documentation comment in .proto file."""

    def GetCount(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_NodeSidecarServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetCount': grpc.unary_unary_rpc_method_handler(
                    servicer.GetCount,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_nodesidecar__pb2.Counter.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'nodesidecar.NodeSidecar', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class NodeSidecar(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def GetCount(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodesidecar.NodeSidecar/GetCount',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_nodesidecar__pb2.Counter.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
