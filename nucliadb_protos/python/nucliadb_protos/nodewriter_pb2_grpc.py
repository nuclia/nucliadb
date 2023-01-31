# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from nucliadb_protos import noderesources_pb2 as nucliadb__protos_dot_noderesources__pb2
from nucliadb_protos import nodewriter_pb2 as nucliadb__protos_dot_nodewriter__pb2


class NodeWriterStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetShard = channel.unary_unary(
                '/nodewriter.NodeWriter/GetShard',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                )
        self.NewShard = channel.unary_unary(
                '/nodewriter.NodeWriter/NewShard',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.EmptyQuery.SerializeToString,
                response_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardCreated.FromString,
                )
        self.CleanAndUpgradeShard = channel.unary_unary(
                '/nodewriter.NodeWriter/CleanAndUpgradeShard',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardCleaned.FromString,
                )
        self.DeleteShard = channel.unary_unary(
                '/nodewriter.NodeWriter/DeleteShard',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                )
        self.ListShards = channel.unary_unary(
                '/nodewriter.NodeWriter/ListShards',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.EmptyQuery.SerializeToString,
                response_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardIds.FromString,
                )
        self.GC = channel.unary_unary(
                '/nodewriter.NodeWriter/GC',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_noderesources__pb2.EmptyResponse.FromString,
                )
        self.SetResource = channel.unary_unary(
                '/nodewriter.NodeWriter/SetResource',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.Resource.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodewriter__pb2.OpStatus.FromString,
                )
        self.DeleteRelationNodes = channel.unary_unary(
                '/nodewriter.NodeWriter/DeleteRelationNodes',
                request_serializer=nucliadb__protos_dot_nodewriter__pb2.DeleteGraphNodes.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodewriter__pb2.OpStatus.FromString,
                )
        self.JoinGraph = channel.unary_unary(
                '/nodewriter.NodeWriter/JoinGraph',
                request_serializer=nucliadb__protos_dot_nodewriter__pb2.SetGraph.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodewriter__pb2.OpStatus.FromString,
                )
        self.RemoveResource = channel.unary_unary(
                '/nodewriter.NodeWriter/RemoveResource',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ResourceID.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodewriter__pb2.OpStatus.FromString,
                )
        self.AddVectorSet = channel.unary_unary(
                '/nodewriter.NodeWriter/AddVectorSet',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.VectorSetID.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodewriter__pb2.OpStatus.FromString,
                )
        self.RemoveVectorSet = channel.unary_unary(
                '/nodewriter.NodeWriter/RemoveVectorSet',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.VectorSetID.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodewriter__pb2.OpStatus.FromString,
                )
        self.ListVectorSets = channel.unary_unary(
                '/nodewriter.NodeWriter/ListVectorSets',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_noderesources__pb2.VectorSetList.FromString,
                )
        self.MoveShard = channel.unary_unary(
                '/nodewriter.NodeWriter/MoveShard',
                request_serializer=nucliadb__protos_dot_nodewriter__pb2.MoveShardRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_noderesources__pb2.EmptyResponse.FromString,
                )
        self.AcceptShard = channel.unary_unary(
                '/nodewriter.NodeWriter/AcceptShard',
                request_serializer=nucliadb__protos_dot_nodewriter__pb2.AcceptShardRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_noderesources__pb2.EmptyResponse.FromString,
                )


class NodeWriterServicer(object):
    """Missing associated documentation comment in .proto file."""

    def GetShard(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def NewShard(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CleanAndUpgradeShard(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DeleteShard(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListShards(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GC(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SetResource(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DeleteRelationNodes(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def JoinGraph(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RemoveResource(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def AddVectorSet(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RemoveVectorSet(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListVectorSets(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def MoveShard(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def AcceptShard(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_NodeWriterServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetShard': grpc.unary_unary_rpc_method_handler(
                    servicer.GetShard,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            ),
            'NewShard': grpc.unary_unary_rpc_method_handler(
                    servicer.NewShard,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.EmptyQuery.FromString,
                    response_serializer=nucliadb__protos_dot_noderesources__pb2.ShardCreated.SerializeToString,
            ),
            'CleanAndUpgradeShard': grpc.unary_unary_rpc_method_handler(
                    servicer.CleanAndUpgradeShard,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_noderesources__pb2.ShardCleaned.SerializeToString,
            ),
            'DeleteShard': grpc.unary_unary_rpc_method_handler(
                    servicer.DeleteShard,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            ),
            'ListShards': grpc.unary_unary_rpc_method_handler(
                    servicer.ListShards,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.EmptyQuery.FromString,
                    response_serializer=nucliadb__protos_dot_noderesources__pb2.ShardIds.SerializeToString,
            ),
            'GC': grpc.unary_unary_rpc_method_handler(
                    servicer.GC,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_noderesources__pb2.EmptyResponse.SerializeToString,
            ),
            'SetResource': grpc.unary_unary_rpc_method_handler(
                    servicer.SetResource,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.Resource.FromString,
                    response_serializer=nucliadb__protos_dot_nodewriter__pb2.OpStatus.SerializeToString,
            ),
            'DeleteRelationNodes': grpc.unary_unary_rpc_method_handler(
                    servicer.DeleteRelationNodes,
                    request_deserializer=nucliadb__protos_dot_nodewriter__pb2.DeleteGraphNodes.FromString,
                    response_serializer=nucliadb__protos_dot_nodewriter__pb2.OpStatus.SerializeToString,
            ),
            'JoinGraph': grpc.unary_unary_rpc_method_handler(
                    servicer.JoinGraph,
                    request_deserializer=nucliadb__protos_dot_nodewriter__pb2.SetGraph.FromString,
                    response_serializer=nucliadb__protos_dot_nodewriter__pb2.OpStatus.SerializeToString,
            ),
            'RemoveResource': grpc.unary_unary_rpc_method_handler(
                    servicer.RemoveResource,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ResourceID.FromString,
                    response_serializer=nucliadb__protos_dot_nodewriter__pb2.OpStatus.SerializeToString,
            ),
            'AddVectorSet': grpc.unary_unary_rpc_method_handler(
                    servicer.AddVectorSet,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.VectorSetID.FromString,
                    response_serializer=nucliadb__protos_dot_nodewriter__pb2.OpStatus.SerializeToString,
            ),
            'RemoveVectorSet': grpc.unary_unary_rpc_method_handler(
                    servicer.RemoveVectorSet,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.VectorSetID.FromString,
                    response_serializer=nucliadb__protos_dot_nodewriter__pb2.OpStatus.SerializeToString,
            ),
            'ListVectorSets': grpc.unary_unary_rpc_method_handler(
                    servicer.ListVectorSets,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_noderesources__pb2.VectorSetList.SerializeToString,
            ),
            'MoveShard': grpc.unary_unary_rpc_method_handler(
                    servicer.MoveShard,
                    request_deserializer=nucliadb__protos_dot_nodewriter__pb2.MoveShardRequest.FromString,
                    response_serializer=nucliadb__protos_dot_noderesources__pb2.EmptyResponse.SerializeToString,
            ),
            'AcceptShard': grpc.unary_unary_rpc_method_handler(
                    servicer.AcceptShard,
                    request_deserializer=nucliadb__protos_dot_nodewriter__pb2.AcceptShardRequest.FromString,
                    response_serializer=nucliadb__protos_dot_noderesources__pb2.EmptyResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'nodewriter.NodeWriter', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class NodeWriter(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def GetShard(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/GetShard',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def NewShard(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/NewShard',
            nucliadb__protos_dot_noderesources__pb2.EmptyQuery.SerializeToString,
            nucliadb__protos_dot_noderesources__pb2.ShardCreated.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def CleanAndUpgradeShard(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/CleanAndUpgradeShard',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_noderesources__pb2.ShardCleaned.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def DeleteShard(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/DeleteShard',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ListShards(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/ListShards',
            nucliadb__protos_dot_noderesources__pb2.EmptyQuery.SerializeToString,
            nucliadb__protos_dot_noderesources__pb2.ShardIds.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GC(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/GC',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_noderesources__pb2.EmptyResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SetResource(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/SetResource',
            nucliadb__protos_dot_noderesources__pb2.Resource.SerializeToString,
            nucliadb__protos_dot_nodewriter__pb2.OpStatus.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def DeleteRelationNodes(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/DeleteRelationNodes',
            nucliadb__protos_dot_nodewriter__pb2.DeleteGraphNodes.SerializeToString,
            nucliadb__protos_dot_nodewriter__pb2.OpStatus.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def JoinGraph(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/JoinGraph',
            nucliadb__protos_dot_nodewriter__pb2.SetGraph.SerializeToString,
            nucliadb__protos_dot_nodewriter__pb2.OpStatus.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RemoveResource(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/RemoveResource',
            nucliadb__protos_dot_noderesources__pb2.ResourceID.SerializeToString,
            nucliadb__protos_dot_nodewriter__pb2.OpStatus.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def AddVectorSet(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/AddVectorSet',
            nucliadb__protos_dot_noderesources__pb2.VectorSetID.SerializeToString,
            nucliadb__protos_dot_nodewriter__pb2.OpStatus.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RemoveVectorSet(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/RemoveVectorSet',
            nucliadb__protos_dot_noderesources__pb2.VectorSetID.SerializeToString,
            nucliadb__protos_dot_nodewriter__pb2.OpStatus.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ListVectorSets(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/ListVectorSets',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_noderesources__pb2.VectorSetList.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def MoveShard(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/MoveShard',
            nucliadb__protos_dot_nodewriter__pb2.MoveShardRequest.SerializeToString,
            nucliadb__protos_dot_noderesources__pb2.EmptyResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def AcceptShard(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeWriter/AcceptShard',
            nucliadb__protos_dot_nodewriter__pb2.AcceptShardRequest.SerializeToString,
            nucliadb__protos_dot_noderesources__pb2.EmptyResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)


class NodeSidecarStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetCount = channel.unary_unary(
                '/nodewriter.NodeSidecar/GetCount',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodewriter__pb2.Counter.FromString,
                )
        self.ShadowShardCreate = channel.unary_unary(
                '/nodewriter.NodeSidecar/ShadowShardCreate',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodewriter__pb2.ShadowShardResponse.FromString,
                )
        self.ShadowShardDelete = channel.unary_unary(
                '/nodewriter.NodeSidecar/ShadowShardDelete',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodewriter__pb2.ShadowShardResponse.FromString,
                )


class NodeSidecarServicer(object):
    """Missing associated documentation comment in .proto file."""

    def GetCount(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ShadowShardCreate(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ShadowShardDelete(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_NodeSidecarServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetCount': grpc.unary_unary_rpc_method_handler(
                    servicer.GetCount,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_nodewriter__pb2.Counter.SerializeToString,
            ),
            'ShadowShardCreate': grpc.unary_unary_rpc_method_handler(
                    servicer.ShadowShardCreate,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_nodewriter__pb2.ShadowShardResponse.SerializeToString,
            ),
            'ShadowShardDelete': grpc.unary_unary_rpc_method_handler(
                    servicer.ShadowShardDelete,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_nodewriter__pb2.ShadowShardResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'nodewriter.NodeSidecar', rpc_method_handlers)
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
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeSidecar/GetCount',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_nodewriter__pb2.Counter.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ShadowShardCreate(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeSidecar/ShadowShardCreate',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_nodewriter__pb2.ShadowShardResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ShadowShardDelete(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodewriter.NodeSidecar/ShadowShardDelete',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_nodewriter__pb2.ShadowShardResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
