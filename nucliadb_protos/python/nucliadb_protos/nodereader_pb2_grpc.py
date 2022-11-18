# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from nucliadb_protos import nodereader_pb2 as nucliadb__protos_dot_nodereader__pb2
from nucliadb_protos import noderesources_pb2 as nucliadb__protos_dot_noderesources__pb2


class NodeReaderStub(object):
    """Implemented at nucliadb_object_storage

    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetShard = channel.unary_unary(
                '/nodereader.NodeReader/GetShard',
                request_serializer=nucliadb__protos_dot_nodereader__pb2.GetShardRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_noderesources__pb2.Shard.FromString,
                )
        self.GetShards = channel.unary_unary(
                '/nodereader.NodeReader/GetShards',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.EmptyQuery.SerializeToString,
                response_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardList.FromString,
                )
        self.DocumentSearch = channel.unary_unary(
                '/nodereader.NodeReader/DocumentSearch',
                request_serializer=nucliadb__protos_dot_nodereader__pb2.DocumentSearchRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodereader__pb2.DocumentSearchResponse.FromString,
                )
        self.ParagraphSearch = channel.unary_unary(
                '/nodereader.NodeReader/ParagraphSearch',
                request_serializer=nucliadb__protos_dot_nodereader__pb2.ParagraphSearchRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodereader__pb2.ParagraphSearchResponse.FromString,
                )
        self.VectorSearch = channel.unary_unary(
                '/nodereader.NodeReader/VectorSearch',
                request_serializer=nucliadb__protos_dot_nodereader__pb2.VectorSearchRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodereader__pb2.VectorSearchResponse.FromString,
                )
        self.RelationSearch = channel.unary_unary(
                '/nodereader.NodeReader/RelationSearch',
                request_serializer=nucliadb__protos_dot_nodereader__pb2.RelationSearchRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodereader__pb2.RelationSearchResponse.FromString,
                )
        self.DocumentIds = channel.unary_unary(
                '/nodereader.NodeReader/DocumentIds',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodereader__pb2.IdCollection.FromString,
                )
        self.ParagraphIds = channel.unary_unary(
                '/nodereader.NodeReader/ParagraphIds',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodereader__pb2.IdCollection.FromString,
                )
        self.VectorIds = channel.unary_unary(
                '/nodereader.NodeReader/VectorIds',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodereader__pb2.IdCollection.FromString,
                )
        self.RelationIds = channel.unary_unary(
                '/nodereader.NodeReader/RelationIds',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodereader__pb2.IdCollection.FromString,
                )
        self.RelationEdges = channel.unary_unary(
                '/nodereader.NodeReader/RelationEdges',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodereader__pb2.EdgeList.FromString,
                )
        self.RelationTypes = channel.unary_unary(
                '/nodereader.NodeReader/RelationTypes',
                request_serializer=nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodereader__pb2.TypeList.FromString,
                )
        self.Search = channel.unary_unary(
                '/nodereader.NodeReader/Search',
                request_serializer=nucliadb__protos_dot_nodereader__pb2.SearchRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodereader__pb2.SearchResponse.FromString,
                )
        self.Suggest = channel.unary_unary(
                '/nodereader.NodeReader/Suggest',
                request_serializer=nucliadb__protos_dot_nodereader__pb2.SuggestRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_nodereader__pb2.SuggestResponse.FromString,
                )


class NodeReaderServicer(object):
    """Implemented at nucliadb_object_storage

    """

    def GetShard(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetShards(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DocumentSearch(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ParagraphSearch(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def VectorSearch(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RelationSearch(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DocumentIds(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ParagraphIds(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def VectorIds(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RelationIds(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RelationEdges(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RelationTypes(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Search(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Suggest(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_NodeReaderServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetShard': grpc.unary_unary_rpc_method_handler(
                    servicer.GetShard,
                    request_deserializer=nucliadb__protos_dot_nodereader__pb2.GetShardRequest.FromString,
                    response_serializer=nucliadb__protos_dot_noderesources__pb2.Shard.SerializeToString,
            ),
            'GetShards': grpc.unary_unary_rpc_method_handler(
                    servicer.GetShards,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.EmptyQuery.FromString,
                    response_serializer=nucliadb__protos_dot_noderesources__pb2.ShardList.SerializeToString,
            ),
            'DocumentSearch': grpc.unary_unary_rpc_method_handler(
                    servicer.DocumentSearch,
                    request_deserializer=nucliadb__protos_dot_nodereader__pb2.DocumentSearchRequest.FromString,
                    response_serializer=nucliadb__protos_dot_nodereader__pb2.DocumentSearchResponse.SerializeToString,
            ),
            'ParagraphSearch': grpc.unary_unary_rpc_method_handler(
                    servicer.ParagraphSearch,
                    request_deserializer=nucliadb__protos_dot_nodereader__pb2.ParagraphSearchRequest.FromString,
                    response_serializer=nucliadb__protos_dot_nodereader__pb2.ParagraphSearchResponse.SerializeToString,
            ),
            'VectorSearch': grpc.unary_unary_rpc_method_handler(
                    servicer.VectorSearch,
                    request_deserializer=nucliadb__protos_dot_nodereader__pb2.VectorSearchRequest.FromString,
                    response_serializer=nucliadb__protos_dot_nodereader__pb2.VectorSearchResponse.SerializeToString,
            ),
            'RelationSearch': grpc.unary_unary_rpc_method_handler(
                    servicer.RelationSearch,
                    request_deserializer=nucliadb__protos_dot_nodereader__pb2.RelationSearchRequest.FromString,
                    response_serializer=nucliadb__protos_dot_nodereader__pb2.RelationSearchResponse.SerializeToString,
            ),
            'DocumentIds': grpc.unary_unary_rpc_method_handler(
                    servicer.DocumentIds,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_nodereader__pb2.IdCollection.SerializeToString,
            ),
            'ParagraphIds': grpc.unary_unary_rpc_method_handler(
                    servicer.ParagraphIds,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_nodereader__pb2.IdCollection.SerializeToString,
            ),
            'VectorIds': grpc.unary_unary_rpc_method_handler(
                    servicer.VectorIds,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_nodereader__pb2.IdCollection.SerializeToString,
            ),
            'RelationIds': grpc.unary_unary_rpc_method_handler(
                    servicer.RelationIds,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_nodereader__pb2.IdCollection.SerializeToString,
            ),
            'RelationEdges': grpc.unary_unary_rpc_method_handler(
                    servicer.RelationEdges,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_nodereader__pb2.EdgeList.SerializeToString,
            ),
            'RelationTypes': grpc.unary_unary_rpc_method_handler(
                    servicer.RelationTypes,
                    request_deserializer=nucliadb__protos_dot_noderesources__pb2.ShardId.FromString,
                    response_serializer=nucliadb__protos_dot_nodereader__pb2.TypeList.SerializeToString,
            ),
            'Search': grpc.unary_unary_rpc_method_handler(
                    servicer.Search,
                    request_deserializer=nucliadb__protos_dot_nodereader__pb2.SearchRequest.FromString,
                    response_serializer=nucliadb__protos_dot_nodereader__pb2.SearchResponse.SerializeToString,
            ),
            'Suggest': grpc.unary_unary_rpc_method_handler(
                    servicer.Suggest,
                    request_deserializer=nucliadb__protos_dot_nodereader__pb2.SuggestRequest.FromString,
                    response_serializer=nucliadb__protos_dot_nodereader__pb2.SuggestResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'nodereader.NodeReader', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class NodeReader(object):
    """Implemented at nucliadb_object_storage

    """

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
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/GetShard',
            nucliadb__protos_dot_nodereader__pb2.GetShardRequest.SerializeToString,
            nucliadb__protos_dot_noderesources__pb2.Shard.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetShards(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/GetShards',
            nucliadb__protos_dot_noderesources__pb2.EmptyQuery.SerializeToString,
            nucliadb__protos_dot_noderesources__pb2.ShardList.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def DocumentSearch(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/DocumentSearch',
            nucliadb__protos_dot_nodereader__pb2.DocumentSearchRequest.SerializeToString,
            nucliadb__protos_dot_nodereader__pb2.DocumentSearchResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ParagraphSearch(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/ParagraphSearch',
            nucliadb__protos_dot_nodereader__pb2.ParagraphSearchRequest.SerializeToString,
            nucliadb__protos_dot_nodereader__pb2.ParagraphSearchResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def VectorSearch(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/VectorSearch',
            nucliadb__protos_dot_nodereader__pb2.VectorSearchRequest.SerializeToString,
            nucliadb__protos_dot_nodereader__pb2.VectorSearchResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RelationSearch(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/RelationSearch',
            nucliadb__protos_dot_nodereader__pb2.RelationSearchRequest.SerializeToString,
            nucliadb__protos_dot_nodereader__pb2.RelationSearchResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def DocumentIds(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/DocumentIds',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_nodereader__pb2.IdCollection.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ParagraphIds(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/ParagraphIds',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_nodereader__pb2.IdCollection.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def VectorIds(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/VectorIds',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_nodereader__pb2.IdCollection.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RelationIds(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/RelationIds',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_nodereader__pb2.IdCollection.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RelationEdges(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/RelationEdges',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_nodereader__pb2.EdgeList.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RelationTypes(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/RelationTypes',
            nucliadb__protos_dot_noderesources__pb2.ShardId.SerializeToString,
            nucliadb__protos_dot_nodereader__pb2.TypeList.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Search(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/Search',
            nucliadb__protos_dot_nodereader__pb2.SearchRequest.SerializeToString,
            nucliadb__protos_dot_nodereader__pb2.SearchResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Suggest(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/nodereader.NodeReader/Suggest',
            nucliadb__protos_dot_nodereader__pb2.SuggestRequest.SerializeToString,
            nucliadb__protos_dot_nodereader__pb2.SuggestResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
