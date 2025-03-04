# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from nucliadb_protos import backups_pb2 as nucliadb__protos_dot_backups__pb2
from nucliadb_protos import knowledgebox_pb2 as nucliadb__protos_dot_knowledgebox__pb2
from nucliadb_protos import writer_pb2 as nucliadb__protos_dot_writer__pb2


class WriterStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.NewKnowledgeBoxV2 = channel.unary_unary(
                '/fdbwriter.Writer/NewKnowledgeBoxV2',
                request_serializer=nucliadb__protos_dot_writer__pb2.NewKnowledgeBoxV2Request.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.NewKnowledgeBoxV2Response.FromString,
                )
        self.DeleteKnowledgeBox = channel.unary_unary(
                '/fdbwriter.Writer/DeleteKnowledgeBox',
                request_serializer=nucliadb__protos_dot_knowledgebox__pb2.KnowledgeBoxID.SerializeToString,
                response_deserializer=nucliadb__protos_dot_knowledgebox__pb2.DeleteKnowledgeBoxResponse.FromString,
                )
        self.UpdateKnowledgeBox = channel.unary_unary(
                '/fdbwriter.Writer/UpdateKnowledgeBox',
                request_serializer=nucliadb__protos_dot_knowledgebox__pb2.KnowledgeBoxUpdate.SerializeToString,
                response_deserializer=nucliadb__protos_dot_knowledgebox__pb2.UpdateKnowledgeBoxResponse.FromString,
                )
        self.ProcessMessage = channel.stream_unary(
                '/fdbwriter.Writer/ProcessMessage',
                request_serializer=nucliadb__protos_dot_writer__pb2.BrokerMessage.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.OpStatusWriter.FromString,
                )
        self.NewEntitiesGroup = channel.unary_unary(
                '/fdbwriter.Writer/NewEntitiesGroup',
                request_serializer=nucliadb__protos_dot_writer__pb2.NewEntitiesGroupRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.NewEntitiesGroupResponse.FromString,
                )
        self.GetEntities = channel.unary_unary(
                '/fdbwriter.Writer/GetEntities',
                request_serializer=nucliadb__protos_dot_writer__pb2.GetEntitiesRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.GetEntitiesResponse.FromString,
                )
        self.GetEntitiesGroup = channel.unary_unary(
                '/fdbwriter.Writer/GetEntitiesGroup',
                request_serializer=nucliadb__protos_dot_writer__pb2.GetEntitiesGroupRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.GetEntitiesGroupResponse.FromString,
                )
        self.ListEntitiesGroups = channel.unary_unary(
                '/fdbwriter.Writer/ListEntitiesGroups',
                request_serializer=nucliadb__protos_dot_writer__pb2.ListEntitiesGroupsRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.ListEntitiesGroupsResponse.FromString,
                )
        self.SetEntities = channel.unary_unary(
                '/fdbwriter.Writer/SetEntities',
                request_serializer=nucliadb__protos_dot_writer__pb2.SetEntitiesRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.OpStatusWriter.FromString,
                )
        self.UpdateEntitiesGroup = channel.unary_unary(
                '/fdbwriter.Writer/UpdateEntitiesGroup',
                request_serializer=nucliadb__protos_dot_writer__pb2.UpdateEntitiesGroupRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.UpdateEntitiesGroupResponse.FromString,
                )
        self.DelEntities = channel.unary_unary(
                '/fdbwriter.Writer/DelEntities',
                request_serializer=nucliadb__protos_dot_writer__pb2.DelEntitiesRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.OpStatusWriter.FromString,
                )
        self.Status = channel.unary_unary(
                '/fdbwriter.Writer/Status',
                request_serializer=nucliadb__protos_dot_writer__pb2.WriterStatusRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.WriterStatusResponse.FromString,
                )
        self.ListMembers = channel.unary_unary(
                '/fdbwriter.Writer/ListMembers',
                request_serializer=nucliadb__protos_dot_writer__pb2.ListMembersRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.ListMembersResponse.FromString,
                )
        self.Index = channel.unary_unary(
                '/fdbwriter.Writer/Index',
                request_serializer=nucliadb__protos_dot_writer__pb2.IndexResource.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.IndexStatus.FromString,
                )
        self.ReIndex = channel.unary_unary(
                '/fdbwriter.Writer/ReIndex',
                request_serializer=nucliadb__protos_dot_writer__pb2.IndexResource.SerializeToString,
                response_deserializer=nucliadb__protos_dot_writer__pb2.IndexStatus.FromString,
                )
        self.CreateBackup = channel.unary_unary(
                '/fdbwriter.Writer/CreateBackup',
                request_serializer=nucliadb__protos_dot_backups__pb2.CreateBackupRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_backups__pb2.CreateBackupResponse.FromString,
                )
        self.DeleteBackup = channel.unary_unary(
                '/fdbwriter.Writer/DeleteBackup',
                request_serializer=nucliadb__protos_dot_backups__pb2.DeleteBackupRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_backups__pb2.DeleteBackupResponse.FromString,
                )
        self.RestoreBackup = channel.unary_unary(
                '/fdbwriter.Writer/RestoreBackup',
                request_serializer=nucliadb__protos_dot_backups__pb2.RestoreBackupRequest.SerializeToString,
                response_deserializer=nucliadb__protos_dot_backups__pb2.RestoreBackupResponse.FromString,
                )


class WriterServicer(object):
    """Missing associated documentation comment in .proto file."""

    def NewKnowledgeBoxV2(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DeleteKnowledgeBox(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UpdateKnowledgeBox(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ProcessMessage(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def NewEntitiesGroup(self, request, context):
        """Entities
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetEntities(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetEntitiesGroup(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListEntitiesGroups(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SetEntities(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UpdateEntitiesGroup(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DelEntities(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Status(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListMembers(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Index(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ReIndex(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CreateBackup(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DeleteBackup(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RestoreBackup(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_WriterServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'NewKnowledgeBoxV2': grpc.unary_unary_rpc_method_handler(
                    servicer.NewKnowledgeBoxV2,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.NewKnowledgeBoxV2Request.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.NewKnowledgeBoxV2Response.SerializeToString,
            ),
            'DeleteKnowledgeBox': grpc.unary_unary_rpc_method_handler(
                    servicer.DeleteKnowledgeBox,
                    request_deserializer=nucliadb__protos_dot_knowledgebox__pb2.KnowledgeBoxID.FromString,
                    response_serializer=nucliadb__protos_dot_knowledgebox__pb2.DeleteKnowledgeBoxResponse.SerializeToString,
            ),
            'UpdateKnowledgeBox': grpc.unary_unary_rpc_method_handler(
                    servicer.UpdateKnowledgeBox,
                    request_deserializer=nucliadb__protos_dot_knowledgebox__pb2.KnowledgeBoxUpdate.FromString,
                    response_serializer=nucliadb__protos_dot_knowledgebox__pb2.UpdateKnowledgeBoxResponse.SerializeToString,
            ),
            'ProcessMessage': grpc.stream_unary_rpc_method_handler(
                    servicer.ProcessMessage,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.BrokerMessage.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.OpStatusWriter.SerializeToString,
            ),
            'NewEntitiesGroup': grpc.unary_unary_rpc_method_handler(
                    servicer.NewEntitiesGroup,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.NewEntitiesGroupRequest.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.NewEntitiesGroupResponse.SerializeToString,
            ),
            'GetEntities': grpc.unary_unary_rpc_method_handler(
                    servicer.GetEntities,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.GetEntitiesRequest.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.GetEntitiesResponse.SerializeToString,
            ),
            'GetEntitiesGroup': grpc.unary_unary_rpc_method_handler(
                    servicer.GetEntitiesGroup,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.GetEntitiesGroupRequest.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.GetEntitiesGroupResponse.SerializeToString,
            ),
            'ListEntitiesGroups': grpc.unary_unary_rpc_method_handler(
                    servicer.ListEntitiesGroups,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.ListEntitiesGroupsRequest.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.ListEntitiesGroupsResponse.SerializeToString,
            ),
            'SetEntities': grpc.unary_unary_rpc_method_handler(
                    servicer.SetEntities,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.SetEntitiesRequest.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.OpStatusWriter.SerializeToString,
            ),
            'UpdateEntitiesGroup': grpc.unary_unary_rpc_method_handler(
                    servicer.UpdateEntitiesGroup,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.UpdateEntitiesGroupRequest.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.UpdateEntitiesGroupResponse.SerializeToString,
            ),
            'DelEntities': grpc.unary_unary_rpc_method_handler(
                    servicer.DelEntities,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.DelEntitiesRequest.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.OpStatusWriter.SerializeToString,
            ),
            'Status': grpc.unary_unary_rpc_method_handler(
                    servicer.Status,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.WriterStatusRequest.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.WriterStatusResponse.SerializeToString,
            ),
            'ListMembers': grpc.unary_unary_rpc_method_handler(
                    servicer.ListMembers,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.ListMembersRequest.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.ListMembersResponse.SerializeToString,
            ),
            'Index': grpc.unary_unary_rpc_method_handler(
                    servicer.Index,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.IndexResource.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.IndexStatus.SerializeToString,
            ),
            'ReIndex': grpc.unary_unary_rpc_method_handler(
                    servicer.ReIndex,
                    request_deserializer=nucliadb__protos_dot_writer__pb2.IndexResource.FromString,
                    response_serializer=nucliadb__protos_dot_writer__pb2.IndexStatus.SerializeToString,
            ),
            'CreateBackup': grpc.unary_unary_rpc_method_handler(
                    servicer.CreateBackup,
                    request_deserializer=nucliadb__protos_dot_backups__pb2.CreateBackupRequest.FromString,
                    response_serializer=nucliadb__protos_dot_backups__pb2.CreateBackupResponse.SerializeToString,
            ),
            'DeleteBackup': grpc.unary_unary_rpc_method_handler(
                    servicer.DeleteBackup,
                    request_deserializer=nucliadb__protos_dot_backups__pb2.DeleteBackupRequest.FromString,
                    response_serializer=nucliadb__protos_dot_backups__pb2.DeleteBackupResponse.SerializeToString,
            ),
            'RestoreBackup': grpc.unary_unary_rpc_method_handler(
                    servicer.RestoreBackup,
                    request_deserializer=nucliadb__protos_dot_backups__pb2.RestoreBackupRequest.FromString,
                    response_serializer=nucliadb__protos_dot_backups__pb2.RestoreBackupResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'fdbwriter.Writer', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Writer(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def NewKnowledgeBoxV2(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/NewKnowledgeBoxV2',
            nucliadb__protos_dot_writer__pb2.NewKnowledgeBoxV2Request.SerializeToString,
            nucliadb__protos_dot_writer__pb2.NewKnowledgeBoxV2Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def DeleteKnowledgeBox(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/DeleteKnowledgeBox',
            nucliadb__protos_dot_knowledgebox__pb2.KnowledgeBoxID.SerializeToString,
            nucliadb__protos_dot_knowledgebox__pb2.DeleteKnowledgeBoxResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def UpdateKnowledgeBox(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/UpdateKnowledgeBox',
            nucliadb__protos_dot_knowledgebox__pb2.KnowledgeBoxUpdate.SerializeToString,
            nucliadb__protos_dot_knowledgebox__pb2.UpdateKnowledgeBoxResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ProcessMessage(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(request_iterator, target, '/fdbwriter.Writer/ProcessMessage',
            nucliadb__protos_dot_writer__pb2.BrokerMessage.SerializeToString,
            nucliadb__protos_dot_writer__pb2.OpStatusWriter.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def NewEntitiesGroup(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/NewEntitiesGroup',
            nucliadb__protos_dot_writer__pb2.NewEntitiesGroupRequest.SerializeToString,
            nucliadb__protos_dot_writer__pb2.NewEntitiesGroupResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetEntities(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/GetEntities',
            nucliadb__protos_dot_writer__pb2.GetEntitiesRequest.SerializeToString,
            nucliadb__protos_dot_writer__pb2.GetEntitiesResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetEntitiesGroup(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/GetEntitiesGroup',
            nucliadb__protos_dot_writer__pb2.GetEntitiesGroupRequest.SerializeToString,
            nucliadb__protos_dot_writer__pb2.GetEntitiesGroupResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ListEntitiesGroups(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/ListEntitiesGroups',
            nucliadb__protos_dot_writer__pb2.ListEntitiesGroupsRequest.SerializeToString,
            nucliadb__protos_dot_writer__pb2.ListEntitiesGroupsResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SetEntities(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/SetEntities',
            nucliadb__protos_dot_writer__pb2.SetEntitiesRequest.SerializeToString,
            nucliadb__protos_dot_writer__pb2.OpStatusWriter.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def UpdateEntitiesGroup(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/UpdateEntitiesGroup',
            nucliadb__protos_dot_writer__pb2.UpdateEntitiesGroupRequest.SerializeToString,
            nucliadb__protos_dot_writer__pb2.UpdateEntitiesGroupResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def DelEntities(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/DelEntities',
            nucliadb__protos_dot_writer__pb2.DelEntitiesRequest.SerializeToString,
            nucliadb__protos_dot_writer__pb2.OpStatusWriter.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Status(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/Status',
            nucliadb__protos_dot_writer__pb2.WriterStatusRequest.SerializeToString,
            nucliadb__protos_dot_writer__pb2.WriterStatusResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ListMembers(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/ListMembers',
            nucliadb__protos_dot_writer__pb2.ListMembersRequest.SerializeToString,
            nucliadb__protos_dot_writer__pb2.ListMembersResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Index(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/Index',
            nucliadb__protos_dot_writer__pb2.IndexResource.SerializeToString,
            nucliadb__protos_dot_writer__pb2.IndexStatus.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ReIndex(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/ReIndex',
            nucliadb__protos_dot_writer__pb2.IndexResource.SerializeToString,
            nucliadb__protos_dot_writer__pb2.IndexStatus.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def CreateBackup(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/CreateBackup',
            nucliadb__protos_dot_backups__pb2.CreateBackupRequest.SerializeToString,
            nucliadb__protos_dot_backups__pb2.CreateBackupResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def DeleteBackup(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/DeleteBackup',
            nucliadb__protos_dot_backups__pb2.DeleteBackupRequest.SerializeToString,
            nucliadb__protos_dot_backups__pb2.DeleteBackupResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RestoreBackup(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fdbwriter.Writer/RestoreBackup',
            nucliadb__protos_dot_backups__pb2.RestoreBackupRequest.SerializeToString,
            nucliadb__protos_dot_backups__pb2.RestoreBackupResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
