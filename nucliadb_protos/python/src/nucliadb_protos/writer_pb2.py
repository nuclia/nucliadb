# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/writer.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from nucliadb_protos import noderesources_pb2 as nucliadb__protos_dot_noderesources__pb2
try:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_noderesources__pb2.nucliadb__protos_dot_utils__pb2
except AttributeError:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_noderesources__pb2.nucliadb_protos.utils_pb2
from nucliadb_protos import resources_pb2 as nucliadb__protos_dot_resources__pb2
try:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_resources__pb2.nucliadb__protos_dot_utils__pb2
except AttributeError:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_resources__pb2.nucliadb_protos.utils_pb2
from nucliadb_protos import knowledgebox_pb2 as nucliadb__protos_dot_knowledgebox__pb2
try:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_knowledgebox__pb2.nucliadb__protos_dot_utils__pb2
except AttributeError:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_knowledgebox__pb2.nucliadb_protos.utils_pb2
try:
  nucliadb__protos_dot_nodewriter__pb2 = nucliadb__protos_dot_knowledgebox__pb2.nucliadb__protos_dot_nodewriter__pb2
except AttributeError:
  nucliadb__protos_dot_nodewriter__pb2 = nucliadb__protos_dot_knowledgebox__pb2.nucliadb_protos.nodewriter_pb2
try:
  nucliadb__protos_dot_noderesources__pb2 = nucliadb__protos_dot_knowledgebox__pb2.nucliadb__protos_dot_noderesources__pb2
except AttributeError:
  nucliadb__protos_dot_noderesources__pb2 = nucliadb__protos_dot_knowledgebox__pb2.nucliadb_protos.noderesources_pb2
try:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_knowledgebox__pb2.nucliadb__protos_dot_utils__pb2
except AttributeError:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_knowledgebox__pb2.nucliadb_protos.utils_pb2
from nucliadb_protos import audit_pb2 as nucliadb__protos_dot_audit__pb2

from nucliadb_protos.noderesources_pb2 import *
from nucliadb_protos.resources_pb2 import *
from nucliadb_protos.knowledgebox_pb2 import *
from nucliadb_protos.audit_pb2 import *

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1cnucliadb_protos/writer.proto\x12\tfdbwriter\x1a\x1fgoogle/protobuf/timestamp.proto\x1a#nucliadb_protos/noderesources.proto\x1a\x1fnucliadb_protos/resources.proto\x1a\"nucliadb_protos/knowledgebox.proto\x1a\x1bnucliadb_protos/audit.proto\"\xd9\x02\n\x05\x41udit\x12\x0c\n\x04user\x18\x01 \x01(\t\x12(\n\x04when\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0e\n\x06origin\x18\x03 \x01(\t\x12\'\n\x06source\x18\x04 \x01(\x0e\x32\x17.fdbwriter.Audit.Source\x12\x0c\n\x04kbid\x18\x05 \x01(\t\x12\x0c\n\x04uuid\x18\x06 \x01(\t\x12>\n\x0emessage_source\x18\x07 \x01(\x0e\x32&.fdbwriter.BrokerMessage.MessageSource\x12*\n\x0e\x66ield_metadata\x18\x08 \x03(\x0b\x32\x12.resources.FieldID\x12\'\n\x0c\x61udit_fields\x18\t \x03(\x0b\x32\x11.audit.AuditField\".\n\x06Source\x12\x08\n\x04HTTP\x10\x00\x12\r\n\tDASHBOARD\x10\x01\x12\x0b\n\x07\x44\x45SKTOP\x10\x02\"\xc3\x01\n\x05\x45rror\x12\r\n\x05\x66ield\x18\x01 \x01(\t\x12(\n\nfield_type\x18\x02 \x01(\x0e\x32\x14.resources.FieldType\x12\r\n\x05\x65rror\x18\x03 \x01(\t\x12(\n\x04\x63ode\x18\x04 \x01(\x0e\x32\x1a.fdbwriter.Error.ErrorCode\"H\n\tErrorCode\x12\x0b\n\x07GENERIC\x10\x00\x12\x0b\n\x07\x45XTRACT\x10\x01\x12\x0b\n\x07PROCESS\x10\x02\x12\x14\n\x10\x44\x41TAAUGMENTATION\x10\x03\"\x9d\x0e\n\rBrokerMessage\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\x0c\n\x04uuid\x18\x03 \x01(\t\x12\x0c\n\x04slug\x18\x04 \x01(\t\x12\x1f\n\x05\x61udit\x18\x05 \x01(\x0b\x32\x10.fdbwriter.Audit\x12\x32\n\x04type\x18\x06 \x01(\x0e\x32$.fdbwriter.BrokerMessage.MessageType\x12\x0f\n\x07multiid\x18\x07 \x01(\t\x12\x1f\n\x05\x62\x61sic\x18\x08 \x01(\x0b\x32\x10.resources.Basic\x12!\n\x06origin\x18\t \x01(\x0b\x32\x11.resources.Origin\x12\"\n\trelations\x18\n \x03(\x0b\x32\x0f.utils.Relation\x12\x42\n\rconversations\x18\x0b \x03(\x0b\x32+.fdbwriter.BrokerMessage.ConversationsEntry\x12\x32\n\x05texts\x18\r \x03(\x0b\x32#.fdbwriter.BrokerMessage.TextsEntry\x12\x32\n\x05links\x18\x10 \x03(\x0b\x32#.fdbwriter.BrokerMessage.LinksEntry\x12\x32\n\x05\x66iles\x18\x11 \x03(\x0b\x32#.fdbwriter.BrokerMessage.FilesEntry\x12\x39\n\x13link_extracted_data\x18\x12 \x03(\x0b\x32\x1c.resources.LinkExtractedData\x12\x39\n\x13\x66ile_extracted_data\x18\x13 \x03(\x0b\x32\x1c.resources.FileExtractedData\x12\x37\n\x0e\x65xtracted_text\x18\x14 \x03(\x0b\x32\x1f.resources.ExtractedTextWrapper\x12?\n\x0e\x66ield_metadata\x18\x15 \x03(\x0b\x32\'.resources.FieldComputedMetadataWrapper\x12\x39\n\rfield_vectors\x18\x16 \x03(\x0b\x32\".resources.ExtractedVectorsWrapper\x12\x45\n\x14\x66ield_large_metadata\x18\x17 \x03(\x0b\x32\'.resources.LargeComputedMetadataWrapper\x12)\n\rdelete_fields\x18\x18 \x03(\x0b\x32\x12.resources.FieldID\x12\x12\n\norigin_seq\x18\x19 \x01(\x05\x12\x1c\n\x14slow_processing_time\x18\x1a \x01(\x02\x12\x1b\n\x13pre_processing_time\x18\x1c \x01(\x02\x12-\n\tdone_time\x18\x1d \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x13\n\x07txseqid\x18\x1e \x01(\x03\x42\x02\x18\x01\x12 \n\x06\x65rrors\x18\x1f \x03(\x0b\x32\x10.fdbwriter.Error\x12\x15\n\rprocessing_id\x18  \x01(\t\x12\x36\n\x06source\x18! \x01(\x0e\x32&.fdbwriter.BrokerMessage.MessageSource\x12\x13\n\x0b\x61\x63\x63ount_seq\x18\" \x01(\x03\x12\x33\n\x0cuser_vectors\x18# \x03(\x0b\x32\x1d.resources.UserVectorsWrapper\x12\x0f\n\x07reindex\x18$ \x01(\x08\x12\x1f\n\x05\x65xtra\x18% \x01(\x0b\x32\x10.resources.Extra\x12?\n\x10question_answers\x18& \x03(\x0b\x32%.resources.FieldQuestionAnswerWrapper\x12!\n\x08security\x18\' \x01(\x0b\x32\x0f.utils.Security\x1aM\n\x12\x43onversationsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12&\n\x05value\x18\x02 \x01(\x0b\x32\x17.resources.Conversation:\x02\x38\x01\x1a\x42\n\nTextsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.resources.FieldText:\x02\x38\x01\x1a\x42\n\nLinksEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.resources.FieldLink:\x02\x38\x01\x1a\x42\n\nFilesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.resources.FieldFile:\x02\x38\x01\"N\n\x0bMessageType\x12\x0e\n\nAUTOCOMMIT\x10\x00\x12\t\n\x05MULTI\x10\x01\x12\n\n\x06\x43OMMIT\x10\x02\x12\x0c\n\x08ROLLBACK\x10\x03\x12\n\n\x06\x44\x45LETE\x10\x04\"*\n\rMessageSource\x12\n\n\x06WRITER\x10\x00\x12\r\n\tPROCESSOR\x10\x01J\x04\x08\x02\x10\x03J\x04\x08\x0c\x10\rJ\x04\x08\x0e\x10\x0fJ\x04\x08\x0f\x10\x10J\x04\x08\x1b\x10\x1c\"M\n\x1a\x42rokerMessageBlobReference\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12\x13\n\x0bstorage_key\x18\x03 \x01(\t\"\x97\x01\n\x14WriterStatusResponse\x12\x16\n\x0eknowledgeboxes\x18\x01 \x03(\t\x12\x39\n\x05msgid\x18\x02 \x03(\x0b\x32*.fdbwriter.WriterStatusResponse.MsgidEntry\x1a,\n\nMsgidEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x03:\x02\x38\x01\"\x15\n\x13WriterStatusRequest\"\x81\x01\n\x17NewEntitiesGroupRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\r\n\x05group\x18\x02 \x01(\t\x12-\n\x08\x65ntities\x18\x03 \x01(\x0b\x32\x1b.knowledgebox.EntitiesGroup\"\x99\x01\n\x18NewEntitiesGroupResponse\x12:\n\x06status\x18\x01 \x01(\x0e\x32*.fdbwriter.NewEntitiesGroupResponse.Status\"A\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\t\n\x05\x45RROR\x10\x01\x12\x10\n\x0cKB_NOT_FOUND\x10\x02\x12\x12\n\x0e\x41LREADY_EXISTS\x10\x03\"|\n\x12SetEntitiesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\r\n\x05group\x18\x02 \x01(\t\x12-\n\x08\x65ntities\x18\x03 \x01(\x0b\x32\x1b.knowledgebox.EntitiesGroup\"\x8a\x03\n\x1aUpdateEntitiesGroupRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\r\n\x05group\x18\x02 \x01(\t\x12;\n\x03\x61\x64\x64\x18\x03 \x03(\x0b\x32..fdbwriter.UpdateEntitiesGroupRequest.AddEntry\x12\x41\n\x06update\x18\x04 \x03(\x0b\x32\x31.fdbwriter.UpdateEntitiesGroupRequest.UpdateEntry\x12\x0e\n\x06\x64\x65lete\x18\x05 \x03(\t\x12\r\n\x05title\x18\x06 \x01(\t\x12\r\n\x05\x63olor\x18\x07 \x01(\t\x1a@\n\x08\x41\x64\x64\x45ntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.knowledgebox.Entity:\x02\x38\x01\x1a\x43\n\x0bUpdateEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.knowledgebox.Entity:\x02\x38\x01\"\xa9\x01\n\x1bUpdateEntitiesGroupResponse\x12=\n\x06status\x18\x01 \x01(\x0e\x32-.fdbwriter.UpdateEntitiesGroupResponse.Status\"K\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\t\n\x05\x45RROR\x10\x01\x12\x10\n\x0cKB_NOT_FOUND\x10\x02\x12\x1c\n\x18\x45NTITIES_GROUP_NOT_FOUND\x10\x03\"E\n\x19ListEntitiesGroupsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\"\x9b\x02\n\x1aListEntitiesGroupsResponse\x12\x41\n\x06groups\x18\x01 \x03(\x0b\x32\x31.fdbwriter.ListEntitiesGroupsResponse.GroupsEntry\x12<\n\x06status\x18\x02 \x01(\x0e\x32,.fdbwriter.ListEntitiesGroupsResponse.Status\x1aQ\n\x0bGroupsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x31\n\x05value\x18\x02 \x01(\x0b\x32\".knowledgebox.EntitiesGroupSummary:\x02\x38\x01\")\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\x12\t\n\x05\x45RROR\x10\x02\">\n\x12GetEntitiesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\"\xa9\x02\n\x13GetEntitiesResponse\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12:\n\x06groups\x18\x02 \x03(\x0b\x32*.fdbwriter.GetEntitiesResponse.GroupsEntry\x12\x35\n\x06status\x18\x03 \x01(\x0e\x32%.fdbwriter.GetEntitiesResponse.Status\x1aJ\n\x0bGroupsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12*\n\x05value\x18\x02 \x01(\x0b\x32\x1b.knowledgebox.EntitiesGroup:\x02\x38\x01\")\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\x12\t\n\x05\x45RROR\x10\x02\"M\n\x12\x44\x65lEntitiesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\r\n\x05group\x18\x02 \x01(\t\"\xd9\x01\n\x14MergeEntitiesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x36\n\x04\x66rom\x18\x02 \x01(\x0b\x32(.fdbwriter.MergeEntitiesRequest.EntityID\x12\x34\n\x02to\x18\x03 \x01(\x0b\x32(.fdbwriter.MergeEntitiesRequest.EntityID\x1a)\n\x08\x45ntityID\x12\r\n\x05group\x18\x01 \x01(\t\x12\x0e\n\x06\x65ntity\x18\x02 \x01(\t\"\xb8\x01\n\x11GetLabelsResponse\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12$\n\x06labels\x18\x02 \x01(\x0b\x32\x14.knowledgebox.Labels\x12\x33\n\x06status\x18\x03 \x01(\x0e\x32#.fdbwriter.GetLabelsResponse.Status\"\x1e\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\"<\n\x10GetLabelsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\"R\n\x17GetEntitiesGroupRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\r\n\x05group\x18\x02 \x01(\t\"\xf9\x01\n\x18GetEntitiesGroupResponse\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12*\n\x05group\x18\x02 \x01(\x0b\x32\x1b.knowledgebox.EntitiesGroup\x12:\n\x06status\x18\x03 \x01(\x0e\x32*.fdbwriter.GetEntitiesGroupResponse.Status\"K\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x10\n\x0cKB_NOT_FOUND\x10\x01\x12\x1c\n\x18\x45NTITIES_GROUP_NOT_FOUND\x10\x02\x12\t\n\x05\x45RROR\x10\x03\"@\n\x14GetVectorSetsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\"\xd3\x01\n\x15GetVectorSetsResponse\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12,\n\nvectorsets\x18\x02 \x01(\x0b\x32\x18.knowledgebox.VectorSets\x12\x37\n\x06status\x18\x03 \x01(\x0e\x32\'.fdbwriter.GetVectorSetsResponse.Status\")\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\x12\t\n\x05\x45RROR\x10\x02\"m\n\x0eOpStatusWriter\x12\x30\n\x06status\x18\x01 \x01(\x0e\x32 .fdbwriter.OpStatusWriter.Status\")\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\t\n\x05\x45RROR\x10\x01\x12\x0c\n\x08NOTFOUND\x10\x02\"\xd2\x03\n\x0cNotification\x12\x11\n\tpartition\x18\x01 \x01(\x05\x12\r\n\x05multi\x18\x02 \x01(\t\x12\x0c\n\x04uuid\x18\x03 \x01(\t\x12\x0c\n\x04kbid\x18\x04 \x01(\t\x12\r\n\x05seqid\x18\x05 \x01(\x03\x12.\n\x06\x61\x63tion\x18\x06 \x01(\x0e\x32\x1e.fdbwriter.Notification.Action\x12\x35\n\nwrite_type\x18\x07 \x01(\x0e\x32!.fdbwriter.Notification.WriteType\x12-\n\x07message\x18\x08 \x01(\x0b\x32\x18.fdbwriter.BrokerMessageB\x02\x18\x01\x12-\n\x06source\x18\t \x01(\x0e\x32\x1d.fdbwriter.NotificationSource\x12\x19\n\x11processing_errors\x18\n \x01(\x08\x12\'\n\rmessage_audit\x18\x0b \x01(\x0b\x32\x10.fdbwriter.Audit\",\n\x06\x41\x63tion\x12\n\n\x06\x43OMMIT\x10\x00\x12\t\n\x05\x41\x42ORT\x10\x01\x12\x0b\n\x07INDEXED\x10\x02\">\n\tWriteType\x12\t\n\x05UNSET\x10\x00\x12\x0b\n\x07\x43REATED\x10\x01\x12\x0c\n\x08MODIFIED\x10\x02\x12\x0b\n\x07\x44\x45LETED\x10\x03\"\xff\x01\n\x06Member\x12\n\n\x02id\x18\x01 \x01(\t\x12\x16\n\x0elisten_address\x18\x02 \x01(\t\x12\x13\n\x07is_self\x18\x03 \x01(\x08\x42\x02\x18\x01\x12(\n\x04type\x18\x04 \x01(\x0e\x32\x16.fdbwriter.Member.TypeB\x02\x18\x01\x12\x11\n\x05\x64ummy\x18\x05 \x01(\x08\x42\x02\x18\x01\x12\x16\n\nload_score\x18\x06 \x01(\x02\x42\x02\x18\x01\x12\x13\n\x0bshard_count\x18\x07 \x01(\r\x12\x12\n\nprimary_id\x18\x08 \x01(\t\">\n\x04Type\x12\x06\n\x02IO\x10\x00\x12\n\n\x06SEARCH\x10\x01\x12\n\n\x06INGEST\x10\x02\x12\t\n\x05TRAIN\x10\x03\x12\x0b\n\x07UNKNOWN\x10\x04\"\x14\n\x12ListMembersRequest\"9\n\x13ListMembersResponse\x12\"\n\x07members\x18\x01 \x03(\x0b\x32\x11.fdbwriter.Member\"H\n\x0cShardReplica\x12*\n\x05shard\x18\x01 \x01(\x0b\x32\x1b.noderesources.ShardCreated\x12\x0c\n\x04node\x18\x02 \x01(\t\"\x8d\x01\n\x0bShardObject\x12\r\n\x05shard\x18\x01 \x01(\t\x12)\n\x08replicas\x18\x03 \x03(\x0b\x32\x17.fdbwriter.ShardReplica\x12\x31\n\ttimestamp\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.TimestampB\x02\x18\x01\x12\x11\n\tread_only\x18\x05 \x01(\x08\"\xc6\x02\n\x06Shards\x12&\n\x06shards\x18\x01 \x03(\x0b\x32\x16.fdbwriter.ShardObject\x12\x0c\n\x04kbid\x18\x02 \x01(\t\x12\x12\n\x06\x61\x63tual\x18\x03 \x01(\x05\x42\x02\x18\x01\x12/\n\nsimilarity\x18\x04 \x01(\x0e\x32\x17.utils.VectorSimilarityB\x02\x18\x01\x12\x36\n\x05model\x18\x05 \x01(\x0b\x32#.knowledgebox.SemanticModelMetadataB\x02\x18\x01\x12.\n\x0frelease_channel\x18\x06 \x01(\x0e\x32\x15.utils.ReleaseChannel\x12+\n\x05\x65xtra\x18\x07 \x03(\x0b\x32\x1c.fdbwriter.Shards.ExtraEntry\x1a,\n\nExtraEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"C\n\rIndexResource\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\x0b\n\x03rid\x18\x02 \x01(\t\x12\x17\n\x0freindex_vectors\x18\x03 \x01(\x08\"\r\n\x0bIndexStatus\"\x1f\n\x0fSynonymsRequest\x12\x0c\n\x04kbid\x18\x01 \x01(\t\"\xe7\x01\n\x13NewVectorSetRequest\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\x14\n\x0cvectorset_id\x18\x02 \x01(\t\x12+\n\x0bvector_type\x18\x03 \x01(\x0e\x32\x16.nodewriter.VectorType\x12+\n\nsimilarity\x18\x04 \x01(\x0e\x32\x17.utils.VectorSimilarity\x12\x18\n\x10vector_dimension\x18\x05 \x01(\r\x12\x19\n\x11normalize_vectors\x18\x06 \x01(\x08\x12\x1d\n\x15matryoshka_dimensions\x18\x07 \x03(\r\"|\n\x14NewVectorSetResponse\x12\x36\n\x06status\x18\x01 \x01(\x0e\x32&.fdbwriter.NewVectorSetResponse.Status\x12\x0f\n\x07\x64\x65tails\x18\x02 \x01(\t\"\x1b\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\t\n\x05\x45RROR\x10\x01\"9\n\x13\x44\x65lVectorSetRequest\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\x14\n\x0cvectorset_id\x18\x02 \x01(\t\"|\n\x14\x44\x65lVectorSetResponse\x12\x36\n\x06status\x18\x01 \x01(\x0e\x32&.fdbwriter.DelVectorSetResponse.Status\x12\x0f\n\x07\x64\x65tails\x18\x02 \x01(\t\"\x1b\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\t\n\x05\x45RROR\x10\x01\"\xc8\x03\n\x18NewKnowledgeBoxV2Request\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\x0c\n\x04slug\x18\x02 \x01(\t\x12\r\n\x05title\x18\x03 \x01(\t\x12\x13\n\x0b\x64\x65scription\x18\x04 \x01(\t\x12\x41\n\nvectorsets\x18\x05 \x03(\x0b\x32-.fdbwriter.NewKnowledgeBoxV2Request.VectorSet\x12R\n\x17\x65xternal_index_provider\x18\x06 \x01(\x0b\x32\x31.knowledgebox.CreateExternalIndexProviderMetadata\x12 \n\x18hidden_resources_enabled\x18\x07 \x01(\x08\x12)\n!hidden_resources_hide_on_creation\x18\x08 \x01(\x08\x1a\x87\x01\n\tVectorSet\x12\x14\n\x0cvectorset_id\x18\x01 \x01(\t\x12+\n\nsimilarity\x18\x02 \x01(\x0e\x32\x17.utils.VectorSimilarity\x12\x18\n\x10vector_dimension\x18\x03 \x01(\r\x12\x1d\n\x15matryoshka_dimensions\x18\x04 \x03(\r\"l\n\x19NewKnowledgeBoxV2Response\x12\x38\n\x06status\x18\x01 \x01(\x0e\x32(.knowledgebox.KnowledgeBoxResponseStatus\x12\x15\n\rerror_message\x18\x02 \x01(\t*:\n\x12NotificationSource\x12\t\n\x05UNSET\x10\x00\x12\n\n\x06WRITER\x10\x01\x12\r\n\tPROCESSOR\x10\x02\x32\xa7\x0b\n\x06Writer\x12`\n\x11NewKnowledgeBoxV2\x12#.fdbwriter.NewKnowledgeBoxV2Request\x1a$.fdbwriter.NewKnowledgeBoxV2Response\"\x00\x12^\n\x12\x44\x65leteKnowledgeBox\x12\x1c.knowledgebox.KnowledgeBoxID\x1a(.knowledgebox.DeleteKnowledgeBoxResponse\"\x00\x12\x62\n\x12UpdateKnowledgeBox\x12 .knowledgebox.KnowledgeBoxUpdate\x1a(.knowledgebox.UpdateKnowledgeBoxResponse\"\x00\x12I\n\x0eProcessMessage\x12\x18.fdbwriter.BrokerMessage\x1a\x19.fdbwriter.OpStatusWriter\"\x00(\x01\x12]\n\x10NewEntitiesGroup\x12\".fdbwriter.NewEntitiesGroupRequest\x1a#.fdbwriter.NewEntitiesGroupResponse\"\x00\x12N\n\x0bGetEntities\x12\x1d.fdbwriter.GetEntitiesRequest\x1a\x1e.fdbwriter.GetEntitiesResponse\"\x00\x12]\n\x10GetEntitiesGroup\x12\".fdbwriter.GetEntitiesGroupRequest\x1a#.fdbwriter.GetEntitiesGroupResponse\"\x00\x12\x63\n\x12ListEntitiesGroups\x12$.fdbwriter.ListEntitiesGroupsRequest\x1a%.fdbwriter.ListEntitiesGroupsResponse\"\x00\x12I\n\x0bSetEntities\x12\x1d.fdbwriter.SetEntitiesRequest\x1a\x19.fdbwriter.OpStatusWriter\"\x00\x12\x66\n\x13UpdateEntitiesGroup\x12%.fdbwriter.UpdateEntitiesGroupRequest\x1a&.fdbwriter.UpdateEntitiesGroupResponse\"\x00\x12I\n\x0b\x44\x65lEntities\x12\x1d.fdbwriter.DelEntitiesRequest\x1a\x19.fdbwriter.OpStatusWriter\"\x00\x12K\n\x06Status\x12\x1e.fdbwriter.WriterStatusRequest\x1a\x1f.fdbwriter.WriterStatusResponse\"\x00\x12L\n\x0bListMembers\x12\x1d.fdbwriter.ListMembersRequest\x1a\x1e.fdbwriter.ListMembersResponse\x12;\n\x05Index\x12\x18.fdbwriter.IndexResource\x1a\x16.fdbwriter.IndexStatus\"\x00\x12=\n\x07ReIndex\x12\x18.fdbwriter.IndexResource\x1a\x16.fdbwriter.IndexStatus\"\x00\x12Q\n\x0cNewVectorSet\x12\x1e.fdbwriter.NewVectorSetRequest\x1a\x1f.fdbwriter.NewVectorSetResponse\"\x00\x12Q\n\x0c\x44\x65lVectorSet\x12\x1e.fdbwriter.DelVectorSetRequest\x1a\x1f.fdbwriter.DelVectorSetResponse\"\x00P\x01P\x02P\x03P\x04\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.writer_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _BROKERMESSAGE_CONVERSATIONSENTRY._options = None
  _BROKERMESSAGE_CONVERSATIONSENTRY._serialized_options = b'8\001'
  _BROKERMESSAGE_TEXTSENTRY._options = None
  _BROKERMESSAGE_TEXTSENTRY._serialized_options = b'8\001'
  _BROKERMESSAGE_LINKSENTRY._options = None
  _BROKERMESSAGE_LINKSENTRY._serialized_options = b'8\001'
  _BROKERMESSAGE_FILESENTRY._options = None
  _BROKERMESSAGE_FILESENTRY._serialized_options = b'8\001'
  _BROKERMESSAGE.fields_by_name['txseqid']._options = None
  _BROKERMESSAGE.fields_by_name['txseqid']._serialized_options = b'\030\001'
  _WRITERSTATUSRESPONSE_MSGIDENTRY._options = None
  _WRITERSTATUSRESPONSE_MSGIDENTRY._serialized_options = b'8\001'
  _UPDATEENTITIESGROUPREQUEST_ADDENTRY._options = None
  _UPDATEENTITIESGROUPREQUEST_ADDENTRY._serialized_options = b'8\001'
  _UPDATEENTITIESGROUPREQUEST_UPDATEENTRY._options = None
  _UPDATEENTITIESGROUPREQUEST_UPDATEENTRY._serialized_options = b'8\001'
  _LISTENTITIESGROUPSRESPONSE_GROUPSENTRY._options = None
  _LISTENTITIESGROUPSRESPONSE_GROUPSENTRY._serialized_options = b'8\001'
  _GETENTITIESRESPONSE_GROUPSENTRY._options = None
  _GETENTITIESRESPONSE_GROUPSENTRY._serialized_options = b'8\001'
  _NOTIFICATION.fields_by_name['message']._options = None
  _NOTIFICATION.fields_by_name['message']._serialized_options = b'\030\001'
  _MEMBER.fields_by_name['is_self']._options = None
  _MEMBER.fields_by_name['is_self']._serialized_options = b'\030\001'
  _MEMBER.fields_by_name['type']._options = None
  _MEMBER.fields_by_name['type']._serialized_options = b'\030\001'
  _MEMBER.fields_by_name['dummy']._options = None
  _MEMBER.fields_by_name['dummy']._serialized_options = b'\030\001'
  _MEMBER.fields_by_name['load_score']._options = None
  _MEMBER.fields_by_name['load_score']._serialized_options = b'\030\001'
  _SHARDOBJECT.fields_by_name['timestamp']._options = None
  _SHARDOBJECT.fields_by_name['timestamp']._serialized_options = b'\030\001'
  _SHARDS_EXTRAENTRY._options = None
  _SHARDS_EXTRAENTRY._serialized_options = b'8\001'
  _SHARDS.fields_by_name['actual']._options = None
  _SHARDS.fields_by_name['actual']._serialized_options = b'\030\001'
  _SHARDS.fields_by_name['similarity']._options = None
  _SHARDS.fields_by_name['similarity']._serialized_options = b'\030\001'
  _SHARDS.fields_by_name['model']._options = None
  _SHARDS.fields_by_name['model']._serialized_options = b'\030\001'
  _globals['_NOTIFICATIONSOURCE']._serialized_start=8402
  _globals['_NOTIFICATIONSOURCE']._serialized_end=8460
  _globals['_AUDIT']._serialized_start=212
  _globals['_AUDIT']._serialized_end=557
  _globals['_AUDIT_SOURCE']._serialized_start=511
  _globals['_AUDIT_SOURCE']._serialized_end=557
  _globals['_ERROR']._serialized_start=560
  _globals['_ERROR']._serialized_end=755
  _globals['_ERROR_ERRORCODE']._serialized_start=683
  _globals['_ERROR_ERRORCODE']._serialized_end=755
  _globals['_BROKERMESSAGE']._serialized_start=758
  _globals['_BROKERMESSAGE']._serialized_end=2579
  _globals['_BROKERMESSAGE_CONVERSATIONSENTRY']._serialized_start=2144
  _globals['_BROKERMESSAGE_CONVERSATIONSENTRY']._serialized_end=2221
  _globals['_BROKERMESSAGE_TEXTSENTRY']._serialized_start=2223
  _globals['_BROKERMESSAGE_TEXTSENTRY']._serialized_end=2289
  _globals['_BROKERMESSAGE_LINKSENTRY']._serialized_start=2291
  _globals['_BROKERMESSAGE_LINKSENTRY']._serialized_end=2357
  _globals['_BROKERMESSAGE_FILESENTRY']._serialized_start=2359
  _globals['_BROKERMESSAGE_FILESENTRY']._serialized_end=2425
  _globals['_BROKERMESSAGE_MESSAGETYPE']._serialized_start=2427
  _globals['_BROKERMESSAGE_MESSAGETYPE']._serialized_end=2505
  _globals['_BROKERMESSAGE_MESSAGESOURCE']._serialized_start=2507
  _globals['_BROKERMESSAGE_MESSAGESOURCE']._serialized_end=2549
  _globals['_BROKERMESSAGEBLOBREFERENCE']._serialized_start=2581
  _globals['_BROKERMESSAGEBLOBREFERENCE']._serialized_end=2658
  _globals['_WRITERSTATUSRESPONSE']._serialized_start=2661
  _globals['_WRITERSTATUSRESPONSE']._serialized_end=2812
  _globals['_WRITERSTATUSRESPONSE_MSGIDENTRY']._serialized_start=2768
  _globals['_WRITERSTATUSRESPONSE_MSGIDENTRY']._serialized_end=2812
  _globals['_WRITERSTATUSREQUEST']._serialized_start=2814
  _globals['_WRITERSTATUSREQUEST']._serialized_end=2835
  _globals['_NEWENTITIESGROUPREQUEST']._serialized_start=2838
  _globals['_NEWENTITIESGROUPREQUEST']._serialized_end=2967
  _globals['_NEWENTITIESGROUPRESPONSE']._serialized_start=2970
  _globals['_NEWENTITIESGROUPRESPONSE']._serialized_end=3123
  _globals['_NEWENTITIESGROUPRESPONSE_STATUS']._serialized_start=3058
  _globals['_NEWENTITIESGROUPRESPONSE_STATUS']._serialized_end=3123
  _globals['_SETENTITIESREQUEST']._serialized_start=3125
  _globals['_SETENTITIESREQUEST']._serialized_end=3249
  _globals['_UPDATEENTITIESGROUPREQUEST']._serialized_start=3252
  _globals['_UPDATEENTITIESGROUPREQUEST']._serialized_end=3646
  _globals['_UPDATEENTITIESGROUPREQUEST_ADDENTRY']._serialized_start=3513
  _globals['_UPDATEENTITIESGROUPREQUEST_ADDENTRY']._serialized_end=3577
  _globals['_UPDATEENTITIESGROUPREQUEST_UPDATEENTRY']._serialized_start=3579
  _globals['_UPDATEENTITIESGROUPREQUEST_UPDATEENTRY']._serialized_end=3646
  _globals['_UPDATEENTITIESGROUPRESPONSE']._serialized_start=3649
  _globals['_UPDATEENTITIESGROUPRESPONSE']._serialized_end=3818
  _globals['_UPDATEENTITIESGROUPRESPONSE_STATUS']._serialized_start=3743
  _globals['_UPDATEENTITIESGROUPRESPONSE_STATUS']._serialized_end=3818
  _globals['_LISTENTITIESGROUPSREQUEST']._serialized_start=3820
  _globals['_LISTENTITIESGROUPSREQUEST']._serialized_end=3889
  _globals['_LISTENTITIESGROUPSRESPONSE']._serialized_start=3892
  _globals['_LISTENTITIESGROUPSRESPONSE']._serialized_end=4175
  _globals['_LISTENTITIESGROUPSRESPONSE_GROUPSENTRY']._serialized_start=4051
  _globals['_LISTENTITIESGROUPSRESPONSE_GROUPSENTRY']._serialized_end=4132
  _globals['_LISTENTITIESGROUPSRESPONSE_STATUS']._serialized_start=4134
  _globals['_LISTENTITIESGROUPSRESPONSE_STATUS']._serialized_end=4175
  _globals['_GETENTITIESREQUEST']._serialized_start=4177
  _globals['_GETENTITIESREQUEST']._serialized_end=4239
  _globals['_GETENTITIESRESPONSE']._serialized_start=4242
  _globals['_GETENTITIESRESPONSE']._serialized_end=4539
  _globals['_GETENTITIESRESPONSE_GROUPSENTRY']._serialized_start=4422
  _globals['_GETENTITIESRESPONSE_GROUPSENTRY']._serialized_end=4496
  _globals['_GETENTITIESRESPONSE_STATUS']._serialized_start=4134
  _globals['_GETENTITIESRESPONSE_STATUS']._serialized_end=4175
  _globals['_DELENTITIESREQUEST']._serialized_start=4541
  _globals['_DELENTITIESREQUEST']._serialized_end=4618
  _globals['_MERGEENTITIESREQUEST']._serialized_start=4621
  _globals['_MERGEENTITIESREQUEST']._serialized_end=4838
  _globals['_MERGEENTITIESREQUEST_ENTITYID']._serialized_start=4797
  _globals['_MERGEENTITIESREQUEST_ENTITYID']._serialized_end=4838
  _globals['_GETLABELSRESPONSE']._serialized_start=4841
  _globals['_GETLABELSRESPONSE']._serialized_end=5025
  _globals['_GETLABELSRESPONSE_STATUS']._serialized_start=4134
  _globals['_GETLABELSRESPONSE_STATUS']._serialized_end=4164
  _globals['_GETLABELSREQUEST']._serialized_start=5027
  _globals['_GETLABELSREQUEST']._serialized_end=5087
  _globals['_GETENTITIESGROUPREQUEST']._serialized_start=5089
  _globals['_GETENTITIESGROUPREQUEST']._serialized_end=5171
  _globals['_GETENTITIESGROUPRESPONSE']._serialized_start=5174
  _globals['_GETENTITIESGROUPRESPONSE']._serialized_end=5423
  _globals['_GETENTITIESGROUPRESPONSE_STATUS']._serialized_start=5348
  _globals['_GETENTITIESGROUPRESPONSE_STATUS']._serialized_end=5423
  _globals['_GETVECTORSETSREQUEST']._serialized_start=5425
  _globals['_GETVECTORSETSREQUEST']._serialized_end=5489
  _globals['_GETVECTORSETSRESPONSE']._serialized_start=5492
  _globals['_GETVECTORSETSRESPONSE']._serialized_end=5703
  _globals['_GETVECTORSETSRESPONSE_STATUS']._serialized_start=4134
  _globals['_GETVECTORSETSRESPONSE_STATUS']._serialized_end=4175
  _globals['_OPSTATUSWRITER']._serialized_start=5705
  _globals['_OPSTATUSWRITER']._serialized_end=5814
  _globals['_OPSTATUSWRITER_STATUS']._serialized_start=5773
  _globals['_OPSTATUSWRITER_STATUS']._serialized_end=5814
  _globals['_NOTIFICATION']._serialized_start=5817
  _globals['_NOTIFICATION']._serialized_end=6283
  _globals['_NOTIFICATION_ACTION']._serialized_start=6175
  _globals['_NOTIFICATION_ACTION']._serialized_end=6219
  _globals['_NOTIFICATION_WRITETYPE']._serialized_start=6221
  _globals['_NOTIFICATION_WRITETYPE']._serialized_end=6283
  _globals['_MEMBER']._serialized_start=6286
  _globals['_MEMBER']._serialized_end=6541
  _globals['_MEMBER_TYPE']._serialized_start=6479
  _globals['_MEMBER_TYPE']._serialized_end=6541
  _globals['_LISTMEMBERSREQUEST']._serialized_start=6543
  _globals['_LISTMEMBERSREQUEST']._serialized_end=6563
  _globals['_LISTMEMBERSRESPONSE']._serialized_start=6565
  _globals['_LISTMEMBERSRESPONSE']._serialized_end=6622
  _globals['_SHARDREPLICA']._serialized_start=6624
  _globals['_SHARDREPLICA']._serialized_end=6696
  _globals['_SHARDOBJECT']._serialized_start=6699
  _globals['_SHARDOBJECT']._serialized_end=6840
  _globals['_SHARDS']._serialized_start=6843
  _globals['_SHARDS']._serialized_end=7169
  _globals['_SHARDS_EXTRAENTRY']._serialized_start=7125
  _globals['_SHARDS_EXTRAENTRY']._serialized_end=7169
  _globals['_INDEXRESOURCE']._serialized_start=7171
  _globals['_INDEXRESOURCE']._serialized_end=7238
  _globals['_INDEXSTATUS']._serialized_start=7240
  _globals['_INDEXSTATUS']._serialized_end=7253
  _globals['_SYNONYMSREQUEST']._serialized_start=7255
  _globals['_SYNONYMSREQUEST']._serialized_end=7286
  _globals['_NEWVECTORSETREQUEST']._serialized_start=7289
  _globals['_NEWVECTORSETREQUEST']._serialized_end=7520
  _globals['_NEWVECTORSETRESPONSE']._serialized_start=7522
  _globals['_NEWVECTORSETRESPONSE']._serialized_end=7646
  _globals['_NEWVECTORSETRESPONSE_STATUS']._serialized_start=3058
  _globals['_NEWVECTORSETRESPONSE_STATUS']._serialized_end=3085
  _globals['_DELVECTORSETREQUEST']._serialized_start=7648
  _globals['_DELVECTORSETREQUEST']._serialized_end=7705
  _globals['_DELVECTORSETRESPONSE']._serialized_start=7707
  _globals['_DELVECTORSETRESPONSE']._serialized_end=7831
  _globals['_DELVECTORSETRESPONSE_STATUS']._serialized_start=3058
  _globals['_DELVECTORSETRESPONSE_STATUS']._serialized_end=3085
  _globals['_NEWKNOWLEDGEBOXV2REQUEST']._serialized_start=7834
  _globals['_NEWKNOWLEDGEBOXV2REQUEST']._serialized_end=8290
  _globals['_NEWKNOWLEDGEBOXV2REQUEST_VECTORSET']._serialized_start=8155
  _globals['_NEWKNOWLEDGEBOXV2REQUEST_VECTORSET']._serialized_end=8290
  _globals['_NEWKNOWLEDGEBOXV2RESPONSE']._serialized_start=8292
  _globals['_NEWKNOWLEDGEBOXV2RESPONSE']._serialized_end=8400
  _globals['_WRITER']._serialized_start=8463
  _globals['_WRITER']._serialized_end=9910
# @@protoc_insertion_point(module_scope)
