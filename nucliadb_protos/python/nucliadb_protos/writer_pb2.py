# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/writer.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
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

from nucliadb_protos.noderesources_pb2 import *
from nucliadb_protos.resources_pb2 import *
from nucliadb_protos.knowledgebox_pb2 import *

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1cnucliadb_protos/writer.proto\x12\tfdbwriter\x1a\x1fgoogle/protobuf/timestamp.proto\x1a#nucliadb_protos/noderesources.proto\x1a\x1fnucliadb_protos/resources.proto\x1a\"nucliadb_protos/knowledgebox.proto\"\xa8\x01\n\x05\x41udit\x12\x0c\n\x04user\x18\x01 \x01(\t\x12(\n\x04when\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0e\n\x06origin\x18\x03 \x01(\t\x12\'\n\x06source\x18\x04 \x01(\x0e\x32\x17.fdbwriter.Audit.Source\".\n\x06Source\x12\x08\n\x04HTTP\x10\x00\x12\r\n\tDASHBOARD\x10\x01\x12\x0b\n\x07\x44\x45SKTOP\x10\x02\"\xad\x01\n\x05\x45rror\x12\r\n\x05\x66ield\x18\x01 \x01(\t\x12(\n\nfield_type\x18\x02 \x01(\x0e\x32\x14.resources.FieldType\x12\r\n\x05\x65rror\x18\x03 \x01(\t\x12(\n\x04\x63ode\x18\x04 \x01(\x0e\x32\x1a.fdbwriter.Error.ErrorCode\"2\n\tErrorCode\x12\x0b\n\x07GENERIC\x10\x00\x12\x0b\n\x07\x45XTRACT\x10\x01\x12\x0b\n\x07PROCESS\x10\x02\"\x81\x10\n\rBrokerMessage\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\x0c\n\x04uuid\x18\x03 \x01(\t\x12\x0c\n\x04slug\x18\x04 \x01(\t\x12\x1f\n\x05\x61udit\x18\x05 \x01(\x0b\x32\x10.fdbwriter.Audit\x12\x32\n\x04type\x18\x06 \x01(\x0e\x32$.fdbwriter.BrokerMessage.MessageType\x12\x0f\n\x07multiid\x18\x07 \x01(\t\x12\x1f\n\x05\x62\x61sic\x18\x08 \x01(\x0b\x32\x10.resources.Basic\x12!\n\x06origin\x18\t \x01(\x0b\x32\x11.resources.Origin\x12\"\n\trelations\x18\n \x03(\x0b\x32\x0f.utils.Relation\x12\x42\n\rconversations\x18\x0b \x03(\x0b\x32+.fdbwriter.BrokerMessage.ConversationsEntry\x12\x36\n\x07layouts\x18\x0c \x03(\x0b\x32%.fdbwriter.BrokerMessage.LayoutsEntry\x12\x32\n\x05texts\x18\r \x03(\x0b\x32#.fdbwriter.BrokerMessage.TextsEntry\x12>\n\x0bkeywordsets\x18\x0e \x03(\x0b\x32).fdbwriter.BrokerMessage.KeywordsetsEntry\x12:\n\tdatetimes\x18\x0f \x03(\x0b\x32\'.fdbwriter.BrokerMessage.DatetimesEntry\x12\x32\n\x05links\x18\x10 \x03(\x0b\x32#.fdbwriter.BrokerMessage.LinksEntry\x12\x32\n\x05\x66iles\x18\x11 \x03(\x0b\x32#.fdbwriter.BrokerMessage.FilesEntry\x12\x39\n\x13link_extracted_data\x18\x12 \x03(\x0b\x32\x1c.resources.LinkExtractedData\x12\x39\n\x13\x66ile_extracted_data\x18\x13 \x03(\x0b\x32\x1c.resources.FileExtractedData\x12\x37\n\x0e\x65xtracted_text\x18\x14 \x03(\x0b\x32\x1f.resources.ExtractedTextWrapper\x12?\n\x0e\x66ield_metadata\x18\x15 \x03(\x0b\x32\'.resources.FieldComputedMetadataWrapper\x12\x39\n\rfield_vectors\x18\x16 \x03(\x0b\x32\".resources.ExtractedVectorsWrapper\x12\x45\n\x14\x66ield_large_metadata\x18\x17 \x03(\x0b\x32\'.resources.LargeComputedMetadataWrapper\x12)\n\rdelete_fields\x18\x18 \x03(\x0b\x32\x12.resources.FieldID\x12\x12\n\norigin_seq\x18\x19 \x01(\x05\x12\x1c\n\x14slow_processing_time\x18\x1a \x01(\x02\x12\x1b\n\x13pre_processing_time\x18\x1c \x01(\x02\x12-\n\tdone_time\x18\x1d \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x13\n\x07txseqid\x18\x1e \x01(\x03\x42\x02\x18\x01\x12 \n\x06\x65rrors\x18\x1f \x03(\x0b\x32\x10.fdbwriter.Error\x12\x15\n\rprocessing_id\x18  \x01(\t\x12\x36\n\x06source\x18! \x01(\x0e\x32&.fdbwriter.BrokerMessage.MessageSource\x12\x13\n\x0b\x61\x63\x63ount_seq\x18\" \x01(\x03\x12\x33\n\x0cuser_vectors\x18# \x03(\x0b\x32\x1d.resources.UserVectorsWrapper\x1aM\n\x12\x43onversationsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12&\n\x05value\x18\x02 \x01(\x0b\x32\x17.resources.Conversation:\x02\x38\x01\x1a\x46\n\x0cLayoutsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12%\n\x05value\x18\x02 \x01(\x0b\x32\x16.resources.FieldLayout:\x02\x38\x01\x1a\x42\n\nTextsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.resources.FieldText:\x02\x38\x01\x1aN\n\x10KeywordsetsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12)\n\x05value\x18\x02 \x01(\x0b\x32\x1a.resources.FieldKeywordset:\x02\x38\x01\x1aJ\n\x0e\x44\x61tetimesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\'\n\x05value\x18\x02 \x01(\x0b\x32\x18.resources.FieldDatetime:\x02\x38\x01\x1a\x42\n\nLinksEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.resources.FieldLink:\x02\x38\x01\x1a\x42\n\nFilesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.resources.FieldFile:\x02\x38\x01\"N\n\x0bMessageType\x12\x0e\n\nAUTOCOMMIT\x10\x00\x12\t\n\x05MULTI\x10\x01\x12\n\n\x06\x43OMMIT\x10\x02\x12\x0c\n\x08ROLLBACK\x10\x03\x12\n\n\x06\x44\x45LETE\x10\x04\"*\n\rMessageSource\x12\n\n\x06WRITER\x10\x00\x12\r\n\tPROCESSOR\x10\x01\"\x97\x01\n\x14WriterStatusResponse\x12\x16\n\x0eknowledgeboxes\x18\x01 \x03(\t\x12\x39\n\x05msgid\x18\x02 \x03(\x0b\x32*.fdbwriter.WriterStatusResponse.MsgidEntry\x1a,\n\nMsgidEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x03:\x02\x38\x01\"\x15\n\x13WriterStatusRequest\"r\n\x10SetLabelsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\n\n\x02id\x18\x02 \x01(\t\x12(\n\x08labelset\x18\x03 \x01(\x0b\x32\x16.knowledgebox.LabelSet\"H\n\x10\x44\x65lLabelsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\n\n\x02id\x18\x02 \x01(\t\"\xb8\x01\n\x11GetLabelsResponse\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12$\n\x06labels\x18\x02 \x01(\x0b\x32\x14.knowledgebox.Labels\x12\x33\n\x06status\x18\x03 \x01(\x0e\x32#.fdbwriter.GetLabelsResponse.Status\"\x1e\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\"<\n\x10GetLabelsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\"|\n\x12SetEntitiesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\r\n\x05group\x18\x02 \x01(\t\x12-\n\x08\x65ntities\x18\x03 \x01(\x0b\x32\x1b.knowledgebox.EntitiesGroup\">\n\x12GetEntitiesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\"\x9e\x02\n\x13GetEntitiesResponse\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12:\n\x06groups\x18\x02 \x03(\x0b\x32*.fdbwriter.GetEntitiesResponse.GroupsEntry\x12\x35\n\x06status\x18\x03 \x01(\x0e\x32%.fdbwriter.GetEntitiesResponse.Status\x1aJ\n\x0bGroupsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12*\n\x05value\x18\x02 \x01(\x0b\x32\x1b.knowledgebox.EntitiesGroup:\x02\x38\x01\"\x1e\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\"M\n\x12\x44\x65lEntitiesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\r\n\x05group\x18\x02 \x01(\t\"\xd9\x01\n\x14MergeEntitiesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x36\n\x04\x66rom\x18\x02 \x01(\x0b\x32(.fdbwriter.MergeEntitiesRequest.EntityID\x12\x34\n\x02to\x18\x03 \x01(\x0b\x32(.fdbwriter.MergeEntitiesRequest.EntityID\x1a)\n\x08\x45ntityID\x12\r\n\x05group\x18\x01 \x01(\t\x12\x0e\n\x06\x65ntity\x18\x02 \x01(\t\"P\n\x12GetLabelSetRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x10\n\x08labelset\x18\x02 \x01(\t\"\xc0\x01\n\x13GetLabelSetResponse\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12(\n\x08labelset\x18\x02 \x01(\x0b\x32\x16.knowledgebox.LabelSet\x12\x35\n\x06status\x18\x03 \x01(\x0e\x32%.fdbwriter.GetLabelSetResponse.Status\"\x1e\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\"R\n\x17GetEntitiesGroupRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\r\n\x05group\x18\x02 \x01(\t\"\xcc\x01\n\x18GetEntitiesGroupResponse\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12*\n\x05group\x18\x02 \x01(\x0b\x32\x1b.knowledgebox.EntitiesGroup\x12:\n\x06status\x18\x03 \x01(\x0e\x32*.fdbwriter.GetEntitiesGroupResponse.Status\"\x1e\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\"L\n\x10GetWidgetRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x0e\n\x06widget\x18\x02 \x01(\t\"\xb8\x01\n\x11GetWidgetResponse\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12$\n\x06widget\x18\x02 \x01(\x0b\x32\x14.knowledgebox.Widget\x12\x33\n\x06status\x18\x03 \x01(\x0e\x32#.fdbwriter.GetWidgetResponse.Status\"\x1e\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\"@\n\x14GetVectorSetsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\"\xd3\x01\n\x15GetVectorSetsResponse\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12,\n\nvectorsets\x18\x02 \x01(\x0b\x32\x18.knowledgebox.VectorSets\x12\x37\n\x06status\x18\x03 \x01(\x0e\x32\'.fdbwriter.GetVectorSetsResponse.Status\")\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\x12\t\n\x05\x45RROR\x10\x02\"R\n\x13\x44\x65lVectorSetRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x11\n\tvectorset\x18\x02 \x01(\t\"w\n\x13SetVectorSetRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\n\n\x02id\x18\x02 \x01(\t\x12*\n\tvectorset\x18\x03 \x01(\x0b\x32\x17.knowledgebox.VectorSet\"=\n\x11GetWidgetsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\"\x97\x02\n\x12GetWidgetsResponse\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12;\n\x07widgets\x18\x02 \x03(\x0b\x32*.fdbwriter.GetWidgetsResponse.WidgetsEntry\x12\x34\n\x06status\x18\x03 \x01(\x0e\x32$.fdbwriter.GetWidgetsResponse.Status\x1a\x44\n\x0cWidgetsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.knowledgebox.Widget:\x02\x38\x01\"\x1e\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\"c\n\x11SetWidgetsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12$\n\x06widget\x18\x02 \x01(\x0b\x32\x14.knowledgebox.Widget\"M\n\x11\x44\x65tWidgetsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x0e\n\x06widget\x18\x02 \x01(\t\"m\n\x0eOpStatusWriter\x12\x30\n\x06status\x18\x01 \x01(\x0e\x32 .fdbwriter.OpStatusWriter.Status\")\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\t\n\x05\x45RROR\x10\x01\x12\x0c\n\x08NOTFOUND\x10\x02\"\xac\x01\n\x0cNotification\x12\x11\n\tpartition\x18\x01 \x01(\x05\x12\r\n\x05multi\x18\x02 \x01(\t\x12\x0c\n\x04uuid\x18\x03 \x01(\t\x12\x0c\n\x04kbid\x18\x04 \x01(\t\x12\r\n\x05seqid\x18\x05 \x01(\x03\x12.\n\x06\x61\x63tion\x18\x06 \x01(\x0e\x32\x1e.fdbwriter.Notification.Action\"\x1f\n\x06\x41\x63tion\x12\n\n\x06\x43OMMIT\x10\x00\x12\t\n\x05\x41\x42ORT\x10\x01\"\xdb\x01\n\x06Member\x12\n\n\x02id\x18\x01 \x01(\t\x12\x16\n\x0elisten_address\x18\x02 \x01(\t\x12\x0f\n\x07is_self\x18\x03 \x01(\x08\x12$\n\x04type\x18\x04 \x01(\x0e\x32\x16.fdbwriter.Member.Type\x12\r\n\x05\x64ummy\x18\x05 \x01(\x08\x12\x12\n\nload_score\x18\x06 \x01(\x02\x12\x13\n\x0bshard_count\x18\x07 \x01(\r\">\n\x04Type\x12\x06\n\x02IO\x10\x00\x12\n\n\x06SEARCH\x10\x01\x12\n\n\x06INGEST\x10\x02\x12\t\n\x05TRAIN\x10\x03\x12\x0b\n\x07UNKNOWN\x10\x04\"\x14\n\x12ListMembersRequest\"9\n\x13ListMembersResponse\x12\"\n\x07members\x18\x01 \x03(\x0b\x32\x11.fdbwriter.Member\"B\n\x0bShadowShard\x12%\n\x05shard\x18\x01 \x01(\x0b\x32\x16.noderesources.ShardId\x12\x0c\n\x04node\x18\x02 \x01(\t\"x\n\x0cShardReplica\x12*\n\x05shard\x18\x01 \x01(\x0b\x32\x1b.noderesources.ShardCreated\x12\x0c\n\x04node\x18\x02 \x01(\t\x12.\n\x0eshadow_replica\x18\x03 \x01(\x0b\x32\x16.fdbwriter.ShadowShard\"v\n\x0bShardObject\x12\r\n\x05shard\x18\x01 \x01(\t\x12)\n\x08replicas\x18\x03 \x03(\x0b\x32\x17.fdbwriter.ShardReplica\x12-\n\ttimestamp\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\"N\n\x06Shards\x12&\n\x06shards\x18\x01 \x03(\x0b\x32\x16.fdbwriter.ShardObject\x12\x0c\n\x04kbid\x18\x02 \x01(\t\x12\x0e\n\x06\x61\x63tual\x18\x03 \x01(\x05\"e\n\x0fResourceFieldId\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\x0b\n\x03rid\x18\x02 \x01(\t\x12(\n\nfield_type\x18\x03 \x01(\x0e\x32\x14.resources.FieldType\x12\r\n\x05\x66ield\x18\x04 \x01(\t\"C\n\rIndexResource\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\x0b\n\x03rid\x18\x02 \x01(\t\x12\x17\n\x0freindex_vectors\x18\x03 \x01(\x08\"\r\n\x0bIndexStatus\",\n\x1bResourceFieldExistsResponse\x12\r\n\x05\x66ound\x18\x01 \x01(\x08\"/\n\x11ResourceIdRequest\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\x0c\n\x04slug\x18\x02 \x01(\t\"\"\n\x12ResourceIdResponse\x12\x0c\n\x04uuid\x18\x01 \x01(\t\"\x1d\n\rExportRequest\x12\x0c\n\x04kbid\x18\x01 \x01(\t\"w\n\x11SetVectorsRequest\x12$\n\x07vectors\x18\x01 \x01(\x0b\x32\x13.utils.VectorObject\x12\x0c\n\x04kbid\x18\x02 \x01(\t\x12\x0b\n\x03rid\x18\x03 \x01(\t\x12!\n\x05\x66ield\x18\x04 \x01(\x0b\x32\x12.resources.FieldID\"#\n\x12SetVectorsResponse\x12\r\n\x05\x66ound\x18\x01 \x01(\x08\"*\n\x0b\x46ileRequest\x12\x0e\n\x06\x62ucket\x18\x01 \x01(\t\x12\x0b\n\x03key\x18\x02 \x01(\t\"\x1a\n\nBinaryData\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\x0c\"a\n\x0e\x42inaryMetadata\x12\x0c\n\x04kbid\x18\x02 \x01(\t\x12\x0b\n\x03key\x18\x03 \x01(\t\x12\x0c\n\x04size\x18\x04 \x01(\x05\x12\x10\n\x08\x66ilename\x18\x05 \x01(\t\x12\x14\n\x0c\x63ontent_type\x18\x06 \x01(\t\"k\n\x10UploadBinaryData\x12\r\n\x05\x63ount\x18\x01 \x01(\x05\x12-\n\x08metadata\x18\x02 \x01(\x0b\x32\x19.fdbwriter.BinaryMetadataH\x00\x12\x11\n\x07payload\x18\x03 \x01(\x0cH\x00\x42\x06\n\x04\x64\x61ta\"\x0e\n\x0c\x46ileUploaded\"_\n\x18\x43reateShadowShardRequest\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\'\n\x07replica\x18\x02 \x01(\x0b\x32\x16.noderesources.ShardId\x12\x0c\n\x04node\x18\x03 \x01(\t\"Q\n\x18\x44\x65leteShadowShardRequest\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\'\n\x07replica\x18\x02 \x01(\x0b\x32\x16.noderesources.ShardId\"T\n\x13ShadowShardResponse\x12,\n\x0cshadow_shard\x18\x01 \x01(\x0b\x32\x16.fdbwriter.ShadowShard\x12\x0f\n\x07success\x18\x02 \x01(\x08\"\x1f\n\x0fSynonymsRequest\x12\x0c\n\x04kbid\x18\x01 \x01(\t\"j\n\x12SetSynonymsRequest\x12*\n\x04kbid\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12(\n\x08synonyms\x18\x02 \x01(\x0b\x32\x16.knowledgebox.Synonyms\"j\n\x13GetSynonymsResponse\x12)\n\x06status\x18\x01 \x01(\x0b\x32\x19.fdbwriter.OpStatusWriter\x12(\n\x08synonyms\x18\x02 \x01(\x0b\x32\x16.knowledgebox.Synonyms2\xe0\x17\n\x06Writer\x12M\n\x0fGetKnowledgeBox\x12\x1c.knowledgebox.KnowledgeBoxID\x1a\x1a.knowledgebox.KnowledgeBox\"\x00\x12Y\n\x0fNewKnowledgeBox\x12\x1d.knowledgebox.KnowledgeBoxNew\x1a%.knowledgebox.NewKnowledgeBoxResponse\"\x00\x12^\n\x12\x44\x65leteKnowledgeBox\x12\x1c.knowledgebox.KnowledgeBoxID\x1a(.knowledgebox.DeleteKnowledgeBoxResponse\"\x00\x12\x62\n\x12UpdateKnowledgeBox\x12 .knowledgebox.KnowledgeBoxUpdate\x1a(.knowledgebox.UpdateKnowledgeBoxResponse\"\x00\x12m\n CleanAndUpgradeKnowledgeBoxIndex\x12\x1c.knowledgebox.KnowledgeBoxID\x1a).knowledgebox.CleanedKnowledgeBoxResponse\"\x00\x12V\n\x10ListKnowledgeBox\x12 .knowledgebox.KnowledgeBoxPrefix\x1a\x1c.knowledgebox.KnowledgeBoxID\"\x00\x30\x01\x12V\n\x0eGCKnowledgeBox\x12\x1c.knowledgebox.KnowledgeBoxID\x1a$.knowledgebox.GCKnowledgeBoxResponse\"\x00\x12K\n\nSetVectors\x12\x1c.fdbwriter.SetVectorsRequest\x1a\x1d.fdbwriter.SetVectorsResponse\"\x00\x12[\n\x13ResourceFieldExists\x12\x1a.fdbwriter.ResourceFieldId\x1a&.fdbwriter.ResourceFieldExistsResponse\"\x00\x12N\n\rGetResourceId\x12\x1c.fdbwriter.ResourceIdRequest\x1a\x1d.fdbwriter.ResourceIdResponse\"\x00\x12I\n\x0eProcessMessage\x12\x18.fdbwriter.BrokerMessage\x1a\x19.fdbwriter.OpStatusWriter\"\x00(\x01\x12H\n\tGetLabels\x12\x1b.fdbwriter.GetLabelsRequest\x1a\x1c.fdbwriter.GetLabelsResponse\"\x00\x12N\n\x0bGetLabelSet\x12\x1d.fdbwriter.GetLabelSetRequest\x1a\x1e.fdbwriter.GetLabelSetResponse\"\x00\x12\x45\n\tSetLabels\x12\x1b.fdbwriter.SetLabelsRequest\x1a\x19.fdbwriter.OpStatusWriter\"\x00\x12\x45\n\tDelLabels\x12\x1b.fdbwriter.DelLabelsRequest\x1a\x19.fdbwriter.OpStatusWriter\"\x00\x12T\n\rGetVectorSets\x12\x1f.fdbwriter.GetVectorSetsRequest\x1a .fdbwriter.GetVectorSetsResponse\"\x00\x12K\n\x0c\x44\x65lVectorSet\x12\x1e.fdbwriter.DelVectorSetRequest\x1a\x19.fdbwriter.OpStatusWriter\"\x00\x12K\n\x0cSetVectorSet\x12\x1e.fdbwriter.SetVectorSetRequest\x1a\x19.fdbwriter.OpStatusWriter\"\x00\x12N\n\x0bGetEntities\x12\x1d.fdbwriter.GetEntitiesRequest\x1a\x1e.fdbwriter.GetEntitiesResponse\"\x00\x12]\n\x10GetEntitiesGroup\x12\".fdbwriter.GetEntitiesGroupRequest\x1a#.fdbwriter.GetEntitiesGroupResponse\"\x00\x12I\n\x0bSetEntities\x12\x1d.fdbwriter.SetEntitiesRequest\x1a\x19.fdbwriter.OpStatusWriter\"\x00\x12I\n\x0b\x44\x65lEntities\x12\x1d.fdbwriter.DelEntitiesRequest\x1a\x19.fdbwriter.OpStatusWriter\"\x00\x12H\n\tGetWidget\x12\x1b.fdbwriter.GetWidgetRequest\x1a\x1c.fdbwriter.GetWidgetResponse\"\x00\x12K\n\nGetWidgets\x12\x1c.fdbwriter.GetWidgetsRequest\x1a\x1d.fdbwriter.GetWidgetsResponse\"\x00\x12G\n\nSetWidgets\x12\x1c.fdbwriter.SetWidgetsRequest\x1a\x19.fdbwriter.OpStatusWriter\"\x00\x12G\n\nDelWidgets\x12\x1c.fdbwriter.DetWidgetsRequest\x1a\x19.fdbwriter.OpStatusWriter\"\x00\x12M\n\x0bGetSynonyms\x12\x1c.knowledgebox.KnowledgeBoxID\x1a\x1e.fdbwriter.GetSynonymsResponse\"\x00\x12I\n\x0bSetSynonyms\x12\x1d.fdbwriter.SetSynonymsRequest\x1a\x19.fdbwriter.OpStatusWriter\"\x00\x12H\n\x0b\x44\x65lSynonyms\x12\x1c.knowledgebox.KnowledgeBoxID\x1a\x19.fdbwriter.OpStatusWriter\"\x00\x12K\n\x06Status\x12\x1e.fdbwriter.WriterStatusRequest\x1a\x1f.fdbwriter.WriterStatusResponse\"\x00\x12L\n\x0bListMembers\x12\x1d.fdbwriter.ListMembersRequest\x1a\x1e.fdbwriter.ListMembersResponse\x12;\n\x05Index\x12\x18.fdbwriter.IndexResource\x1a\x16.fdbwriter.IndexStatus\"\x00\x12=\n\x07ReIndex\x12\x18.fdbwriter.IndexResource\x1a\x16.fdbwriter.IndexStatus\"\x00\x12@\n\x06\x45xport\x12\x18.fdbwriter.ExportRequest\x1a\x18.fdbwriter.BrokerMessage\"\x00\x30\x01\x12\x41\n\x0c\x44ownloadFile\x12\x16.fdbwriter.FileRequest\x1a\x15.fdbwriter.BinaryData\"\x00\x30\x01\x12\x46\n\nUploadFile\x12\x1b.fdbwriter.UploadBinaryData\x1a\x17.fdbwriter.FileUploaded\"\x00(\x01\x12Z\n\x11\x43reateShadowShard\x12#.fdbwriter.CreateShadowShardRequest\x1a\x1e.fdbwriter.ShadowShardResponse\"\x00\x12Z\n\x11\x44\x65leteShadowShard\x12#.fdbwriter.DeleteShadowShardRequest\x1a\x1e.fdbwriter.ShadowShardResponse\"\x00P\x01P\x02P\x03\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.writer_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _BROKERMESSAGE_CONVERSATIONSENTRY._options = None
  _BROKERMESSAGE_CONVERSATIONSENTRY._serialized_options = b'8\001'
  _BROKERMESSAGE_LAYOUTSENTRY._options = None
  _BROKERMESSAGE_LAYOUTSENTRY._serialized_options = b'8\001'
  _BROKERMESSAGE_TEXTSENTRY._options = None
  _BROKERMESSAGE_TEXTSENTRY._serialized_options = b'8\001'
  _BROKERMESSAGE_KEYWORDSETSENTRY._options = None
  _BROKERMESSAGE_KEYWORDSETSENTRY._serialized_options = b'8\001'
  _BROKERMESSAGE_DATETIMESENTRY._options = None
  _BROKERMESSAGE_DATETIMESENTRY._serialized_options = b'8\001'
  _BROKERMESSAGE_LINKSENTRY._options = None
  _BROKERMESSAGE_LINKSENTRY._serialized_options = b'8\001'
  _BROKERMESSAGE_FILESENTRY._options = None
  _BROKERMESSAGE_FILESENTRY._serialized_options = b'8\001'
  _BROKERMESSAGE.fields_by_name['txseqid']._options = None
  _BROKERMESSAGE.fields_by_name['txseqid']._serialized_options = b'\030\001'
  _WRITERSTATUSRESPONSE_MSGIDENTRY._options = None
  _WRITERSTATUSRESPONSE_MSGIDENTRY._serialized_options = b'8\001'
  _GETENTITIESRESPONSE_GROUPSENTRY._options = None
  _GETENTITIESRESPONSE_GROUPSENTRY._serialized_options = b'8\001'
  _GETWIDGETSRESPONSE_WIDGETSENTRY._options = None
  _GETWIDGETSRESPONSE_WIDGETSENTRY._serialized_options = b'8\001'
  _AUDIT._serialized_start=183
  _AUDIT._serialized_end=351
  _AUDIT_SOURCE._serialized_start=305
  _AUDIT_SOURCE._serialized_end=351
  _ERROR._serialized_start=354
  _ERROR._serialized_end=527
  _ERROR_ERRORCODE._serialized_start=477
  _ERROR_ERRORCODE._serialized_end=527
  _BROKERMESSAGE._serialized_start=530
  _BROKERMESSAGE._serialized_end=2579
  _BROKERMESSAGE_CONVERSATIONSENTRY._serialized_start=1946
  _BROKERMESSAGE_CONVERSATIONSENTRY._serialized_end=2023
  _BROKERMESSAGE_LAYOUTSENTRY._serialized_start=2025
  _BROKERMESSAGE_LAYOUTSENTRY._serialized_end=2095
  _BROKERMESSAGE_TEXTSENTRY._serialized_start=2097
  _BROKERMESSAGE_TEXTSENTRY._serialized_end=2163
  _BROKERMESSAGE_KEYWORDSETSENTRY._serialized_start=2165
  _BROKERMESSAGE_KEYWORDSETSENTRY._serialized_end=2243
  _BROKERMESSAGE_DATETIMESENTRY._serialized_start=2245
  _BROKERMESSAGE_DATETIMESENTRY._serialized_end=2319
  _BROKERMESSAGE_LINKSENTRY._serialized_start=2321
  _BROKERMESSAGE_LINKSENTRY._serialized_end=2387
  _BROKERMESSAGE_FILESENTRY._serialized_start=2389
  _BROKERMESSAGE_FILESENTRY._serialized_end=2455
  _BROKERMESSAGE_MESSAGETYPE._serialized_start=2457
  _BROKERMESSAGE_MESSAGETYPE._serialized_end=2535
  _BROKERMESSAGE_MESSAGESOURCE._serialized_start=2537
  _BROKERMESSAGE_MESSAGESOURCE._serialized_end=2579
  _WRITERSTATUSRESPONSE._serialized_start=2582
  _WRITERSTATUSRESPONSE._serialized_end=2733
  _WRITERSTATUSRESPONSE_MSGIDENTRY._serialized_start=2689
  _WRITERSTATUSRESPONSE_MSGIDENTRY._serialized_end=2733
  _WRITERSTATUSREQUEST._serialized_start=2735
  _WRITERSTATUSREQUEST._serialized_end=2756
  _SETLABELSREQUEST._serialized_start=2758
  _SETLABELSREQUEST._serialized_end=2872
  _DELLABELSREQUEST._serialized_start=2874
  _DELLABELSREQUEST._serialized_end=2946
  _GETLABELSRESPONSE._serialized_start=2949
  _GETLABELSRESPONSE._serialized_end=3133
  _GETLABELSRESPONSE_STATUS._serialized_start=3103
  _GETLABELSRESPONSE_STATUS._serialized_end=3133
  _GETLABELSREQUEST._serialized_start=3135
  _GETLABELSREQUEST._serialized_end=3195
  _SETENTITIESREQUEST._serialized_start=3197
  _SETENTITIESREQUEST._serialized_end=3321
  _GETENTITIESREQUEST._serialized_start=3323
  _GETENTITIESREQUEST._serialized_end=3385
  _GETENTITIESRESPONSE._serialized_start=3388
  _GETENTITIESRESPONSE._serialized_end=3674
  _GETENTITIESRESPONSE_GROUPSENTRY._serialized_start=3568
  _GETENTITIESRESPONSE_GROUPSENTRY._serialized_end=3642
  _GETENTITIESRESPONSE_STATUS._serialized_start=3103
  _GETENTITIESRESPONSE_STATUS._serialized_end=3133
  _DELENTITIESREQUEST._serialized_start=3676
  _DELENTITIESREQUEST._serialized_end=3753
  _MERGEENTITIESREQUEST._serialized_start=3756
  _MERGEENTITIESREQUEST._serialized_end=3973
  _MERGEENTITIESREQUEST_ENTITYID._serialized_start=3932
  _MERGEENTITIESREQUEST_ENTITYID._serialized_end=3973
  _GETLABELSETREQUEST._serialized_start=3975
  _GETLABELSETREQUEST._serialized_end=4055
  _GETLABELSETRESPONSE._serialized_start=4058
  _GETLABELSETRESPONSE._serialized_end=4250
  _GETLABELSETRESPONSE_STATUS._serialized_start=3103
  _GETLABELSETRESPONSE_STATUS._serialized_end=3133
  _GETENTITIESGROUPREQUEST._serialized_start=4252
  _GETENTITIESGROUPREQUEST._serialized_end=4334
  _GETENTITIESGROUPRESPONSE._serialized_start=4337
  _GETENTITIESGROUPRESPONSE._serialized_end=4541
  _GETENTITIESGROUPRESPONSE_STATUS._serialized_start=3103
  _GETENTITIESGROUPRESPONSE_STATUS._serialized_end=3133
  _GETWIDGETREQUEST._serialized_start=4543
  _GETWIDGETREQUEST._serialized_end=4619
  _GETWIDGETRESPONSE._serialized_start=4622
  _GETWIDGETRESPONSE._serialized_end=4806
  _GETWIDGETRESPONSE_STATUS._serialized_start=3103
  _GETWIDGETRESPONSE_STATUS._serialized_end=3133
  _GETVECTORSETSREQUEST._serialized_start=4808
  _GETVECTORSETSREQUEST._serialized_end=4872
  _GETVECTORSETSRESPONSE._serialized_start=4875
  _GETVECTORSETSRESPONSE._serialized_end=5086
  _GETVECTORSETSRESPONSE_STATUS._serialized_start=5045
  _GETVECTORSETSRESPONSE_STATUS._serialized_end=5086
  _DELVECTORSETREQUEST._serialized_start=5088
  _DELVECTORSETREQUEST._serialized_end=5170
  _SETVECTORSETREQUEST._serialized_start=5172
  _SETVECTORSETREQUEST._serialized_end=5291
  _GETWIDGETSREQUEST._serialized_start=5293
  _GETWIDGETSREQUEST._serialized_end=5354
  _GETWIDGETSRESPONSE._serialized_start=5357
  _GETWIDGETSRESPONSE._serialized_end=5636
  _GETWIDGETSRESPONSE_WIDGETSENTRY._serialized_start=5536
  _GETWIDGETSRESPONSE_WIDGETSENTRY._serialized_end=5604
  _GETWIDGETSRESPONSE_STATUS._serialized_start=3103
  _GETWIDGETSRESPONSE_STATUS._serialized_end=3133
  _SETWIDGETSREQUEST._serialized_start=5638
  _SETWIDGETSREQUEST._serialized_end=5737
  _DETWIDGETSREQUEST._serialized_start=5739
  _DETWIDGETSREQUEST._serialized_end=5816
  _OPSTATUSWRITER._serialized_start=5818
  _OPSTATUSWRITER._serialized_end=5927
  _OPSTATUSWRITER_STATUS._serialized_start=5886
  _OPSTATUSWRITER_STATUS._serialized_end=5927
  _NOTIFICATION._serialized_start=5930
  _NOTIFICATION._serialized_end=6102
  _NOTIFICATION_ACTION._serialized_start=6071
  _NOTIFICATION_ACTION._serialized_end=6102
  _MEMBER._serialized_start=6105
  _MEMBER._serialized_end=6324
  _MEMBER_TYPE._serialized_start=6262
  _MEMBER_TYPE._serialized_end=6324
  _LISTMEMBERSREQUEST._serialized_start=6326
  _LISTMEMBERSREQUEST._serialized_end=6346
  _LISTMEMBERSRESPONSE._serialized_start=6348
  _LISTMEMBERSRESPONSE._serialized_end=6405
  _SHADOWSHARD._serialized_start=6407
  _SHADOWSHARD._serialized_end=6473
  _SHARDREPLICA._serialized_start=6475
  _SHARDREPLICA._serialized_end=6595
  _SHARDOBJECT._serialized_start=6597
  _SHARDOBJECT._serialized_end=6715
  _SHARDS._serialized_start=6717
<<<<<<< HEAD
  _SHARDS._serialized_end=6795
  _RESOURCEFIELDID._serialized_start=6797
  _RESOURCEFIELDID._serialized_end=6898
  _INDEXRESOURCE._serialized_start=6900
  _INDEXRESOURCE._serialized_end=6967
  _INDEXSTATUS._serialized_start=6969
  _INDEXSTATUS._serialized_end=6982
  _RESOURCEFIELDEXISTSRESPONSE._serialized_start=6984
  _RESOURCEFIELDEXISTSRESPONSE._serialized_end=7028
  _RESOURCEIDREQUEST._serialized_start=7030
  _RESOURCEIDREQUEST._serialized_end=7077
  _RESOURCEIDRESPONSE._serialized_start=7079
  _RESOURCEIDRESPONSE._serialized_end=7113
  _EXPORTREQUEST._serialized_start=7115
  _EXPORTREQUEST._serialized_end=7144
  _SETVECTORSREQUEST._serialized_start=7146
  _SETVECTORSREQUEST._serialized_end=7265
  _SETVECTORSRESPONSE._serialized_start=7267
  _SETVECTORSRESPONSE._serialized_end=7302
  _FILEREQUEST._serialized_start=7304
  _FILEREQUEST._serialized_end=7346
  _BINARYDATA._serialized_start=7348
  _BINARYDATA._serialized_end=7374
  _BINARYMETADATA._serialized_start=7376
  _BINARYMETADATA._serialized_end=7473
  _UPLOADBINARYDATA._serialized_start=7475
  _UPLOADBINARYDATA._serialized_end=7582
  _FILEUPLOADED._serialized_start=7584
  _FILEUPLOADED._serialized_end=7598
  _CREATESHADOWSHARDREQUEST._serialized_start=7600
  _CREATESHADOWSHARDREQUEST._serialized_end=7695
  _DELETESHADOWSHARDREQUEST._serialized_start=7697
  _DELETESHADOWSHARDREQUEST._serialized_end=7778
  _SHADOWSHARDRESPONSE._serialized_start=7780
  _SHADOWSHARDRESPONSE._serialized_end=7864
  _SYNONYMSREQUEST._serialized_start=7866
  _SYNONYMSREQUEST._serialized_end=7897
  _SETSYNONYMSREQUEST._serialized_start=7899
  _SETSYNONYMSREQUEST._serialized_end=8005
  _GETSYNONYMSRESPONSE._serialized_start=8007
  _GETSYNONYMSRESPONSE._serialized_end=8113
  _WRITER._serialized_start=8116
  _WRITER._serialized_end=11156
=======
  _SHARDS._serialized_end=6840
  _RESOURCEFIELDID._serialized_start=6842
  _RESOURCEFIELDID._serialized_end=6943
  _INDEXRESOURCE._serialized_start=6945
  _INDEXRESOURCE._serialized_end=7012
  _INDEXSTATUS._serialized_start=7014
  _INDEXSTATUS._serialized_end=7027
  _RESOURCEFIELDEXISTSRESPONSE._serialized_start=7029
  _RESOURCEFIELDEXISTSRESPONSE._serialized_end=7073
  _RESOURCEIDREQUEST._serialized_start=7075
  _RESOURCEIDREQUEST._serialized_end=7122
  _RESOURCEIDRESPONSE._serialized_start=7124
  _RESOURCEIDRESPONSE._serialized_end=7158
  _EXPORTREQUEST._serialized_start=7160
  _EXPORTREQUEST._serialized_end=7189
  _SETVECTORSREQUEST._serialized_start=7191
  _SETVECTORSREQUEST._serialized_end=7310
  _SETVECTORSRESPONSE._serialized_start=7312
  _SETVECTORSRESPONSE._serialized_end=7347
  _FILEREQUEST._serialized_start=7349
  _FILEREQUEST._serialized_end=7391
  _BINARYDATA._serialized_start=7393
  _BINARYDATA._serialized_end=7419
  _BINARYMETADATA._serialized_start=7421
  _BINARYMETADATA._serialized_end=7518
  _UPLOADBINARYDATA._serialized_start=7520
  _UPLOADBINARYDATA._serialized_end=7627
  _FILEUPLOADED._serialized_start=7629
  _FILEUPLOADED._serialized_end=7643
  _CREATESHADOWSHARDREQUEST._serialized_start=7645
  _CREATESHADOWSHARDREQUEST._serialized_end=7740
  _DELETESHADOWSHARDREQUEST._serialized_start=7742
  _DELETESHADOWSHARDREQUEST._serialized_end=7823
  _SHADOWSHARDRESPONSE._serialized_start=7825
  _SHADOWSHARDRESPONSE._serialized_end=7909
  _WRITER._serialized_start=7912
  _WRITER._serialized_end=10724
>>>>>>> 0543cfe0 (Store similarity in shards object)
# @@protoc_insertion_point(module_scope)
