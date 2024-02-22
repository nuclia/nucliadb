# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/noderesources.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from nucliadb_protos import utils_pb2 as nucliadb__protos_dot_utils__pb2

from nucliadb_protos.utils_pb2 import *

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n#nucliadb_protos/noderesources.proto\x12\rnoderesources\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1bnucliadb_protos/utils.proto\"/\n\x0fTextInformation\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x0e\n\x06labels\x18\x02 \x03(\t\"j\n\rIndexMetadata\x12,\n\x08modified\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12+\n\x07\x63reated\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\"\x15\n\x07ShardId\x12\n\n\x02id\x18\x01 \x01(\t\"/\n\x08ShardIds\x12#\n\x03ids\x18\x01 \x03(\x0b\x32\x16.noderesources.ShardId\"\xa7\x04\n\x0cShardCreated\x12\n\n\x02id\x18\x01 \x01(\t\x12\x45\n\x10\x64ocument_service\x18\x02 \x01(\x0e\x32+.noderesources.ShardCreated.DocumentService\x12G\n\x11paragraph_service\x18\x03 \x01(\x0e\x32,.noderesources.ShardCreated.ParagraphService\x12\x41\n\x0evector_service\x18\x04 \x01(\x0e\x32).noderesources.ShardCreated.VectorService\x12\x45\n\x10relation_service\x18\x05 \x01(\x0e\x32+.noderesources.ShardCreated.RelationService\"D\n\x0f\x44ocumentService\x12\x0f\n\x0b\x44OCUMENT_V0\x10\x00\x12\x0f\n\x0b\x44OCUMENT_V1\x10\x01\x12\x0f\n\x0b\x44OCUMENT_V2\x10\x02\"6\n\x10ParagraphService\x12\x10\n\x0cPARAGRAPH_V0\x10\x00\x12\x10\n\x0cPARAGRAPH_V1\x10\x01\"-\n\rVectorService\x12\r\n\tVECTOR_V0\x10\x00\x12\r\n\tVECTOR_V1\x10\x01\"D\n\x0fRelationService\x12\x0f\n\x0bRELATION_V0\x10\x00\x12\x0f\n\x0bRELATION_V1\x10\x01\x12\x0f\n\x0bRELATION_V2\x10\x02\",\n\nResourceID\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\x0c\n\x04uuid\x18\x02 \x01(\t\"\x80\x01\n\x05Shard\x12.\n\x08metadata\x18\x05 \x01(\x0b\x32\x1c.noderesources.ShardMetadata\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\x0e\n\x06\x66ields\x18\x02 \x01(\x04\x12\x12\n\nparagraphs\x18\x03 \x01(\x04\x12\x11\n\tsentences\x18\x04 \x01(\x04\"\x0f\n\rEmptyResponse\"\x0c\n\nEmptyQuery\"v\n\x08Position\x12\r\n\x05index\x18\x01 \x01(\x04\x12\r\n\x05start\x18\x02 \x01(\x04\x12\x0b\n\x03\x65nd\x18\x03 \x01(\x04\x12\x13\n\x0bpage_number\x18\x04 \x01(\x04\x12\x15\n\rstart_seconds\x18\x05 \x03(\r\x12\x13\n\x0b\x65nd_seconds\x18\x06 \x03(\r\"=\n\x10SentenceMetadata\x12)\n\x08position\x18\x01 \x01(\x0b\x32\x17.noderesources.Position\"S\n\x0eVectorSentence\x12\x0e\n\x06vector\x18\x01 \x03(\x02\x12\x31\n\x08metadata\x18\t \x01(\x0b\x32\x1f.noderesources.SentenceMetadata\">\n\x11ParagraphMetadata\x12)\n\x08position\x18\x01 \x01(\x0b\x32\x17.noderesources.Position\"\xca\x02\n\x0eIndexParagraph\x12\r\n\x05start\x18\x01 \x01(\x05\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x05\x12\x0e\n\x06labels\x18\x03 \x03(\t\x12?\n\tsentences\x18\x04 \x03(\x0b\x32,.noderesources.IndexParagraph.SentencesEntry\x12\r\n\x05\x66ield\x18\x05 \x01(\t\x12\r\n\x05split\x18\x06 \x01(\t\x12\r\n\x05index\x18\x07 \x01(\x04\x12\x19\n\x11repeated_in_field\x18\x08 \x01(\x08\x12\x32\n\x08metadata\x18\t \x01(\x0b\x32 .noderesources.ParagraphMetadata\x1aO\n\x0eSentencesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.noderesources.VectorSentence:\x02\x38\x01\"G\n\x0bVectorSetID\x12%\n\x05shard\x18\x01 \x01(\x0b\x32\x16.noderesources.ShardId\x12\x11\n\tvectorset\x18\x02 \x01(\t\"I\n\rVectorSetList\x12%\n\x05shard\x18\x01 \x01(\x0b\x32\x16.noderesources.ShardId\x12\x11\n\tvectorset\x18\x02 \x03(\t\"\xa7\x01\n\x0fIndexParagraphs\x12\x42\n\nparagraphs\x18\x01 \x03(\x0b\x32..noderesources.IndexParagraphs.ParagraphsEntry\x1aP\n\x0fParagraphsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.noderesources.IndexParagraph:\x02\x38\x01\"\xe4\x07\n\x08Resource\x12+\n\x08resource\x18\x01 \x01(\x0b\x32\x19.noderesources.ResourceID\x12.\n\x08metadata\x18\x02 \x01(\x0b\x32\x1c.noderesources.IndexMetadata\x12\x31\n\x05texts\x18\x03 \x03(\x0b\x32\".noderesources.Resource.TextsEntry\x12\x0e\n\x06labels\x18\x04 \x03(\t\x12\x36\n\x06status\x18\x05 \x01(\x0e\x32&.noderesources.Resource.ResourceStatus\x12;\n\nparagraphs\x18\x06 \x03(\x0b\x32\'.noderesources.Resource.ParagraphsEntry\x12\x1c\n\x14paragraphs_to_delete\x18\x07 \x03(\t\x12\x1b\n\x13sentences_to_delete\x18\x08 \x03(\t\x12\"\n\trelations\x18\t \x03(\x0b\x32\x0f.utils.Relation\x12\x10\n\x08shard_id\x18\x0b \x01(\t\x12\x35\n\x07vectors\x18\x0c \x03(\x0b\x32$.noderesources.Resource.VectorsEntry\x12G\n\x11vectors_to_delete\x18\r \x03(\x0b\x32,.noderesources.Resource.VectorsToDeleteEntry\x12&\n\x08security\x18\x0e \x01(\x0b\x32\x0f.utils.SecurityH\x00\x88\x01\x01\x1aL\n\nTextsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12-\n\x05value\x18\x02 \x01(\x0b\x32\x1e.noderesources.TextInformation:\x02\x38\x01\x1aQ\n\x0fParagraphsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12-\n\x05value\x18\x02 \x01(\x0b\x32\x1e.noderesources.IndexParagraphs:\x02\x38\x01\x1a\x42\n\x0cVectorsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12!\n\x05value\x18\x02 \x01(\x0b\x32\x12.utils.UserVectors:\x02\x38\x01\x1aN\n\x14VectorsToDeleteEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12%\n\x05value\x18\x02 \x01(\x0b\x32\x16.utils.UserVectorsList:\x02\x38\x01\"h\n\x0eResourceStatus\x12\r\n\tPROCESSED\x10\x00\x12\t\n\x05\x45MPTY\x10\x01\x12\t\n\x05\x45RROR\x10\x02\x12\n\n\x06\x44\x45LETE\x10\x03\x12\x0b\n\x07PENDING\x10\x04\x12\x0b\n\x07\x42LOCKED\x10\x05\x12\x0b\n\x07\x45XPIRED\x10\x06\x42\x0b\n\t_security\"M\n\rShardMetadata\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12.\n\x0frelease_channel\x18\x02 \x01(\x0e\x32\x15.utils.ReleaseChannel\"\xf8\x02\n\x0cNodeMetadata\x12\x16\n\nload_score\x18\x01 \x01(\x02\x42\x02\x18\x01\x12\x13\n\x0bshard_count\x18\x02 \x01(\x04\x12;\n\x06shards\x18\x03 \x03(\x0b\x32\'.noderesources.NodeMetadata.ShardsEntryB\x02\x18\x01\x12\x0f\n\x07node_id\x18\x04 \x01(\t\x12\x1c\n\x0fprimary_node_id\x18\x05 \x01(\tH\x00\x88\x01\x01\x12\x16\n\x0e\x61vailable_disk\x18\x06 \x01(\x04\x12\x12\n\ntotal_disk\x18\x07 \x01(\x04\x1a\x35\n\rShardMetadata\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\x16\n\nload_score\x18\x02 \x01(\x02\x42\x02\x18\x01\x1aX\n\x0bShardsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x38\n\x05value\x18\x02 \x01(\x0b\x32).noderesources.NodeMetadata.ShardMetadata:\x02\x38\x01\x42\x12\n\x10_primary_node_idP\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.noderesources_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _INDEXPARAGRAPH_SENTENCESENTRY._options = None
  _INDEXPARAGRAPH_SENTENCESENTRY._serialized_options = b'8\001'
  _INDEXPARAGRAPHS_PARAGRAPHSENTRY._options = None
  _INDEXPARAGRAPHS_PARAGRAPHSENTRY._serialized_options = b'8\001'
  _RESOURCE_TEXTSENTRY._options = None
  _RESOURCE_TEXTSENTRY._serialized_options = b'8\001'
  _RESOURCE_PARAGRAPHSENTRY._options = None
  _RESOURCE_PARAGRAPHSENTRY._serialized_options = b'8\001'
  _RESOURCE_VECTORSENTRY._options = None
  _RESOURCE_VECTORSENTRY._serialized_options = b'8\001'
  _RESOURCE_VECTORSTODELETEENTRY._options = None
  _RESOURCE_VECTORSTODELETEENTRY._serialized_options = b'8\001'
  _NODEMETADATA_SHARDMETADATA.fields_by_name['load_score']._options = None
  _NODEMETADATA_SHARDMETADATA.fields_by_name['load_score']._serialized_options = b'\030\001'
  _NODEMETADATA_SHARDSENTRY._options = None
  _NODEMETADATA_SHARDSENTRY._serialized_options = b'8\001'
  _NODEMETADATA.fields_by_name['load_score']._options = None
  _NODEMETADATA.fields_by_name['load_score']._serialized_options = b'\030\001'
  _NODEMETADATA.fields_by_name['shards']._options = None
  _NODEMETADATA.fields_by_name['shards']._serialized_options = b'\030\001'
  _globals['_TEXTINFORMATION']._serialized_start=116
  _globals['_TEXTINFORMATION']._serialized_end=163
  _globals['_INDEXMETADATA']._serialized_start=165
  _globals['_INDEXMETADATA']._serialized_end=271
  _globals['_SHARDID']._serialized_start=273
  _globals['_SHARDID']._serialized_end=294
  _globals['_SHARDIDS']._serialized_start=296
  _globals['_SHARDIDS']._serialized_end=343
  _globals['_SHARDCREATED']._serialized_start=346
  _globals['_SHARDCREATED']._serialized_end=897
  _globals['_SHARDCREATED_DOCUMENTSERVICE']._serialized_start=656
  _globals['_SHARDCREATED_DOCUMENTSERVICE']._serialized_end=724
  _globals['_SHARDCREATED_PARAGRAPHSERVICE']._serialized_start=726
  _globals['_SHARDCREATED_PARAGRAPHSERVICE']._serialized_end=780
  _globals['_SHARDCREATED_VECTORSERVICE']._serialized_start=782
  _globals['_SHARDCREATED_VECTORSERVICE']._serialized_end=827
  _globals['_SHARDCREATED_RELATIONSERVICE']._serialized_start=829
  _globals['_SHARDCREATED_RELATIONSERVICE']._serialized_end=897
  _globals['_RESOURCEID']._serialized_start=899
  _globals['_RESOURCEID']._serialized_end=943
  _globals['_SHARD']._serialized_start=946
  _globals['_SHARD']._serialized_end=1074
  _globals['_EMPTYRESPONSE']._serialized_start=1076
  _globals['_EMPTYRESPONSE']._serialized_end=1091
  _globals['_EMPTYQUERY']._serialized_start=1093
  _globals['_EMPTYQUERY']._serialized_end=1105
  _globals['_POSITION']._serialized_start=1107
  _globals['_POSITION']._serialized_end=1225
  _globals['_SENTENCEMETADATA']._serialized_start=1227
  _globals['_SENTENCEMETADATA']._serialized_end=1288
  _globals['_VECTORSENTENCE']._serialized_start=1290
  _globals['_VECTORSENTENCE']._serialized_end=1373
  _globals['_PARAGRAPHMETADATA']._serialized_start=1375
  _globals['_PARAGRAPHMETADATA']._serialized_end=1437
  _globals['_INDEXPARAGRAPH']._serialized_start=1440
  _globals['_INDEXPARAGRAPH']._serialized_end=1770
  _globals['_INDEXPARAGRAPH_SENTENCESENTRY']._serialized_start=1691
  _globals['_INDEXPARAGRAPH_SENTENCESENTRY']._serialized_end=1770
  _globals['_VECTORSETID']._serialized_start=1772
  _globals['_VECTORSETID']._serialized_end=1843
  _globals['_VECTORSETLIST']._serialized_start=1845
  _globals['_VECTORSETLIST']._serialized_end=1918
  _globals['_INDEXPARAGRAPHS']._serialized_start=1921
  _globals['_INDEXPARAGRAPHS']._serialized_end=2088
  _globals['_INDEXPARAGRAPHS_PARAGRAPHSENTRY']._serialized_start=2008
  _globals['_INDEXPARAGRAPHS_PARAGRAPHSENTRY']._serialized_end=2088
  _globals['_RESOURCE']._serialized_start=2091
  _globals['_RESOURCE']._serialized_end=3087
  _globals['_RESOURCE_TEXTSENTRY']._serialized_start=2661
  _globals['_RESOURCE_TEXTSENTRY']._serialized_end=2737
  _globals['_RESOURCE_PARAGRAPHSENTRY']._serialized_start=2739
  _globals['_RESOURCE_PARAGRAPHSENTRY']._serialized_end=2820
  _globals['_RESOURCE_VECTORSENTRY']._serialized_start=2822
  _globals['_RESOURCE_VECTORSENTRY']._serialized_end=2888
  _globals['_RESOURCE_VECTORSTODELETEENTRY']._serialized_start=2890
  _globals['_RESOURCE_VECTORSTODELETEENTRY']._serialized_end=2968
  _globals['_RESOURCE_RESOURCESTATUS']._serialized_start=2970
  _globals['_RESOURCE_RESOURCESTATUS']._serialized_end=3074
  _globals['_SHARDMETADATA']._serialized_start=3089
  _globals['_SHARDMETADATA']._serialized_end=3166
  _globals['_NODEMETADATA']._serialized_start=3169
  _globals['_NODEMETADATA']._serialized_end=3545
  _globals['_NODEMETADATA_SHARDMETADATA']._serialized_start=3382
  _globals['_NODEMETADATA_SHARDMETADATA']._serialized_end=3435
  _globals['_NODEMETADATA_SHARDSENTRY']._serialized_start=3437
  _globals['_NODEMETADATA_SHARDSENTRY']._serialized_end=3525
# @@protoc_insertion_point(module_scope)
