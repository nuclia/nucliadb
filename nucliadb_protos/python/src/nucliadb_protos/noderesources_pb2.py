# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/noderesources.proto
# Protobuf Python Version: 4.25.1
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

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n#nucliadb_protos/noderesources.proto\x12\rnoderesources\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1bnucliadb_protos/utils.proto\"/\n\x0fTextInformation\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x0e\n\x06labels\x18\x02 \x03(\t\"j\n\rIndexMetadata\x12,\n\x08modified\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12+\n\x07\x63reated\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\"\x15\n\x07ShardId\x12\n\n\x02id\x18\x01 \x01(\t\"/\n\x08ShardIds\x12#\n\x03ids\x18\x01 \x03(\x0b\x32\x16.noderesources.ShardId\"\xdc\x04\n\x0cShardCreated\x12\n\n\x02id\x18\x01 \x01(\t\x12\x45\n\x10\x64ocument_service\x18\x02 \x01(\x0e\x32+.noderesources.ShardCreated.DocumentService\x12G\n\x11paragraph_service\x18\x03 \x01(\x0e\x32,.noderesources.ShardCreated.ParagraphService\x12\x41\n\x0evector_service\x18\x04 \x01(\x0e\x32).noderesources.ShardCreated.VectorService\x12\x45\n\x10relation_service\x18\x05 \x01(\x0e\x32+.noderesources.ShardCreated.RelationService\"U\n\x0f\x44ocumentService\x12\x0f\n\x0b\x44OCUMENT_V0\x10\x00\x12\x0f\n\x0b\x44OCUMENT_V1\x10\x01\x12\x0f\n\x0b\x44OCUMENT_V2\x10\x02\x12\x0f\n\x0b\x44OCUMENT_V3\x10\x03\"Z\n\x10ParagraphService\x12\x10\n\x0cPARAGRAPH_V0\x10\x00\x12\x10\n\x0cPARAGRAPH_V1\x10\x01\x12\x10\n\x0cPARAGRAPH_V2\x10\x02\x12\x10\n\x0cPARAGRAPH_V3\x10\x03\"-\n\rVectorService\x12\r\n\tVECTOR_V0\x10\x00\x12\r\n\tVECTOR_V1\x10\x01\"D\n\x0fRelationService\x12\x0f\n\x0bRELATION_V0\x10\x00\x12\x0f\n\x0bRELATION_V1\x10\x01\x12\x0f\n\x0bRELATION_V2\x10\x02\",\n\nResourceID\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\x0c\n\x04uuid\x18\x02 \x01(\t\"\x94\x01\n\x05Shard\x12.\n\x08metadata\x18\x05 \x01(\x0b\x32\x1c.noderesources.ShardMetadata\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\x0e\n\x06\x66ields\x18\x02 \x01(\x04\x12\x12\n\nparagraphs\x18\x03 \x01(\x04\x12\x11\n\tsentences\x18\x04 \x01(\x04\x12\x12\n\nsize_bytes\x18\x06 \x01(\x04\"\x0f\n\rEmptyResponse\"\x0c\n\nEmptyQuery\"\x87\x01\n\x08Position\x12\r\n\x05index\x18\x01 \x01(\x04\x12\r\n\x05start\x18\x02 \x01(\x04\x12\x0b\n\x03\x65nd\x18\x03 \x01(\x04\x12\x13\n\x0bpage_number\x18\x04 \x01(\x04\x12\x0f\n\x07in_page\x18\x07 \x01(\x08\x12\x15\n\rstart_seconds\x18\x05 \x03(\r\x12\x13\n\x0b\x65nd_seconds\x18\x06 \x03(\r\"2\n\x0eRepresentation\x12\x12\n\nis_a_table\x18\x01 \x01(\x08\x12\x0c\n\x04\x66ile\x18\x02 \x01(\t\"\x8e\x01\n\x10SentenceMetadata\x12)\n\x08position\x18\x01 \x01(\x0b\x32\x17.noderesources.Position\x12\x18\n\x10page_with_visual\x18\x02 \x01(\x08\x12\x35\n\x0erepresentation\x18\x03 \x01(\x0b\x32\x1d.noderesources.Representation\"S\n\x0eVectorSentence\x12\x0e\n\x06vector\x18\x01 \x03(\x02\x12\x31\n\x08metadata\x18\t \x01(\x0b\x32\x1f.noderesources.SentenceMetadata\"\xaa\x01\n\x12VectorsetSentences\x12\x43\n\tsentences\x18\x01 \x03(\x0b\x32\x30.noderesources.VectorsetSentences.SentencesEntry\x1aO\n\x0eSentencesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.noderesources.VectorSentence:\x02\x38\x01\"\x8f\x01\n\x11ParagraphMetadata\x12)\n\x08position\x18\x01 \x01(\x0b\x32\x17.noderesources.Position\x12\x18\n\x10page_with_visual\x18\x02 \x01(\x08\x12\x35\n\x0erepresentation\x18\x03 \x01(\x0b\x32\x1d.noderesources.Representation\"\xff\x03\n\x0eIndexParagraph\x12\r\n\x05start\x18\x01 \x01(\x05\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x05\x12\x0e\n\x06labels\x18\x03 \x03(\t\x12?\n\tsentences\x18\x04 \x03(\x0b\x32,.noderesources.IndexParagraph.SentencesEntry\x12T\n\x14vectorsets_sentences\x18\n \x03(\x0b\x32\x36.noderesources.IndexParagraph.VectorsetsSentencesEntry\x12\r\n\x05\x66ield\x18\x05 \x01(\t\x12\r\n\x05split\x18\x06 \x01(\t\x12\r\n\x05index\x18\x07 \x01(\x04\x12\x19\n\x11repeated_in_field\x18\x08 \x01(\x08\x12\x32\n\x08metadata\x18\t \x01(\x0b\x32 .noderesources.ParagraphMetadata\x1aO\n\x0eSentencesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.noderesources.VectorSentence:\x02\x38\x01\x1a]\n\x18VectorsetsSentencesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x30\n\x05value\x18\x02 \x01(\x0b\x32!.noderesources.VectorsetSentences:\x02\x38\x01\"G\n\x0bVectorSetID\x12%\n\x05shard\x18\x01 \x01(\x0b\x32\x16.noderesources.ShardId\x12\x11\n\tvectorset\x18\x02 \x01(\t\"J\n\rVectorSetList\x12%\n\x05shard\x18\x01 \x01(\x0b\x32\x16.noderesources.ShardId\x12\x12\n\nvectorsets\x18\x02 \x03(\t\"\xa7\x01\n\x0fIndexParagraphs\x12\x42\n\nparagraphs\x18\x01 \x03(\x0b\x32..noderesources.IndexParagraphs.ParagraphsEntry\x1aP\n\x0fParagraphsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.noderesources.IndexParagraph:\x02\x38\x01\"\xdc\n\n\x08Resource\x12+\n\x08resource\x18\x01 \x01(\x0b\x32\x19.noderesources.ResourceID\x12.\n\x08metadata\x18\x02 \x01(\x0b\x32\x1c.noderesources.IndexMetadata\x12\x31\n\x05texts\x18\x03 \x03(\x0b\x32\".noderesources.Resource.TextsEntry\x12\x0e\n\x06labels\x18\x04 \x03(\t\x12\x36\n\x06status\x18\x05 \x01(\x0e\x32&.noderesources.Resource.ResourceStatus\x12;\n\nparagraphs\x18\x06 \x03(\x0b\x32\'.noderesources.Resource.ParagraphsEntry\x12\x1c\n\x14paragraphs_to_delete\x18\x07 \x03(\t\x12\x1f\n\x13sentences_to_delete\x18\x08 \x03(\tB\x02\x18\x01\x12V\n\x19vector_prefixes_to_delete\x18\x0f \x03(\x0b\x32\x33.noderesources.Resource.VectorPrefixesToDeleteEntry\x12!\n\x19relation_fields_to_delete\x18\x10 \x03(\t\x12\x1d\n\x15text_fields_to_delete\x18\x11 \x03(\t\x12\x44\n\x0f\x66ield_relations\x18\n \x03(\x0b\x32+.noderesources.Resource.FieldRelationsEntry\x12\x10\n\x08shard_id\x18\x0b \x01(\t\x12\x39\n\x07vectors\x18\x0c \x03(\x0b\x32$.noderesources.Resource.VectorsEntryB\x02\x18\x01\x12K\n\x11vectors_to_delete\x18\r \x03(\x0b\x32,.noderesources.Resource.VectorsToDeleteEntryB\x02\x18\x01\x12&\n\x08security\x18\x0e \x01(\x0b\x32\x0f.utils.SecurityH\x00\x88\x01\x01\x1aL\n\nTextsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12-\n\x05value\x18\x02 \x01(\x0b\x32\x1e.noderesources.TextInformation:\x02\x38\x01\x1aQ\n\x0fParagraphsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12-\n\x05value\x18\x02 \x01(\x0b\x32\x1e.noderesources.IndexParagraphs:\x02\x38\x01\x1aX\n\x1bVectorPrefixesToDeleteEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12(\n\x05value\x18\x02 \x01(\x0b\x32\x19.noderesources.StringList:\x02\x38\x01\x1aT\n\x13\x46ieldRelationsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.noderesources.IndexRelations:\x02\x38\x01\x1a\x42\n\x0cVectorsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12!\n\x05value\x18\x02 \x01(\x0b\x32\x12.utils.UserVectors:\x02\x38\x01\x1aN\n\x14VectorsToDeleteEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12%\n\x05value\x18\x02 \x01(\x0b\x32\x16.utils.UserVectorsList:\x02\x38\x01\"h\n\x0eResourceStatus\x12\r\n\tPROCESSED\x10\x00\x12\t\n\x05\x45MPTY\x10\x01\x12\t\n\x05\x45RROR\x10\x02\x12\n\n\x06\x44\x45LETE\x10\x03\x12\x0b\n\x07PENDING\x10\x04\x12\x0b\n\x07\x42LOCKED\x10\x05\x12\x0b\n\x07\x45XPIRED\x10\x06\x42\x0b\n\t_security\"M\n\rShardMetadata\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12.\n\x0frelease_channel\x18\x02 \x01(\x0e\x32\x15.utils.ReleaseChannel\"\xf8\x02\n\x0cNodeMetadata\x12\x16\n\nload_score\x18\x01 \x01(\x02\x42\x02\x18\x01\x12\x13\n\x0bshard_count\x18\x02 \x01(\x04\x12;\n\x06shards\x18\x03 \x03(\x0b\x32\'.noderesources.NodeMetadata.ShardsEntryB\x02\x18\x01\x12\x0f\n\x07node_id\x18\x04 \x01(\t\x12\x1c\n\x0fprimary_node_id\x18\x05 \x01(\tH\x00\x88\x01\x01\x12\x16\n\x0e\x61vailable_disk\x18\x06 \x01(\x04\x12\x12\n\ntotal_disk\x18\x07 \x01(\x04\x1a\x35\n\rShardMetadata\x12\x0c\n\x04kbid\x18\x01 \x01(\t\x12\x16\n\nload_score\x18\x02 \x01(\x02\x42\x02\x18\x01\x1aX\n\x0bShardsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x38\n\x05value\x18\x02 \x01(\x0b\x32).noderesources.NodeMetadata.ShardMetadata:\x02\x38\x01\x42\x12\n\x10_primary_node_id\"\x1b\n\nStringList\x12\r\n\x05items\x18\x01 \x03(\t\"x\n\rIndexRelation\x12!\n\x08relation\x18\x01 \x01(\x0b\x32\x0f.utils.Relation\x12\x1e\n\x11resource_field_id\x18\x02 \x01(\tH\x00\x88\x01\x01\x12\x0e\n\x06\x66\x61\x63\x65ts\x18\x03 \x03(\tB\x14\n\x12_resource_field_id\"A\n\x0eIndexRelations\x12/\n\trelations\x18\x01 \x03(\x0b\x32\x1c.noderesources.IndexRelationP\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.noderesources_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_VECTORSETSENTENCES_SENTENCESENTRY']._options = None
  _globals['_VECTORSETSENTENCES_SENTENCESENTRY']._serialized_options = b'8\001'
  _globals['_INDEXPARAGRAPH_SENTENCESENTRY']._options = None
  _globals['_INDEXPARAGRAPH_SENTENCESENTRY']._serialized_options = b'8\001'
  _globals['_INDEXPARAGRAPH_VECTORSETSSENTENCESENTRY']._options = None
  _globals['_INDEXPARAGRAPH_VECTORSETSSENTENCESENTRY']._serialized_options = b'8\001'
  _globals['_INDEXPARAGRAPHS_PARAGRAPHSENTRY']._options = None
  _globals['_INDEXPARAGRAPHS_PARAGRAPHSENTRY']._serialized_options = b'8\001'
  _globals['_RESOURCE_TEXTSENTRY']._options = None
  _globals['_RESOURCE_TEXTSENTRY']._serialized_options = b'8\001'
  _globals['_RESOURCE_PARAGRAPHSENTRY']._options = None
  _globals['_RESOURCE_PARAGRAPHSENTRY']._serialized_options = b'8\001'
  _globals['_RESOURCE_VECTORPREFIXESTODELETEENTRY']._options = None
  _globals['_RESOURCE_VECTORPREFIXESTODELETEENTRY']._serialized_options = b'8\001'
  _globals['_RESOURCE_FIELDRELATIONSENTRY']._options = None
  _globals['_RESOURCE_FIELDRELATIONSENTRY']._serialized_options = b'8\001'
  _globals['_RESOURCE_VECTORSENTRY']._options = None
  _globals['_RESOURCE_VECTORSENTRY']._serialized_options = b'8\001'
  _globals['_RESOURCE_VECTORSTODELETEENTRY']._options = None
  _globals['_RESOURCE_VECTORSTODELETEENTRY']._serialized_options = b'8\001'
  _globals['_RESOURCE'].fields_by_name['sentences_to_delete']._options = None
  _globals['_RESOURCE'].fields_by_name['sentences_to_delete']._serialized_options = b'\030\001'
  _globals['_RESOURCE'].fields_by_name['vectors']._options = None
  _globals['_RESOURCE'].fields_by_name['vectors']._serialized_options = b'\030\001'
  _globals['_RESOURCE'].fields_by_name['vectors_to_delete']._options = None
  _globals['_RESOURCE'].fields_by_name['vectors_to_delete']._serialized_options = b'\030\001'
  _globals['_NODEMETADATA_SHARDMETADATA'].fields_by_name['load_score']._options = None
  _globals['_NODEMETADATA_SHARDMETADATA'].fields_by_name['load_score']._serialized_options = b'\030\001'
  _globals['_NODEMETADATA_SHARDSENTRY']._options = None
  _globals['_NODEMETADATA_SHARDSENTRY']._serialized_options = b'8\001'
  _globals['_NODEMETADATA'].fields_by_name['load_score']._options = None
  _globals['_NODEMETADATA'].fields_by_name['load_score']._serialized_options = b'\030\001'
  _globals['_NODEMETADATA'].fields_by_name['shards']._options = None
  _globals['_NODEMETADATA'].fields_by_name['shards']._serialized_options = b'\030\001'
  _globals['_TEXTINFORMATION']._serialized_start=116
  _globals['_TEXTINFORMATION']._serialized_end=163
  _globals['_INDEXMETADATA']._serialized_start=165
  _globals['_INDEXMETADATA']._serialized_end=271
  _globals['_SHARDID']._serialized_start=273
  _globals['_SHARDID']._serialized_end=294
  _globals['_SHARDIDS']._serialized_start=296
  _globals['_SHARDIDS']._serialized_end=343
  _globals['_SHARDCREATED']._serialized_start=346
  _globals['_SHARDCREATED']._serialized_end=950
  _globals['_SHARDCREATED_DOCUMENTSERVICE']._serialized_start=656
  _globals['_SHARDCREATED_DOCUMENTSERVICE']._serialized_end=741
  _globals['_SHARDCREATED_PARAGRAPHSERVICE']._serialized_start=743
  _globals['_SHARDCREATED_PARAGRAPHSERVICE']._serialized_end=833
  _globals['_SHARDCREATED_VECTORSERVICE']._serialized_start=835
  _globals['_SHARDCREATED_VECTORSERVICE']._serialized_end=880
  _globals['_SHARDCREATED_RELATIONSERVICE']._serialized_start=882
  _globals['_SHARDCREATED_RELATIONSERVICE']._serialized_end=950
  _globals['_RESOURCEID']._serialized_start=952
  _globals['_RESOURCEID']._serialized_end=996
  _globals['_SHARD']._serialized_start=999
  _globals['_SHARD']._serialized_end=1147
  _globals['_EMPTYRESPONSE']._serialized_start=1149
  _globals['_EMPTYRESPONSE']._serialized_end=1164
  _globals['_EMPTYQUERY']._serialized_start=1166
  _globals['_EMPTYQUERY']._serialized_end=1178
  _globals['_POSITION']._serialized_start=1181
  _globals['_POSITION']._serialized_end=1316
  _globals['_REPRESENTATION']._serialized_start=1318
  _globals['_REPRESENTATION']._serialized_end=1368
  _globals['_SENTENCEMETADATA']._serialized_start=1371
  _globals['_SENTENCEMETADATA']._serialized_end=1513
  _globals['_VECTORSENTENCE']._serialized_start=1515
  _globals['_VECTORSENTENCE']._serialized_end=1598
  _globals['_VECTORSETSENTENCES']._serialized_start=1601
  _globals['_VECTORSETSENTENCES']._serialized_end=1771
  _globals['_VECTORSETSENTENCES_SENTENCESENTRY']._serialized_start=1692
  _globals['_VECTORSETSENTENCES_SENTENCESENTRY']._serialized_end=1771
  _globals['_PARAGRAPHMETADATA']._serialized_start=1774
  _globals['_PARAGRAPHMETADATA']._serialized_end=1917
  _globals['_INDEXPARAGRAPH']._serialized_start=1920
  _globals['_INDEXPARAGRAPH']._serialized_end=2431
  _globals['_INDEXPARAGRAPH_SENTENCESENTRY']._serialized_start=1692
  _globals['_INDEXPARAGRAPH_SENTENCESENTRY']._serialized_end=1771
  _globals['_INDEXPARAGRAPH_VECTORSETSSENTENCESENTRY']._serialized_start=2338
  _globals['_INDEXPARAGRAPH_VECTORSETSSENTENCESENTRY']._serialized_end=2431
  _globals['_VECTORSETID']._serialized_start=2433
  _globals['_VECTORSETID']._serialized_end=2504
  _globals['_VECTORSETLIST']._serialized_start=2506
  _globals['_VECTORSETLIST']._serialized_end=2580
  _globals['_INDEXPARAGRAPHS']._serialized_start=2583
  _globals['_INDEXPARAGRAPHS']._serialized_end=2750
  _globals['_INDEXPARAGRAPHS_PARAGRAPHSENTRY']._serialized_start=2670
  _globals['_INDEXPARAGRAPHS_PARAGRAPHSENTRY']._serialized_end=2750
  _globals['_RESOURCE']._serialized_start=2753
  _globals['_RESOURCE']._serialized_end=4125
  _globals['_RESOURCE_TEXTSENTRY']._serialized_start=3523
  _globals['_RESOURCE_TEXTSENTRY']._serialized_end=3599
  _globals['_RESOURCE_PARAGRAPHSENTRY']._serialized_start=3601
  _globals['_RESOURCE_PARAGRAPHSENTRY']._serialized_end=3682
  _globals['_RESOURCE_VECTORPREFIXESTODELETEENTRY']._serialized_start=3684
  _globals['_RESOURCE_VECTORPREFIXESTODELETEENTRY']._serialized_end=3772
  _globals['_RESOURCE_FIELDRELATIONSENTRY']._serialized_start=3774
  _globals['_RESOURCE_FIELDRELATIONSENTRY']._serialized_end=3858
  _globals['_RESOURCE_VECTORSENTRY']._serialized_start=3860
  _globals['_RESOURCE_VECTORSENTRY']._serialized_end=3926
  _globals['_RESOURCE_VECTORSTODELETEENTRY']._serialized_start=3928
  _globals['_RESOURCE_VECTORSTODELETEENTRY']._serialized_end=4006
  _globals['_RESOURCE_RESOURCESTATUS']._serialized_start=4008
  _globals['_RESOURCE_RESOURCESTATUS']._serialized_end=4112
  _globals['_SHARDMETADATA']._serialized_start=4127
  _globals['_SHARDMETADATA']._serialized_end=4204
  _globals['_NODEMETADATA']._serialized_start=4207
  _globals['_NODEMETADATA']._serialized_end=4583
  _globals['_NODEMETADATA_SHARDMETADATA']._serialized_start=4420
  _globals['_NODEMETADATA_SHARDMETADATA']._serialized_end=4473
  _globals['_NODEMETADATA_SHARDSENTRY']._serialized_start=4475
  _globals['_NODEMETADATA_SHARDSENTRY']._serialized_end=4563
  _globals['_STRINGLIST']._serialized_start=4585
  _globals['_STRINGLIST']._serialized_end=4612
  _globals['_INDEXRELATION']._serialized_start=4614
  _globals['_INDEXRELATION']._serialized_end=4734
  _globals['_INDEXRELATIONS']._serialized_start=4736
  _globals['_INDEXRELATIONS']._serialized_end=4801
# @@protoc_insertion_point(module_scope)
