# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/nodereader.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from nucliadb_protos import noderesources_pb2 as nucliadb__protos_dot_noderesources__pb2
try:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_noderesources__pb2.nucliadb__protos_dot_utils__pb2
except AttributeError:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_noderesources__pb2.nucliadb_protos.utils_pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from nucliadb_protos import utils_pb2 as nucliadb__protos_dot_utils__pb2

from nucliadb_protos.noderesources_pb2 import *
from nucliadb_protos.utils_pb2 import *

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n nucliadb_protos/nodereader.proto\x12\nnodereader\x1a#nucliadb_protos/noderesources.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1bnucliadb_protos/utils.proto\"\x16\n\x06\x46ilter\x12\x0c\n\x04tags\x18\x01 \x03(\t\"\x17\n\x07\x46\x61\x63\x65ted\x12\x0c\n\x04tags\x18\x01 \x03(\t\"\xc3\x01\n\x07OrderBy\x12\x11\n\x05\x66ield\x18\x01 \x01(\tB\x02\x18\x01\x12+\n\x04type\x18\x02 \x01(\x0e\x32\x1d.nodereader.OrderBy.OrderType\x12/\n\x07sort_by\x18\x03 \x01(\x0e\x32\x1e.nodereader.OrderBy.OrderField\"\x1e\n\tOrderType\x12\x08\n\x04\x44\x45SC\x10\x00\x12\x07\n\x03\x41SC\x10\x01\"\'\n\nOrderField\x12\x0b\n\x07\x43REATED\x10\x00\x12\x0c\n\x08MODIFIED\x10\x01\"\xd2\x01\n\nTimestamps\x12\x31\n\rfrom_modified\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12/\n\x0bto_modified\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x30\n\x0c\x66rom_created\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12.\n\nto_created\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\")\n\x0b\x46\x61\x63\x65tResult\x12\x0b\n\x03tag\x18\x01 \x01(\t\x12\r\n\x05total\x18\x02 \x01(\x05\"=\n\x0c\x46\x61\x63\x65tResults\x12-\n\x0c\x66\x61\x63\x65tresults\x18\x01 \x03(\x0b\x32\x17.nodereader.FacetResult\"\xb1\x03\n\x15\x44ocumentSearchRequest\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04\x62ody\x18\x02 \x01(\t\x12\x0e\n\x06\x66ields\x18\x03 \x03(\t\x12\"\n\x06\x66ilter\x18\x04 \x01(\x0b\x32\x12.nodereader.Filter\x12\"\n\x05order\x18\x05 \x01(\x0b\x32\x13.nodereader.OrderBy\x12$\n\x07\x66\x61\x63\x65ted\x18\x06 \x01(\x0b\x32\x13.nodereader.Faceted\x12\x13\n\x0bpage_number\x18\x07 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x08 \x01(\x05\x12*\n\ntimestamps\x18\t \x01(\x0b\x32\x16.nodereader.Timestamps\x12\x0e\n\x06reload\x18\n \x01(\x08\x12\x14\n\x0conly_faceted\x18\x0f \x01(\x08\x12@\n\x0bwith_status\x18\x10 \x01(\x0e\x32&.noderesources.Resource.ResourceStatusH\x00\x88\x01\x01\x12\x1b\n\x0e\x61\x64vanced_query\x18\x11 \x01(\tH\x01\x88\x01\x01\x42\x0e\n\x0c_with_statusB\x11\n\x0f_advanced_query\"\x87\x03\n\x16ParagraphSearchRequest\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12\x0e\n\x06\x66ields\x18\x03 \x03(\t\x12\x0c\n\x04\x62ody\x18\x04 \x01(\t\x12\"\n\x06\x66ilter\x18\x05 \x01(\x0b\x32\x12.nodereader.Filter\x12\"\n\x05order\x18\x07 \x01(\x0b\x32\x13.nodereader.OrderBy\x12$\n\x07\x66\x61\x63\x65ted\x18\x08 \x01(\x0b\x32\x13.nodereader.Faceted\x12\x13\n\x0bpage_number\x18\n \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x0b \x01(\x05\x12*\n\ntimestamps\x18\x0c \x01(\x0b\x32\x16.nodereader.Timestamps\x12\x0e\n\x06reload\x18\r \x01(\x08\x12\x17\n\x0fwith_duplicates\x18\x0e \x01(\x08\x12\x14\n\x0conly_faceted\x18\x0f \x01(\x08\x12\x1b\n\x0e\x61\x64vanced_query\x18\x10 \x01(\tH\x00\x88\x01\x01\x42\x11\n\x0f_advanced_query\",\n\x0bResultScore\x12\x0c\n\x04\x62m25\x18\x01 \x01(\x02\x12\x0f\n\x07\x62ooster\x18\x02 \x01(\x02\"e\n\x0e\x44ocumentResult\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12&\n\x05score\x18\x03 \x01(\x0b\x32\x17.nodereader.ResultScore\x12\r\n\x05\x66ield\x18\x04 \x01(\t\x12\x0e\n\x06labels\x18\x05 \x03(\t\"\xbb\x02\n\x16\x44ocumentSearchResponse\x12\r\n\x05total\x18\x01 \x01(\x05\x12+\n\x07results\x18\x02 \x03(\x0b\x32\x1a.nodereader.DocumentResult\x12>\n\x06\x66\x61\x63\x65ts\x18\x03 \x03(\x0b\x32..nodereader.DocumentSearchResponse.FacetsEntry\x12\x13\n\x0bpage_number\x18\x04 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x05 \x01(\x05\x12\r\n\x05query\x18\x06 \x01(\t\x12\x11\n\tnext_page\x18\x07 \x01(\x08\x12\x0c\n\x04\x62m25\x18\x08 \x01(\x08\x1aG\n\x0b\x46\x61\x63\x65tsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\'\n\x05value\x18\x02 \x01(\x0b\x32\x18.nodereader.FacetResults:\x02\x38\x01\"\xf8\x01\n\x0fParagraphResult\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12\r\n\x05\x66ield\x18\x03 \x01(\t\x12\r\n\x05start\x18\x04 \x01(\x04\x12\x0b\n\x03\x65nd\x18\x05 \x01(\x04\x12\x11\n\tparagraph\x18\x06 \x01(\t\x12\r\n\x05split\x18\x07 \x01(\t\x12\r\n\x05index\x18\x08 \x01(\x04\x12&\n\x05score\x18\t \x01(\x0b\x32\x17.nodereader.ResultScore\x12\x0f\n\x07matches\x18\n \x03(\t\x12\x32\n\x08metadata\x18\x0b \x01(\x0b\x32 .noderesources.ParagraphMetadata\x12\x0e\n\x06labels\x18\x0c \x03(\t\"\xe8\x02\n\x17ParagraphSearchResponse\x12\x16\n\x0e\x66uzzy_distance\x18\n \x01(\x05\x12\r\n\x05total\x18\x01 \x01(\x05\x12,\n\x07results\x18\x02 \x03(\x0b\x32\x1b.nodereader.ParagraphResult\x12?\n\x06\x66\x61\x63\x65ts\x18\x03 \x03(\x0b\x32/.nodereader.ParagraphSearchResponse.FacetsEntry\x12\x13\n\x0bpage_number\x18\x04 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x05 \x01(\x05\x12\r\n\x05query\x18\x06 \x01(\t\x12\x11\n\tnext_page\x18\x07 \x01(\x08\x12\x0c\n\x04\x62m25\x18\x08 \x01(\x08\x12\x10\n\x08\x65matches\x18\t \x03(\t\x1aG\n\x0b\x46\x61\x63\x65tsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\'\n\x05value\x18\x02 \x01(\x0b\x32\x18.nodereader.FacetResults:\x02\x38\x01\"\xaa\x01\n\x13VectorSearchRequest\x12\n\n\x02id\x18\x01 \x01(\t\x12\x12\n\nvector_set\x18\x0f \x01(\t\x12\x0e\n\x06vector\x18\x02 \x03(\x02\x12\x0c\n\x04tags\x18\x03 \x03(\t\x12\x13\n\x0bpage_number\x18\x04 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x05 \x01(\x05\x12\x17\n\x0fwith_duplicates\x18\x0e \x01(\x08\x12\x0e\n\x06reload\x18\r \x01(\x08\"&\n\x18\x44ocumentVectorIdentifier\x12\n\n\x02id\x18\x01 \x01(\t\"U\n\x0e\x44ocumentScored\x12\x34\n\x06\x64oc_id\x18\x01 \x01(\x0b\x32$.nodereader.DocumentVectorIdentifier\x12\r\n\x05score\x18\x02 \x01(\x02\"s\n\x14VectorSearchResponse\x12-\n\tdocuments\x18\x01 \x03(\x0b\x32\x1a.nodereader.DocumentScored\x12\x13\n\x0bpage_number\x18\x04 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x05 \x01(\x05\"q\n\x12RelationNodeFilter\x12/\n\tnode_type\x18\x01 \x01(\x0e\x32\x1c.utils.RelationNode.NodeType\x12\x19\n\x0cnode_subtype\x18\x02 \x01(\tH\x00\x88\x01\x01\x42\x0f\n\r_node_subtype\"}\n\x12RelationEdgeFilter\x12\x33\n\rrelation_type\x18\x01 \x01(\x0e\x32\x1c.utils.Relation.RelationType\x12\x1d\n\x10relation_subtype\x18\x02 \x01(\tH\x00\x88\x01\x01\x42\x13\n\x11_relation_subtype\"-\n\x1bRelationPrefixSearchRequest\x12\x0e\n\x06prefix\x18\x01 \x01(\t\"B\n\x1cRelationPrefixSearchResponse\x12\"\n\x05nodes\x18\x01 \x03(\x0b\x32\x13.utils.RelationNode\"\xce\x01\n\x17\x45ntitiesSubgraphRequest\x12)\n\x0c\x65ntry_points\x18\x01 \x03(\x0b\x32\x13.utils.RelationNode\x12\x34\n\x0cnode_filters\x18\x02 \x03(\x0b\x32\x1e.nodereader.RelationNodeFilter\x12\x34\n\x0c\x65\x64ge_filters\x18\x04 \x03(\x0b\x32\x1e.nodereader.RelationEdgeFilter\x12\x12\n\x05\x64\x65pth\x18\x03 \x01(\x05H\x00\x88\x01\x01\x42\x08\n\x06_depth\">\n\x18\x45ntitiesSubgraphResponse\x12\"\n\trelations\x18\x01 \x03(\x0b\x32\x0f.utils.Relation\"\xa9\x01\n\x15RelationSearchRequest\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\x0e\n\x06reload\x18\x05 \x01(\x08\x12\x37\n\x06prefix\x18\x0b \x01(\x0b\x32\'.nodereader.RelationPrefixSearchRequest\x12\x35\n\x08subgraph\x18\x0c \x01(\x0b\x32#.nodereader.EntitiesSubgraphRequest\"\x8a\x01\n\x16RelationSearchResponse\x12\x38\n\x06prefix\x18\x0b \x01(\x0b\x32(.nodereader.RelationPrefixSearchResponse\x12\x36\n\x08subgraph\x18\x0c \x01(\x0b\x32$.nodereader.EntitiesSubgraphResponse\"\xc3\x04\n\rSearchRequest\x12\r\n\x05shard\x18\x01 \x01(\t\x12\x0e\n\x06\x66ields\x18\x02 \x03(\t\x12\x0c\n\x04\x62ody\x18\x03 \x01(\t\x12\"\n\x06\x66ilter\x18\x04 \x01(\x0b\x32\x12.nodereader.Filter\x12\"\n\x05order\x18\x05 \x01(\x0b\x32\x13.nodereader.OrderBy\x12$\n\x07\x66\x61\x63\x65ted\x18\x06 \x01(\x0b\x32\x13.nodereader.Faceted\x12\x13\n\x0bpage_number\x18\x07 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x08 \x01(\x05\x12*\n\ntimestamps\x18\t \x01(\x0b\x32\x16.nodereader.Timestamps\x12\x0e\n\x06vector\x18\n \x03(\x02\x12\x11\n\tvectorset\x18\x0f \x01(\t\x12\x0e\n\x06reload\x18\x0b \x01(\x08\x12\x11\n\tparagraph\x18\x0c \x01(\x08\x12\x10\n\x08\x64ocument\x18\r \x01(\x08\x12\x17\n\x0fwith_duplicates\x18\x0e \x01(\x08\x12\x14\n\x0conly_faceted\x18\x10 \x01(\x08\x12\x1b\n\x0e\x61\x64vanced_query\x18\x12 \x01(\tH\x00\x88\x01\x01\x12@\n\x0bwith_status\x18\x11 \x01(\x0e\x32&.noderesources.Resource.ResourceStatusH\x01\x88\x01\x01\x12\x34\n\trelations\x18\x13 \x01(\x0b\x32!.nodereader.RelationSearchRequestB\x11\n\x0f_advanced_queryB\x0e\n\x0c_with_status\"\x8d\x01\n\x0eSuggestRequest\x12\r\n\x05shard\x18\x01 \x01(\t\x12\x0c\n\x04\x62ody\x18\x02 \x01(\t\x12\"\n\x06\x66ilter\x18\x03 \x01(\x0b\x32\x12.nodereader.Filter\x12*\n\ntimestamps\x18\x04 \x01(\x0b\x32\x16.nodereader.Timestamps\x12\x0e\n\x06\x66ields\x18\x05 \x03(\t\"2\n\x0fRelatedEntities\x12\x10\n\x08\x65ntities\x18\x01 \x03(\t\x12\r\n\x05total\x18\x02 \x01(\r\"\x9e\x01\n\x0fSuggestResponse\x12\r\n\x05total\x18\x01 \x01(\x05\x12,\n\x07results\x18\x02 \x03(\x0b\x32\x1b.nodereader.ParagraphResult\x12\r\n\x05query\x18\x03 \x01(\t\x12\x10\n\x08\x65matches\x18\x04 \x03(\t\x12-\n\x08\x65ntities\x18\x05 \x01(\x0b\x32\x1b.nodereader.RelatedEntities\"\xe6\x01\n\x0eSearchResponse\x12\x34\n\x08\x64ocument\x18\x01 \x01(\x0b\x32\".nodereader.DocumentSearchResponse\x12\x36\n\tparagraph\x18\x02 \x01(\x0b\x32#.nodereader.ParagraphSearchResponse\x12\x30\n\x06vector\x18\x03 \x01(\x0b\x32 .nodereader.VectorSearchResponse\x12\x34\n\x08relation\x18\x04 \x01(\x0b\x32\".nodereader.RelationSearchResponse\"\x1b\n\x0cIdCollection\x12\x0b\n\x03ids\x18\x01 \x03(\t\"Q\n\x0cRelationEdge\x12/\n\tedge_type\x18\x01 \x01(\x0e\x32\x1c.utils.Relation.RelationType\x12\x10\n\x08property\x18\x02 \x01(\t\"2\n\x08\x45\x64geList\x12&\n\x04list\x18\x01 \x03(\x0b\x32\x18.nodereader.RelationEdge\"_\n\x16RelationTypeListMember\x12/\n\twith_type\x18\x01 \x01(\x0e\x32\x1c.utils.RelationNode.NodeType\x12\x14\n\x0cwith_subtype\x18\x02 \x01(\t\"<\n\x08TypeList\x12\x30\n\x04list\x18\x01 \x03(\x0b\x32\".nodereader.RelationTypeListMember\"N\n\x0fGetShardRequest\x12(\n\x08shard_id\x18\x01 \x01(\x0b\x32\x16.noderesources.ShardId\x12\x11\n\tvectorset\x18\x02 \x01(\t\"+\n\rParagraphItem\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0e\n\x06labels\x18\x02 \x03(\t\";\n\x0c\x44ocumentItem\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12\r\n\x05\x66ield\x18\x02 \x01(\t\x12\x0e\n\x06labels\x18\x03 \x03(\t\"m\n\rStreamRequest\x12\"\n\x06\x66ilter\x18\x01 \x01(\x0b\x32\x12.nodereader.Filter\x12\x0e\n\x06reload\x18\x02 \x01(\x08\x12(\n\x08shard_id\x18\x03 \x01(\x0b\x32\x16.noderesources.ShardId2\x9e\t\n\nNodeReader\x12?\n\x08GetShard\x12\x1b.nodereader.GetShardRequest\x1a\x14.noderesources.Shard\"\x00\x12\x42\n\tGetShards\x12\x19.noderesources.EmptyQuery\x1a\x18.noderesources.ShardList\"\x00\x12Y\n\x0e\x44ocumentSearch\x12!.nodereader.DocumentSearchRequest\x1a\".nodereader.DocumentSearchResponse\"\x00\x12\\\n\x0fParagraphSearch\x12\".nodereader.ParagraphSearchRequest\x1a#.nodereader.ParagraphSearchResponse\"\x00\x12S\n\x0cVectorSearch\x12\x1f.nodereader.VectorSearchRequest\x1a .nodereader.VectorSearchResponse\"\x00\x12Y\n\x0eRelationSearch\x12!.nodereader.RelationSearchRequest\x1a\".nodereader.RelationSearchResponse\"\x00\x12\x41\n\x0b\x44ocumentIds\x12\x16.noderesources.ShardId\x1a\x18.nodereader.IdCollection\"\x00\x12\x42\n\x0cParagraphIds\x12\x16.noderesources.ShardId\x1a\x18.nodereader.IdCollection\"\x00\x12?\n\tVectorIds\x12\x16.noderesources.ShardId\x1a\x18.nodereader.IdCollection\"\x00\x12\x41\n\x0bRelationIds\x12\x16.noderesources.ShardId\x1a\x18.nodereader.IdCollection\"\x00\x12?\n\rRelationEdges\x12\x16.noderesources.ShardId\x1a\x14.nodereader.EdgeList\"\x00\x12?\n\rRelationTypes\x12\x16.noderesources.ShardId\x1a\x14.nodereader.TypeList\"\x00\x12\x41\n\x06Search\x12\x19.nodereader.SearchRequest\x1a\x1a.nodereader.SearchResponse\"\x00\x12\x44\n\x07Suggest\x12\x1a.nodereader.SuggestRequest\x1a\x1b.nodereader.SuggestResponse\"\x00\x12\x46\n\nParagraphs\x12\x19.nodereader.StreamRequest\x1a\x19.nodereader.ParagraphItem\"\x00\x30\x01\x12\x44\n\tDocuments\x12\x19.nodereader.StreamRequest\x1a\x18.nodereader.DocumentItem\"\x00\x30\x01P\x00P\x02\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.nodereader_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _ORDERBY.fields_by_name['field']._options = None
  _ORDERBY.fields_by_name['field']._serialized_options = b'\030\001'
  _DOCUMENTSEARCHRESPONSE_FACETSENTRY._options = None
  _DOCUMENTSEARCHRESPONSE_FACETSENTRY._serialized_options = b'8\001'
  _PARAGRAPHSEARCHRESPONSE_FACETSENTRY._options = None
  _PARAGRAPHSEARCHRESPONSE_FACETSENTRY._serialized_options = b'8\001'
  _FILTER._serialized_start=147
  _FILTER._serialized_end=169
  _FACETED._serialized_start=171
  _FACETED._serialized_end=194
  _ORDERBY._serialized_start=197
  _ORDERBY._serialized_end=392
  _ORDERBY_ORDERTYPE._serialized_start=321
  _ORDERBY_ORDERTYPE._serialized_end=351
  _ORDERBY_ORDERFIELD._serialized_start=353
  _ORDERBY_ORDERFIELD._serialized_end=392
  _TIMESTAMPS._serialized_start=395
  _TIMESTAMPS._serialized_end=605
  _FACETRESULT._serialized_start=607
  _FACETRESULT._serialized_end=648
  _FACETRESULTS._serialized_start=650
  _FACETRESULTS._serialized_end=711
  _DOCUMENTSEARCHREQUEST._serialized_start=714
  _DOCUMENTSEARCHREQUEST._serialized_end=1147
  _PARAGRAPHSEARCHREQUEST._serialized_start=1150
  _PARAGRAPHSEARCHREQUEST._serialized_end=1541
  _RESULTSCORE._serialized_start=1543
  _RESULTSCORE._serialized_end=1587
  _DOCUMENTRESULT._serialized_start=1589
  _DOCUMENTRESULT._serialized_end=1690
  _DOCUMENTSEARCHRESPONSE._serialized_start=1693
  _DOCUMENTSEARCHRESPONSE._serialized_end=2008
  _DOCUMENTSEARCHRESPONSE_FACETSENTRY._serialized_start=1937
  _DOCUMENTSEARCHRESPONSE_FACETSENTRY._serialized_end=2008
  _PARAGRAPHRESULT._serialized_start=2011
  _PARAGRAPHRESULT._serialized_end=2259
  _PARAGRAPHSEARCHRESPONSE._serialized_start=2262
  _PARAGRAPHSEARCHRESPONSE._serialized_end=2622
  _PARAGRAPHSEARCHRESPONSE_FACETSENTRY._serialized_start=1937
  _PARAGRAPHSEARCHRESPONSE_FACETSENTRY._serialized_end=2008
  _VECTORSEARCHREQUEST._serialized_start=2625
  _VECTORSEARCHREQUEST._serialized_end=2795
  _DOCUMENTVECTORIDENTIFIER._serialized_start=2797
  _DOCUMENTVECTORIDENTIFIER._serialized_end=2835
  _DOCUMENTSCORED._serialized_start=2837
  _DOCUMENTSCORED._serialized_end=2922
  _VECTORSEARCHRESPONSE._serialized_start=2924
  _VECTORSEARCHRESPONSE._serialized_end=3039
  _RELATIONNODEFILTER._serialized_start=3041
  _RELATIONNODEFILTER._serialized_end=3154
  _RELATIONEDGEFILTER._serialized_start=3156
  _RELATIONEDGEFILTER._serialized_end=3281
  _RELATIONPREFIXSEARCHREQUEST._serialized_start=3283
  _RELATIONPREFIXSEARCHREQUEST._serialized_end=3328
  _RELATIONPREFIXSEARCHRESPONSE._serialized_start=3330
  _RELATIONPREFIXSEARCHRESPONSE._serialized_end=3396
  _ENTITIESSUBGRAPHREQUEST._serialized_start=3399
  _ENTITIESSUBGRAPHREQUEST._serialized_end=3605
  _ENTITIESSUBGRAPHRESPONSE._serialized_start=3607
  _ENTITIESSUBGRAPHRESPONSE._serialized_end=3669
  _RELATIONSEARCHREQUEST._serialized_start=3672
  _RELATIONSEARCHREQUEST._serialized_end=3841
  _RELATIONSEARCHRESPONSE._serialized_start=3844
  _RELATIONSEARCHRESPONSE._serialized_end=3982
  _SEARCHREQUEST._serialized_start=3985
  _SEARCHREQUEST._serialized_end=4564
  _SUGGESTREQUEST._serialized_start=4567
  _SUGGESTREQUEST._serialized_end=4708
  _RELATEDENTITIES._serialized_start=4710
  _RELATEDENTITIES._serialized_end=4760
  _SUGGESTRESPONSE._serialized_start=4763
  _SUGGESTRESPONSE._serialized_end=4921
  _SEARCHRESPONSE._serialized_start=4924
  _SEARCHRESPONSE._serialized_end=5154
  _IDCOLLECTION._serialized_start=5156
  _IDCOLLECTION._serialized_end=5183
  _RELATIONEDGE._serialized_start=5185
  _RELATIONEDGE._serialized_end=5266
  _EDGELIST._serialized_start=5268
  _EDGELIST._serialized_end=5318
  _RELATIONTYPELISTMEMBER._serialized_start=5320
  _RELATIONTYPELISTMEMBER._serialized_end=5415
  _TYPELIST._serialized_start=5417
  _TYPELIST._serialized_end=5477
  _GETSHARDREQUEST._serialized_start=5479
  _GETSHARDREQUEST._serialized_end=5557
  _PARAGRAPHITEM._serialized_start=5559
  _PARAGRAPHITEM._serialized_end=5602
  _DOCUMENTITEM._serialized_start=5604
  _DOCUMENTITEM._serialized_end=5663
  _STREAMREQUEST._serialized_start=5665
  _STREAMREQUEST._serialized_end=5774
  _NODEREADER._serialized_start=5777
  _NODEREADER._serialized_end=6959
# @@protoc_insertion_point(module_scope)
