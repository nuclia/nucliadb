# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/nodereader.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
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

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n nucliadb_protos/nodereader.proto\x12\nnodereader\x1a#nucliadb_protos/noderesources.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1bnucliadb_protos/utils.proto\"\x16\n\x06\x46ilter\x12\x0c\n\x04tags\x18\x01 \x03(\t\"\x17\n\x07\x46\x61\x63\x65ted\x12\x0c\n\x04tags\x18\x01 \x03(\t\"e\n\x07OrderBy\x12\r\n\x05\x66ield\x18\x01 \x01(\t\x12+\n\x04type\x18\x02 \x01(\x0e\x32\x1d.nodereader.OrderBy.OrderType\"\x1e\n\tOrderType\x12\x08\n\x04\x44\x45SC\x10\x00\x12\x07\n\x03\x41SC\x10\x01\"\xd2\x01\n\nTimestamps\x12\x31\n\rfrom_modified\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12/\n\x0bto_modified\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x30\n\x0c\x66rom_created\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12.\n\nto_created\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\")\n\x0b\x46\x61\x63\x65tResult\x12\x0b\n\x03tag\x18\x01 \x01(\t\x12\r\n\x05total\x18\x02 \x01(\x05\"=\n\x0c\x46\x61\x63\x65tResults\x12-\n\x0c\x66\x61\x63\x65tresults\x18\x01 \x03(\x0b\x32\x17.nodereader.FacetResult\"\x99\x02\n\x15\x44ocumentSearchRequest\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04\x62ody\x18\x02 \x01(\t\x12\x0e\n\x06\x66ields\x18\x03 \x03(\t\x12\"\n\x06\x66ilter\x18\x04 \x01(\x0b\x32\x12.nodereader.Filter\x12\"\n\x05order\x18\x05 \x01(\x0b\x32\x13.nodereader.OrderBy\x12$\n\x07\x66\x61\x63\x65ted\x18\x06 \x01(\x0b\x32\x13.nodereader.Faceted\x12\x13\n\x0bpage_number\x18\x07 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x08 \x01(\x05\x12*\n\ntimestamps\x18\t \x01(\x0b\x32\x16.nodereader.Timestamps\x12\x0e\n\x06reload\x18\n \x01(\x08\"\xa8\x02\n\x16ParagraphSearchRequest\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12\x0e\n\x06\x66ields\x18\x03 \x03(\t\x12\x0c\n\x04\x62ody\x18\x04 \x01(\t\x12\"\n\x06\x66ilter\x18\x05 \x01(\x0b\x32\x12.nodereader.Filter\x12\"\n\x05order\x18\x07 \x01(\x0b\x32\x13.nodereader.OrderBy\x12$\n\x07\x66\x61\x63\x65ted\x18\x08 \x01(\x0b\x32\x13.nodereader.Faceted\x12\x13\n\x0bpage_number\x18\n \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x0b \x01(\x05\x12*\n\ntimestamps\x18\x0c \x01(\x0b\x32\x16.nodereader.Timestamps\x12\x0e\n\x06reload\x18\r \x01(\x08\"P\n\x0e\x44ocumentResult\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12\r\n\x05score\x18\x02 \x01(\x04\x12\x12\n\nscore_bm25\x18\x03 \x01(\x02\x12\r\n\x05\x66ield\x18\x04 \x01(\t\"\x8b\x02\n\x16\x44ocumentSearchResponse\x12\r\n\x05total\x18\x01 \x01(\x05\x12+\n\x07results\x18\x02 \x03(\x0b\x32\x1a.nodereader.DocumentResult\x12>\n\x06\x66\x61\x63\x65ts\x18\x03 \x03(\x0b\x32..nodereader.DocumentSearchResponse.FacetsEntry\x12\x13\n\x0bpage_number\x18\x04 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x05 \x01(\x05\x1aG\n\x0b\x46\x61\x63\x65tsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\'\n\x05value\x18\x02 \x01(\x0b\x32\x18.nodereader.FacetResults:\x02\x38\x01\"\x8a\x01\n\x0fParagraphResult\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12\r\n\x05score\x18\x02 \x01(\x02\x12\r\n\x05\x66ield\x18\x03 \x01(\t\x12\r\n\x05start\x18\x04 \x01(\x04\x12\x0b\n\x03\x65nd\x18\x05 \x01(\x04\x12\x11\n\tparagraph\x18\x06 \x01(\t\x12\r\n\x05split\x18\x07 \x01(\t\x12\r\n\x05index\x18\x08 \x01(\x04\"\x8e\x02\n\x17ParagraphSearchResponse\x12\r\n\x05total\x18\x01 \x01(\x05\x12,\n\x07results\x18\x02 \x03(\x0b\x32\x1b.nodereader.ParagraphResult\x12?\n\x06\x66\x61\x63\x65ts\x18\x03 \x03(\x0b\x32/.nodereader.ParagraphSearchResponse.FacetsEntry\x12\x13\n\x0bpage_number\x18\x04 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x05 \x01(\x05\x1aG\n\x0b\x46\x61\x63\x65tsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\'\n\x05value\x18\x02 \x01(\x0b\x32\x18.nodereader.FacetResults:\x02\x38\x01\"O\n\x13VectorSearchRequest\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0e\n\x06vector\x18\x02 \x03(\x02\x12\x0c\n\x04tags\x18\x03 \x03(\t\x12\x0e\n\x06reload\x18\r \x01(\x08\"&\n\x18\x44ocumentVectorIdentifier\x12\n\n\x02id\x18\x01 \x01(\t\"U\n\x0e\x44ocumentScored\x12\x34\n\x06\x64oc_id\x18\x01 \x01(\x0b\x32$.nodereader.DocumentVectorIdentifier\x12\r\n\x05score\x18\x02 \x01(\x02\"E\n\x14VectorSearchResponse\x12-\n\tdocuments\x18\x01 \x03(\x0b\x32\x1a.nodereader.DocumentScored\"N\n\x0eRelationFilter\x12+\n\x05ntype\x18\x01 \x01(\x0e\x32\x1c.utils.RelationNode.NodeType\x12\x0f\n\x07subtype\x18\x02 \x01(\t\"\xaf\x01\n\x15RelationSearchRequest\x12\n\n\x02id\x18\x01 \x01(\t\x12)\n\x0c\x65ntry_points\x18\x02 \x03(\x0b\x32\x13.utils.RelationNode\x12\x30\n\x0ctype_filters\x18\x03 \x03(\x0b\x32\x1a.nodereader.RelationFilter\x12\r\n\x05\x64\x65pth\x18\x04 \x01(\x05\x12\x0e\n\x06prefix\x18\x05 \x01(\t\x12\x0e\n\x06reload\x18\r \x01(\x08\"A\n\x16RelationSearchResponse\x12\'\n\nneighbours\x18\x01 \x03(\x0b\x32\x13.utils.RelationNode\"\xa4\x02\n\rSearchRequest\x12\r\n\x05shard\x18\x01 \x01(\t\x12\x0e\n\x06\x66ields\x18\x02 \x03(\t\x12\x0c\n\x04\x62ody\x18\x03 \x01(\t\x12\"\n\x06\x66ilter\x18\x04 \x01(\x0b\x32\x12.nodereader.Filter\x12\"\n\x05order\x18\x05 \x01(\x0b\x32\x13.nodereader.OrderBy\x12$\n\x07\x66\x61\x63\x65ted\x18\x06 \x01(\x0b\x32\x13.nodereader.Faceted\x12\x13\n\x0bpage_number\x18\x07 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x08 \x01(\x05\x12*\n\ntimestamps\x18\t \x01(\x0b\x32\x16.nodereader.Timestamps\x12\x0e\n\x06vector\x18\n \x03(\x02\x12\x0e\n\x06reload\x18\x0b \x01(\x08\"}\n\x0eSuggestRequest\x12\r\n\x05shard\x18\x01 \x01(\t\x12\x0c\n\x04\x62ody\x18\x02 \x01(\t\x12\"\n\x06\x66ilter\x18\x03 \x01(\x0b\x32\x12.nodereader.Filter\x12*\n\ntimestamps\x18\x04 \x01(\x0b\x32\x16.nodereader.Timestamps\"N\n\x0fSuggestResponse\x12\r\n\x05total\x18\x01 \x01(\x05\x12,\n\x07results\x18\x02 \x03(\x0b\x32\x1b.nodereader.ParagraphResult\"\xb0\x01\n\x0eSearchResponse\x12\x34\n\x08\x64ocument\x18\x01 \x01(\x0b\x32\".nodereader.DocumentSearchResponse\x12\x36\n\tparagraph\x18\x02 \x01(\x0b\x32#.nodereader.ParagraphSearchResponse\x12\x30\n\x06vector\x18\x03 \x01(\x0b\x32 .nodereader.VectorSearchResponse\"\x1b\n\x0cIdCollection\x12\x0b\n\x03ids\x18\x01 \x03(\t\"Q\n\x0cRelationEdge\x12/\n\tedge_type\x18\x01 \x01(\x0e\x32\x1c.utils.Relation.RelationType\x12\x10\n\x08property\x18\x02 \x01(\t\"2\n\x08\x45\x64geList\x12&\n\x04list\x18\x01 \x03(\x0b\x32\x18.nodereader.RelationEdge\"L\n\x0cRelationType\x12\x11\n\twith_type\x18\x01 \x01(\t\x12\x14\n\x0cwith_subtype\x18\x02 \x01(\t\x12\x13\n\x0bhas_subtype\x18\x03 \x01(\x08\"2\n\x08TypeList\x12&\n\x04list\x18\x01 \x03(\x0b\x32\x18.nodereader.RelationType2\x8b\x08\n\nNodeReader\x12:\n\x08GetShard\x12\x16.noderesources.ShardId\x1a\x14.noderesources.Shard\"\x00\x12\x42\n\tGetShards\x12\x19.noderesources.EmptyQuery\x1a\x18.noderesources.ShardList\"\x00\x12Y\n\x0e\x44ocumentSearch\x12!.nodereader.DocumentSearchRequest\x1a\".nodereader.DocumentSearchResponse\"\x00\x12\\\n\x0fParagraphSearch\x12\".nodereader.ParagraphSearchRequest\x1a#.nodereader.ParagraphSearchResponse\"\x00\x12S\n\x0cVectorSearch\x12\x1f.nodereader.VectorSearchRequest\x1a .nodereader.VectorSearchResponse\"\x00\x12Y\n\x0eRelationSearch\x12!.nodereader.RelationSearchRequest\x1a\".nodereader.RelationSearchResponse\"\x00\x12\x41\n\x0b\x44ocumentIds\x12\x16.noderesources.ShardId\x1a\x18.nodereader.IdCollection\"\x00\x12\x42\n\x0cParagraphIds\x12\x16.noderesources.ShardId\x1a\x18.nodereader.IdCollection\"\x00\x12?\n\tVectorIds\x12\x16.noderesources.ShardId\x1a\x18.nodereader.IdCollection\"\x00\x12\x41\n\x0bRelationIds\x12\x16.noderesources.ShardId\x1a\x18.nodereader.IdCollection\"\x00\x12?\n\rRelationEdges\x12\x16.noderesources.ShardId\x1a\x14.nodereader.EdgeList\"\x00\x12?\n\rRelationTypes\x12\x16.noderesources.ShardId\x1a\x14.nodereader.TypeList\"\x00\x12\x41\n\x06Search\x12\x19.nodereader.SearchRequest\x1a\x1a.nodereader.SearchResponse\"\x00\x12\x44\n\x07Suggest\x12\x1a.nodereader.SuggestRequest\x1a\x1b.nodereader.SuggestResponse\"\x00P\x00P\x02\x62\x06proto3')



_FILTER = DESCRIPTOR.message_types_by_name['Filter']
_FACETED = DESCRIPTOR.message_types_by_name['Faceted']
_ORDERBY = DESCRIPTOR.message_types_by_name['OrderBy']
_TIMESTAMPS = DESCRIPTOR.message_types_by_name['Timestamps']
_FACETRESULT = DESCRIPTOR.message_types_by_name['FacetResult']
_FACETRESULTS = DESCRIPTOR.message_types_by_name['FacetResults']
_DOCUMENTSEARCHREQUEST = DESCRIPTOR.message_types_by_name['DocumentSearchRequest']
_PARAGRAPHSEARCHREQUEST = DESCRIPTOR.message_types_by_name['ParagraphSearchRequest']
_DOCUMENTRESULT = DESCRIPTOR.message_types_by_name['DocumentResult']
_DOCUMENTSEARCHRESPONSE = DESCRIPTOR.message_types_by_name['DocumentSearchResponse']
_DOCUMENTSEARCHRESPONSE_FACETSENTRY = _DOCUMENTSEARCHRESPONSE.nested_types_by_name['FacetsEntry']
_PARAGRAPHRESULT = DESCRIPTOR.message_types_by_name['ParagraphResult']
_PARAGRAPHSEARCHRESPONSE = DESCRIPTOR.message_types_by_name['ParagraphSearchResponse']
_PARAGRAPHSEARCHRESPONSE_FACETSENTRY = _PARAGRAPHSEARCHRESPONSE.nested_types_by_name['FacetsEntry']
_VECTORSEARCHREQUEST = DESCRIPTOR.message_types_by_name['VectorSearchRequest']
_DOCUMENTVECTORIDENTIFIER = DESCRIPTOR.message_types_by_name['DocumentVectorIdentifier']
_DOCUMENTSCORED = DESCRIPTOR.message_types_by_name['DocumentScored']
_VECTORSEARCHRESPONSE = DESCRIPTOR.message_types_by_name['VectorSearchResponse']
_RELATIONFILTER = DESCRIPTOR.message_types_by_name['RelationFilter']
_RELATIONSEARCHREQUEST = DESCRIPTOR.message_types_by_name['RelationSearchRequest']
_RELATIONSEARCHRESPONSE = DESCRIPTOR.message_types_by_name['RelationSearchResponse']
_SEARCHREQUEST = DESCRIPTOR.message_types_by_name['SearchRequest']
_SUGGESTREQUEST = DESCRIPTOR.message_types_by_name['SuggestRequest']
_SUGGESTRESPONSE = DESCRIPTOR.message_types_by_name['SuggestResponse']
_SEARCHRESPONSE = DESCRIPTOR.message_types_by_name['SearchResponse']
_IDCOLLECTION = DESCRIPTOR.message_types_by_name['IdCollection']
_RELATIONEDGE = DESCRIPTOR.message_types_by_name['RelationEdge']
_EDGELIST = DESCRIPTOR.message_types_by_name['EdgeList']
_RELATIONTYPE = DESCRIPTOR.message_types_by_name['RelationType']
_TYPELIST = DESCRIPTOR.message_types_by_name['TypeList']
_ORDERBY_ORDERTYPE = _ORDERBY.enum_types_by_name['OrderType']
Filter = _reflection.GeneratedProtocolMessageType('Filter', (_message.Message,), {
  'DESCRIPTOR' : _FILTER,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.Filter)
  })
_sym_db.RegisterMessage(Filter)

Faceted = _reflection.GeneratedProtocolMessageType('Faceted', (_message.Message,), {
  'DESCRIPTOR' : _FACETED,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.Faceted)
  })
_sym_db.RegisterMessage(Faceted)

OrderBy = _reflection.GeneratedProtocolMessageType('OrderBy', (_message.Message,), {
  'DESCRIPTOR' : _ORDERBY,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.OrderBy)
  })
_sym_db.RegisterMessage(OrderBy)

Timestamps = _reflection.GeneratedProtocolMessageType('Timestamps', (_message.Message,), {
  'DESCRIPTOR' : _TIMESTAMPS,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.Timestamps)
  })
_sym_db.RegisterMessage(Timestamps)

FacetResult = _reflection.GeneratedProtocolMessageType('FacetResult', (_message.Message,), {
  'DESCRIPTOR' : _FACETRESULT,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.FacetResult)
  })
_sym_db.RegisterMessage(FacetResult)

FacetResults = _reflection.GeneratedProtocolMessageType('FacetResults', (_message.Message,), {
  'DESCRIPTOR' : _FACETRESULTS,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.FacetResults)
  })
_sym_db.RegisterMessage(FacetResults)

DocumentSearchRequest = _reflection.GeneratedProtocolMessageType('DocumentSearchRequest', (_message.Message,), {
  'DESCRIPTOR' : _DOCUMENTSEARCHREQUEST,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.DocumentSearchRequest)
  })
_sym_db.RegisterMessage(DocumentSearchRequest)

ParagraphSearchRequest = _reflection.GeneratedProtocolMessageType('ParagraphSearchRequest', (_message.Message,), {
  'DESCRIPTOR' : _PARAGRAPHSEARCHREQUEST,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.ParagraphSearchRequest)
  })
_sym_db.RegisterMessage(ParagraphSearchRequest)

DocumentResult = _reflection.GeneratedProtocolMessageType('DocumentResult', (_message.Message,), {
  'DESCRIPTOR' : _DOCUMENTRESULT,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.DocumentResult)
  })
_sym_db.RegisterMessage(DocumentResult)

DocumentSearchResponse = _reflection.GeneratedProtocolMessageType('DocumentSearchResponse', (_message.Message,), {

  'FacetsEntry' : _reflection.GeneratedProtocolMessageType('FacetsEntry', (_message.Message,), {
    'DESCRIPTOR' : _DOCUMENTSEARCHRESPONSE_FACETSENTRY,
    '__module__' : 'nucliadb_protos.nodereader_pb2'
    # @@protoc_insertion_point(class_scope:nodereader.DocumentSearchResponse.FacetsEntry)
    })
  ,
  'DESCRIPTOR' : _DOCUMENTSEARCHRESPONSE,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.DocumentSearchResponse)
  })
_sym_db.RegisterMessage(DocumentSearchResponse)
_sym_db.RegisterMessage(DocumentSearchResponse.FacetsEntry)

ParagraphResult = _reflection.GeneratedProtocolMessageType('ParagraphResult', (_message.Message,), {
  'DESCRIPTOR' : _PARAGRAPHRESULT,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.ParagraphResult)
  })
_sym_db.RegisterMessage(ParagraphResult)

ParagraphSearchResponse = _reflection.GeneratedProtocolMessageType('ParagraphSearchResponse', (_message.Message,), {

  'FacetsEntry' : _reflection.GeneratedProtocolMessageType('FacetsEntry', (_message.Message,), {
    'DESCRIPTOR' : _PARAGRAPHSEARCHRESPONSE_FACETSENTRY,
    '__module__' : 'nucliadb_protos.nodereader_pb2'
    # @@protoc_insertion_point(class_scope:nodereader.ParagraphSearchResponse.FacetsEntry)
    })
  ,
  'DESCRIPTOR' : _PARAGRAPHSEARCHRESPONSE,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.ParagraphSearchResponse)
  })
_sym_db.RegisterMessage(ParagraphSearchResponse)
_sym_db.RegisterMessage(ParagraphSearchResponse.FacetsEntry)

VectorSearchRequest = _reflection.GeneratedProtocolMessageType('VectorSearchRequest', (_message.Message,), {
  'DESCRIPTOR' : _VECTORSEARCHREQUEST,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.VectorSearchRequest)
  })
_sym_db.RegisterMessage(VectorSearchRequest)

DocumentVectorIdentifier = _reflection.GeneratedProtocolMessageType('DocumentVectorIdentifier', (_message.Message,), {
  'DESCRIPTOR' : _DOCUMENTVECTORIDENTIFIER,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.DocumentVectorIdentifier)
  })
_sym_db.RegisterMessage(DocumentVectorIdentifier)

DocumentScored = _reflection.GeneratedProtocolMessageType('DocumentScored', (_message.Message,), {
  'DESCRIPTOR' : _DOCUMENTSCORED,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.DocumentScored)
  })
_sym_db.RegisterMessage(DocumentScored)

VectorSearchResponse = _reflection.GeneratedProtocolMessageType('VectorSearchResponse', (_message.Message,), {
  'DESCRIPTOR' : _VECTORSEARCHRESPONSE,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.VectorSearchResponse)
  })
_sym_db.RegisterMessage(VectorSearchResponse)

RelationFilter = _reflection.GeneratedProtocolMessageType('RelationFilter', (_message.Message,), {
  'DESCRIPTOR' : _RELATIONFILTER,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.RelationFilter)
  })
_sym_db.RegisterMessage(RelationFilter)

RelationSearchRequest = _reflection.GeneratedProtocolMessageType('RelationSearchRequest', (_message.Message,), {
  'DESCRIPTOR' : _RELATIONSEARCHREQUEST,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.RelationSearchRequest)
  })
_sym_db.RegisterMessage(RelationSearchRequest)

RelationSearchResponse = _reflection.GeneratedProtocolMessageType('RelationSearchResponse', (_message.Message,), {
  'DESCRIPTOR' : _RELATIONSEARCHRESPONSE,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.RelationSearchResponse)
  })
_sym_db.RegisterMessage(RelationSearchResponse)

SearchRequest = _reflection.GeneratedProtocolMessageType('SearchRequest', (_message.Message,), {
  'DESCRIPTOR' : _SEARCHREQUEST,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.SearchRequest)
  })
_sym_db.RegisterMessage(SearchRequest)

SuggestRequest = _reflection.GeneratedProtocolMessageType('SuggestRequest', (_message.Message,), {
  'DESCRIPTOR' : _SUGGESTREQUEST,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.SuggestRequest)
  })
_sym_db.RegisterMessage(SuggestRequest)

SuggestResponse = _reflection.GeneratedProtocolMessageType('SuggestResponse', (_message.Message,), {
  'DESCRIPTOR' : _SUGGESTRESPONSE,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.SuggestResponse)
  })
_sym_db.RegisterMessage(SuggestResponse)

SearchResponse = _reflection.GeneratedProtocolMessageType('SearchResponse', (_message.Message,), {
  'DESCRIPTOR' : _SEARCHRESPONSE,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.SearchResponse)
  })
_sym_db.RegisterMessage(SearchResponse)

IdCollection = _reflection.GeneratedProtocolMessageType('IdCollection', (_message.Message,), {
  'DESCRIPTOR' : _IDCOLLECTION,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.IdCollection)
  })
_sym_db.RegisterMessage(IdCollection)

RelationEdge = _reflection.GeneratedProtocolMessageType('RelationEdge', (_message.Message,), {
  'DESCRIPTOR' : _RELATIONEDGE,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.RelationEdge)
  })
_sym_db.RegisterMessage(RelationEdge)

EdgeList = _reflection.GeneratedProtocolMessageType('EdgeList', (_message.Message,), {
  'DESCRIPTOR' : _EDGELIST,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.EdgeList)
  })
_sym_db.RegisterMessage(EdgeList)

RelationType = _reflection.GeneratedProtocolMessageType('RelationType', (_message.Message,), {
  'DESCRIPTOR' : _RELATIONTYPE,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.RelationType)
  })
_sym_db.RegisterMessage(RelationType)

TypeList = _reflection.GeneratedProtocolMessageType('TypeList', (_message.Message,), {
  'DESCRIPTOR' : _TYPELIST,
  '__module__' : 'nucliadb_protos.nodereader_pb2'
  # @@protoc_insertion_point(class_scope:nodereader.TypeList)
  })
_sym_db.RegisterMessage(TypeList)

_NODEREADER = DESCRIPTOR.services_by_name['NodeReader']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _DOCUMENTSEARCHRESPONSE_FACETSENTRY._options = None
  _DOCUMENTSEARCHRESPONSE_FACETSENTRY._serialized_options = b'8\001'
  _PARAGRAPHSEARCHRESPONSE_FACETSENTRY._options = None
  _PARAGRAPHSEARCHRESPONSE_FACETSENTRY._serialized_options = b'8\001'
  _FILTER._serialized_start=147
  _FILTER._serialized_end=169
  _FACETED._serialized_start=171
  _FACETED._serialized_end=194
  _ORDERBY._serialized_start=196
  _ORDERBY._serialized_end=297
  _ORDERBY_ORDERTYPE._serialized_start=267
  _ORDERBY_ORDERTYPE._serialized_end=297
  _TIMESTAMPS._serialized_start=300
  _TIMESTAMPS._serialized_end=510
  _FACETRESULT._serialized_start=512
  _FACETRESULT._serialized_end=553
  _FACETRESULTS._serialized_start=555
  _FACETRESULTS._serialized_end=616
  _DOCUMENTSEARCHREQUEST._serialized_start=619
  _DOCUMENTSEARCHREQUEST._serialized_end=900
  _PARAGRAPHSEARCHREQUEST._serialized_start=903
  _PARAGRAPHSEARCHREQUEST._serialized_end=1199
  _DOCUMENTRESULT._serialized_start=1201
  _DOCUMENTRESULT._serialized_end=1281
  _DOCUMENTSEARCHRESPONSE._serialized_start=1284
  _DOCUMENTSEARCHRESPONSE._serialized_end=1551
  _DOCUMENTSEARCHRESPONSE_FACETSENTRY._serialized_start=1480
  _DOCUMENTSEARCHRESPONSE_FACETSENTRY._serialized_end=1551
  _PARAGRAPHRESULT._serialized_start=1554
  _PARAGRAPHRESULT._serialized_end=1692
  _PARAGRAPHSEARCHRESPONSE._serialized_start=1695
  _PARAGRAPHSEARCHRESPONSE._serialized_end=1965
  _PARAGRAPHSEARCHRESPONSE_FACETSENTRY._serialized_start=1480
  _PARAGRAPHSEARCHRESPONSE_FACETSENTRY._serialized_end=1551
  _VECTORSEARCHREQUEST._serialized_start=1967
  _VECTORSEARCHREQUEST._serialized_end=2046
  _DOCUMENTVECTORIDENTIFIER._serialized_start=2048
  _DOCUMENTVECTORIDENTIFIER._serialized_end=2086
  _DOCUMENTSCORED._serialized_start=2088
  _DOCUMENTSCORED._serialized_end=2173
  _VECTORSEARCHRESPONSE._serialized_start=2175
  _VECTORSEARCHRESPONSE._serialized_end=2244
  _RELATIONFILTER._serialized_start=2246
  _RELATIONFILTER._serialized_end=2324
  _RELATIONSEARCHREQUEST._serialized_start=2327
  _RELATIONSEARCHREQUEST._serialized_end=2502
  _RELATIONSEARCHRESPONSE._serialized_start=2504
  _RELATIONSEARCHRESPONSE._serialized_end=2569
  _SEARCHREQUEST._serialized_start=2572
  _SEARCHREQUEST._serialized_end=2864
  _SUGGESTREQUEST._serialized_start=2866
  _SUGGESTREQUEST._serialized_end=2991
  _SUGGESTRESPONSE._serialized_start=2993
  _SUGGESTRESPONSE._serialized_end=3071
  _SEARCHRESPONSE._serialized_start=3074
  _SEARCHRESPONSE._serialized_end=3250
  _IDCOLLECTION._serialized_start=3252
  _IDCOLLECTION._serialized_end=3279
  _RELATIONEDGE._serialized_start=3281
  _RELATIONEDGE._serialized_end=3362
  _EDGELIST._serialized_start=3364
  _EDGELIST._serialized_end=3414
  _RELATIONTYPE._serialized_start=3416
  _RELATIONTYPE._serialized_end=3492
  _TYPELIST._serialized_start=3494
  _TYPELIST._serialized_end=3544
  _NODEREADER._serialized_start=3547
  _NODEREADER._serialized_end=4582
# @@protoc_insertion_point(module_scope)
