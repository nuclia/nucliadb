# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/nodereader.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
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

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n nucliadb_protos/nodereader.proto\x12\nnodereader\x1a#nucliadb_protos/noderesources.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1bnucliadb_protos/utils.proto\"\x82\x01\n\x0cStreamFilter\x12\x39\n\x0b\x63onjunction\x18\x01 \x01(\x0e\x32$.nodereader.StreamFilter.Conjunction\x12\x0e\n\x06labels\x18\x02 \x03(\t\"\'\n\x0b\x43onjunction\x12\x07\n\x03\x41ND\x10\x00\x12\x06\n\x02OR\x10\x01\x12\x07\n\x03NOT\x10\x02\"\x19\n\x07\x46\x61\x63\x65ted\x12\x0e\n\x06labels\x18\x01 \x03(\t\"\xc3\x01\n\x07OrderBy\x12\x11\n\x05\x66ield\x18\x01 \x01(\tB\x02\x18\x01\x12+\n\x04type\x18\x02 \x01(\x0e\x32\x1d.nodereader.OrderBy.OrderType\x12/\n\x07sort_by\x18\x03 \x01(\x0e\x32\x1e.nodereader.OrderBy.OrderField\"\x1e\n\tOrderType\x12\x08\n\x04\x44\x45SC\x10\x00\x12\x07\n\x03\x41SC\x10\x01\"\'\n\nOrderField\x12\x0b\n\x07\x43REATED\x10\x00\x12\x0c\n\x08MODIFIED\x10\x01\")\n\x0b\x46\x61\x63\x65tResult\x12\x0b\n\x03tag\x18\x01 \x01(\t\x12\r\n\x05total\x18\x02 \x01(\x05\"=\n\x0c\x46\x61\x63\x65tResults\x12-\n\x0c\x66\x61\x63\x65tresults\x18\x01 \x03(\x0b\x32\x17.nodereader.FacetResult\",\n\x0bResultScore\x12\x0c\n\x04\x62m25\x18\x01 \x01(\x02\x12\x0f\n\x07\x62ooster\x18\x02 \x01(\x02\"e\n\x0e\x44ocumentResult\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12&\n\x05score\x18\x03 \x01(\x0b\x32\x17.nodereader.ResultScore\x12\r\n\x05\x66ield\x18\x04 \x01(\t\x12\x0e\n\x06labels\x18\x05 \x03(\t\"\xbb\x02\n\x16\x44ocumentSearchResponse\x12\r\n\x05total\x18\x01 \x01(\x05\x12+\n\x07results\x18\x02 \x03(\x0b\x32\x1a.nodereader.DocumentResult\x12>\n\x06\x66\x61\x63\x65ts\x18\x03 \x03(\x0b\x32..nodereader.DocumentSearchResponse.FacetsEntry\x12\x13\n\x0bpage_number\x18\x04 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x05 \x01(\x05\x12\r\n\x05query\x18\x06 \x01(\t\x12\x11\n\tnext_page\x18\x07 \x01(\x08\x12\x0c\n\x04\x62m25\x18\x08 \x01(\x08\x1aG\n\x0b\x46\x61\x63\x65tsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\'\n\x05value\x18\x02 \x01(\x0b\x32\x18.nodereader.FacetResults:\x02\x38\x01\"\xf8\x01\n\x0fParagraphResult\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12\r\n\x05\x66ield\x18\x03 \x01(\t\x12\r\n\x05start\x18\x04 \x01(\x04\x12\x0b\n\x03\x65nd\x18\x05 \x01(\x04\x12\x11\n\tparagraph\x18\x06 \x01(\t\x12\r\n\x05split\x18\x07 \x01(\t\x12\r\n\x05index\x18\x08 \x01(\x04\x12&\n\x05score\x18\t \x01(\x0b\x32\x17.nodereader.ResultScore\x12\x0f\n\x07matches\x18\n \x03(\t\x12\x32\n\x08metadata\x18\x0b \x01(\x0b\x32 .noderesources.ParagraphMetadata\x12\x0e\n\x06labels\x18\x0c \x03(\t\"\xe8\x02\n\x17ParagraphSearchResponse\x12\x16\n\x0e\x66uzzy_distance\x18\n \x01(\x05\x12\r\n\x05total\x18\x01 \x01(\x05\x12,\n\x07results\x18\x02 \x03(\x0b\x32\x1b.nodereader.ParagraphResult\x12?\n\x06\x66\x61\x63\x65ts\x18\x03 \x03(\x0b\x32/.nodereader.ParagraphSearchResponse.FacetsEntry\x12\x13\n\x0bpage_number\x18\x04 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x05 \x01(\x05\x12\r\n\x05query\x18\x06 \x01(\t\x12\x11\n\tnext_page\x18\x07 \x01(\x08\x12\x0c\n\x04\x62m25\x18\x08 \x01(\x08\x12\x10\n\x08\x65matches\x18\t \x03(\t\x1aG\n\x0b\x46\x61\x63\x65tsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\'\n\x05value\x18\x02 \x01(\x0b\x32\x18.nodereader.FacetResults:\x02\x38\x01\"&\n\x18\x44ocumentVectorIdentifier\x12\n\n\x02id\x18\x01 \x01(\t\"\x98\x01\n\x0e\x44ocumentScored\x12\x34\n\x06\x64oc_id\x18\x01 \x01(\x0b\x32$.nodereader.DocumentVectorIdentifier\x12\r\n\x05score\x18\x02 \x01(\x02\x12\x31\n\x08metadata\x18\x03 \x01(\x0b\x32\x1f.noderesources.SentenceMetadata\x12\x0e\n\x06labels\x18\x04 \x03(\t\"s\n\x14VectorSearchResponse\x12-\n\tdocuments\x18\x01 \x03(\x0b\x32\x1a.nodereader.DocumentScored\x12\x13\n\x0bpage_number\x18\x04 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x05 \x01(\x05\"q\n\x12RelationNodeFilter\x12/\n\tnode_type\x18\x01 \x01(\x0e\x32\x1c.utils.RelationNode.NodeType\x12\x19\n\x0cnode_subtype\x18\x02 \x01(\tH\x00\x88\x01\x01\x42\x0f\n\r_node_subtype\"\x95\x01\n\x12RelationEdgeFilter\x12\x33\n\rrelation_type\x18\x01 \x01(\x0e\x32\x1c.utils.Relation.RelationType\x12\x1d\n\x10relation_subtype\x18\x02 \x01(\tH\x00\x88\x01\x01\x12\x16\n\x0erelation_value\x18\x03 \x03(\tB\x13\n\x11_relation_subtype\"\x80\x01\n\x1bRelationPrefixSearchRequest\x12\x10\n\x06prefix\x18\x01 \x01(\tH\x00\x12\x0f\n\x05query\x18\x03 \x01(\tH\x00\x12\x34\n\x0cnode_filters\x18\x02 \x03(\x0b\x32\x1e.nodereader.RelationNodeFilterB\x08\n\x06search\"B\n\x1cRelationPrefixSearchResponse\x12\"\n\x05nodes\x18\x01 \x03(\x0b\x32\x13.utils.RelationNode\"\x87\x02\n\x17\x45ntitiesSubgraphRequest\x12)\n\x0c\x65ntry_points\x18\x01 \x03(\x0b\x32\x13.utils.RelationNode\x12\x12\n\x05\x64\x65pth\x18\x03 \x01(\x05H\x00\x88\x01\x01\x12M\n\x10\x64\x65leted_entities\x18\x04 \x03(\x0b\x32\x33.nodereader.EntitiesSubgraphRequest.DeletedEntities\x12\x16\n\x0e\x64\x65leted_groups\x18\x05 \x03(\t\x1a<\n\x0f\x44\x65letedEntities\x12\x14\n\x0cnode_subtype\x18\x01 \x01(\t\x12\x13\n\x0bnode_values\x18\x02 \x03(\tB\x08\n\x06_depth\"K\n\x18\x45ntitiesSubgraphResponse\x12/\n\trelations\x18\x02 \x03(\x0b\x32\x1c.noderesources.IndexRelation\"\x88\x0b\n\nGraphQuery\x12.\n\x04path\x18\x01 \x01(\x0b\x32 .nodereader.GraphQuery.PathQuery\x1a\xf4\x04\n\x04Node\x12\x12\n\x05value\x18\x01 \x01(\tH\x01\x88\x01\x01\x12\x34\n\tnode_type\x18\x02 \x01(\x0e\x32\x1c.utils.RelationNode.NodeTypeH\x02\x88\x01\x01\x12\x19\n\x0cnode_subtype\x18\x03 \x01(\tH\x03\x88\x01\x01\x12=\n\nmatch_kind\x18\x04 \x01(\x0e\x32%.nodereader.GraphQuery.Node.MatchKindB\x02\x18\x01\x12\x37\n\x05\x65xact\x18\x05 \x01(\x0b\x32&.nodereader.GraphQuery.Node.ExactMatchH\x00\x12\x37\n\x05\x66uzzy\x18\x06 \x01(\x0b\x32&.nodereader.GraphQuery.Node.FuzzyMatchH\x00\x1a\x45\n\nExactMatch\x12\x37\n\x04kind\x18\x01 \x01(\x0e\x32).nodereader.GraphQuery.Node.MatchLocation\x1aW\n\nFuzzyMatch\x12\x37\n\x04kind\x18\x01 \x01(\x0e\x32).nodereader.GraphQuery.Node.MatchLocation\x12\x10\n\x08\x64istance\x18\x02 \x01(\r\"7\n\tMatchKind\x12\x14\n\x10\x44\x45PRECATED_EXACT\x10\x00\x12\x14\n\x10\x44\x45PRECATED_FUZZY\x10\x01\"B\n\rMatchLocation\x12\x08\n\x04\x46ULL\x10\x00\x12\n\n\x06PREFIX\x10\x01\x12\t\n\x05WORDS\x10\x02\x12\x10\n\x0cPREFIX_WORDS\x10\x03\x42\x10\n\x0enew_match_kindB\x08\n\x06_valueB\x0c\n\n_node_typeB\x0f\n\r_node_subtype\x1at\n\x08Relation\x12\x12\n\x05value\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x38\n\rrelation_type\x18\x02 \x01(\x0e\x32\x1c.utils.Relation.RelationTypeH\x01\x88\x01\x01\x42\x08\n\x06_valueB\x10\n\x0e_relation_type\x1a\xe3\x01\n\x04Path\x12\x30\n\x06source\x18\x01 \x01(\x0b\x32\x1b.nodereader.GraphQuery.NodeH\x00\x88\x01\x01\x12\x36\n\x08relation\x18\x02 \x01(\x0b\x32\x1f.nodereader.GraphQuery.RelationH\x01\x88\x01\x01\x12\x35\n\x0b\x64\x65stination\x18\x03 \x01(\x0b\x32\x1b.nodereader.GraphQuery.NodeH\x02\x88\x01\x01\x12\x12\n\nundirected\x18\x04 \x01(\x08\x42\t\n\x07_sourceB\x0b\n\t_relationB\x0e\n\x0c_destination\x1a?\n\tBoolQuery\x12\x32\n\x08operands\x18\x01 \x03(\x0b\x32 .nodereader.GraphQuery.PathQuery\x1a\x1c\n\x0b\x46\x61\x63\x65tFilter\x12\r\n\x05\x66\x61\x63\x65t\x18\x01 \x01(\t\x1a\x97\x02\n\tPathQuery\x12+\n\x04path\x18\x01 \x01(\x0b\x32\x1b.nodereader.GraphQuery.PathH\x00\x12\x34\n\x08\x62ool_not\x18\x02 \x01(\x0b\x32 .nodereader.GraphQuery.PathQueryH\x00\x12\x34\n\x08\x62ool_and\x18\x03 \x01(\x0b\x32 .nodereader.GraphQuery.BoolQueryH\x00\x12\x33\n\x07\x62ool_or\x18\x04 \x01(\x0b\x32 .nodereader.GraphQuery.BoolQueryH\x00\x12\x33\n\x05\x66\x61\x63\x65t\x18\x05 \x01(\x0b\x32\".nodereader.GraphQuery.FacetFilterH\x00\x42\x07\n\x05query\"\xc1\x02\n\x12GraphSearchRequest\x12\r\n\x05shard\x18\x01 \x01(\t\x12%\n\x05query\x18\x02 \x01(\x0b\x32\x16.nodereader.GraphQuery\x12\x36\n\x04kind\x18\x03 \x01(\x0e\x32(.nodereader.GraphSearchRequest.QueryKind\x12\r\n\x05top_k\x18\x04 \x01(\r\x12&\n\x08security\x18\x05 \x01(\x0b\x32\x0f.utils.SecurityH\x00\x88\x01\x01\x12\x37\n\x0c\x66ield_filter\x18\x06 \x01(\x0b\x32\x1c.nodereader.FilterExpressionH\x01\x88\x01\x01\"/\n\tQueryKind\x12\x08\n\x04PATH\x10\x00\x12\t\n\x05NODES\x10\x01\x12\r\n\tRELATIONS\x10\x02\x42\x0b\n\t_securityB\x0f\n\r_field_filter\"\xf7\x02\n\x13GraphSearchResponse\x12\"\n\x05nodes\x18\x01 \x03(\x0b\x32\x13.utils.RelationNode\x12;\n\trelations\x18\x02 \x03(\x0b\x32(.nodereader.GraphSearchResponse.Relation\x12\x33\n\x05graph\x18\x03 \x03(\x0b\x32$.nodereader.GraphSearchResponse.Path\x1aN\n\x08Relation\x12\x33\n\rrelation_type\x18\x01 \x01(\x0e\x32\x1c.utils.Relation.RelationType\x12\r\n\x05label\x18\x02 \x01(\t\x1az\n\x04Path\x12\x0e\n\x06source\x18\x01 \x01(\r\x12\x10\n\x08relation\x18\x02 \x01(\r\x12\x13\n\x0b\x64\x65stination\x18\x03 \x01(\r\x12.\n\x08metadata\x18\x04 \x01(\x0b\x32\x17.utils.RelationMetadataH\x00\x88\x01\x01\x42\x0b\n\t_metadata\"f\n\x15RelationSearchRequest\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\x35\n\x08subgraph\x18\x0c \x01(\x0b\x32#.nodereader.EntitiesSubgraphRequestJ\x04\x08\x0b\x10\x0c\"\x8a\x01\n\x16RelationSearchResponse\x12\x38\n\x06prefix\x18\x0b \x01(\x0b\x32(.nodereader.RelationPrefixSearchResponse\x12\x36\n\x08subgraph\x18\x0c \x01(\x0b\x32$.nodereader.EntitiesSubgraphResponse\"\xfa\x07\n\x10\x46ilterExpression\x12\x45\n\x08\x62ool_and\x18\x01 \x01(\x0b\x32\x31.nodereader.FilterExpression.FilterExpressionListH\x00\x12\x44\n\x07\x62ool_or\x18\x02 \x01(\x0b\x32\x31.nodereader.FilterExpression.FilterExpressionListH\x00\x12\x30\n\x08\x62ool_not\x18\x03 \x01(\x0b\x32\x1c.nodereader.FilterExpressionH\x00\x12?\n\x08resource\x18\x04 \x01(\x0b\x32+.nodereader.FilterExpression.ResourceFilterH\x00\x12\x39\n\x05\x66ield\x18\x05 \x01(\x0b\x32(.nodereader.FilterExpression.FieldFilterH\x00\x12=\n\x07keyword\x18\x06 \x01(\x0b\x32*.nodereader.FilterExpression.KeywordFilterH\x00\x12<\n\x04\x64\x61te\x18\x07 \x01(\x0b\x32,.nodereader.FilterExpression.DateRangeFilterH\x00\x12\x39\n\x05\x66\x61\x63\x65t\x18\x08 \x01(\x0b\x32(.nodereader.FilterExpression.FacetFilterH\x00\x1a\x46\n\x14\x46ilterExpressionList\x12.\n\x08operands\x18\x01 \x03(\x0b\x32\x1c.nodereader.FilterExpression\x1a%\n\x0eResourceFilter\x12\x13\n\x0bresource_id\x18\x01 \x01(\t\x1a\x45\n\x0b\x46ieldFilter\x12\x12\n\nfield_type\x18\x01 \x01(\t\x12\x15\n\x08\x66ield_id\x18\x02 \x01(\tH\x00\x88\x01\x01\x42\x0b\n\t_field_id\x1a\xf4\x01\n\x0f\x44\x61teRangeFilter\x12\x45\n\x05\x66ield\x18\x01 \x01(\x0e\x32\x36.nodereader.FilterExpression.DateRangeFilter.DateField\x12.\n\x05since\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x00\x88\x01\x01\x12.\n\x05until\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x01\x88\x01\x01\"&\n\tDateField\x12\x0b\n\x07\x43REATED\x10\x00\x12\x0c\n\x08MODIFIED\x10\x01\x42\x08\n\x06_sinceB\x08\n\x06_until\x1a \n\rKeywordFilter\x12\x0f\n\x07keyword\x18\x01 \x01(\t\x1a\x1c\n\x0b\x46\x61\x63\x65tFilter\x12\r\n\x05\x66\x61\x63\x65t\x18\x01 \x01(\tB\x06\n\x04\x65xpr\"\x86\x06\n\rSearchRequest\x12\r\n\x05shard\x18\x01 \x01(\t\x12\x0c\n\x04\x62ody\x18\x03 \x01(\t\x12\"\n\x05order\x18\x05 \x01(\x0b\x32\x13.nodereader.OrderBy\x12$\n\x07\x66\x61\x63\x65ted\x18\x06 \x01(\x0b\x32\x13.nodereader.Faceted\x12\x13\n\x0bpage_number\x18\x07 \x01(\x05\x12\x17\n\x0fresult_per_page\x18\x08 \x01(\x05\x12\x0e\n\x06vector\x18\n \x03(\x02\x12\x11\n\tvectorset\x18\x0f \x01(\t\x12\x11\n\tparagraph\x18\x0c \x01(\x08\x12\x10\n\x08\x64ocument\x18\r \x01(\x08\x12\x17\n\x0fwith_duplicates\x18\x0e \x01(\x08\x12\x14\n\x0conly_faceted\x18\x10 \x01(\x08\x12\x1b\n\x0e\x61\x64vanced_query\x18\x12 \x01(\tH\x00\x88\x01\x01\x12\x42\n\x11relation_subgraph\x18\x15 \x01(\x0b\x32#.nodereader.EntitiesSubgraphRequestB\x02\x18\x01\x12\x35\n\rgraph_request\x18\x1d \x01(\x0b\x32\x1e.nodereader.GraphSearchRequest\x12\x1a\n\x12min_score_semantic\x18\x17 \x01(\x02\x12\x16\n\x0emin_score_bm25\x18\x19 \x01(\x02\x12&\n\x08security\x18\x18 \x01(\x0b\x32\x0f.utils.SecurityH\x01\x88\x01\x01\x12\x37\n\x0c\x66ield_filter\x18\x1a \x01(\x0b\x32\x1c.nodereader.FilterExpressionH\x02\x88\x01\x01\x12;\n\x10paragraph_filter\x18\x1b \x01(\x0b\x32\x1c.nodereader.FilterExpressionH\x03\x88\x01\x01\x12\x33\n\x0f\x66ilter_operator\x18\x1c \x01(\x0e\x32\x1a.nodereader.FilterOperatorB\x11\n\x0f_advanced_queryB\x0b\n\t_securityB\x0f\n\r_field_filterB\x13\n\x11_paragraph_filterJ\x04\x08\x14\x10\x15\"\xad\x02\n\x0eSuggestRequest\x12\r\n\x05shard\x18\x01 \x01(\t\x12\x0c\n\x04\x62ody\x18\x02 \x01(\t\x12-\n\x08\x66\x65\x61tures\x18\x06 \x03(\x0e\x32\x1b.nodereader.SuggestFeatures\x12\x37\n\x0c\x66ield_filter\x18\x07 \x01(\x0b\x32\x1c.nodereader.FilterExpressionH\x00\x88\x01\x01\x12;\n\x10paragraph_filter\x18\x08 \x01(\x0b\x32\x1c.nodereader.FilterExpressionH\x01\x88\x01\x01\x12\x33\n\x0f\x66ilter_operator\x18\t \x01(\x0e\x32\x1a.nodereader.FilterOperatorB\x0f\n\r_field_filterB\x13\n\x11_paragraph_filter\"2\n\x0fRelatedEntities\x12\x10\n\x08\x65ntities\x18\x01 \x03(\t\x12\r\n\x05total\x18\x02 \x01(\r\"\xb1\x01\n\x0fSuggestResponse\x12\r\n\x05total\x18\x01 \x01(\x05\x12,\n\x07results\x18\x02 \x03(\x0b\x32\x1b.nodereader.ParagraphResult\x12\r\n\x05query\x18\x03 \x01(\t\x12\x10\n\x08\x65matches\x18\x04 \x03(\t\x12@\n\x0e\x65ntity_results\x18\x06 \x01(\x0b\x32(.nodereader.RelationPrefixSearchResponse\"\xe6\x01\n\x0eSearchResponse\x12\x34\n\x08\x64ocument\x18\x01 \x01(\x0b\x32\".nodereader.DocumentSearchResponse\x12\x36\n\tparagraph\x18\x02 \x01(\x0b\x32#.nodereader.ParagraphSearchResponse\x12\x30\n\x06vector\x18\x03 \x01(\x0b\x32 .nodereader.VectorSearchResponse\x12\x34\n\x08relation\x18\x04 \x01(\x0b\x32\".nodereader.RelationSearchResponse\"\x1b\n\x0cIdCollection\x12\x0b\n\x03ids\x18\x01 \x03(\t\"Q\n\x0cRelationEdge\x12/\n\tedge_type\x18\x01 \x01(\x0e\x32\x1c.utils.Relation.RelationType\x12\x10\n\x08property\x18\x02 \x01(\t\"2\n\x08\x45\x64geList\x12&\n\x04list\x18\x01 \x03(\x0b\x32\x18.nodereader.RelationEdge\"N\n\x0fGetShardRequest\x12(\n\x08shard_id\x18\x01 \x01(\x0b\x32\x16.noderesources.ShardId\x12\x11\n\tvectorset\x18\x02 \x01(\t\"+\n\rParagraphItem\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0e\n\x06labels\x18\x02 \x03(\t\";\n\x0c\x44ocumentItem\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12\r\n\x05\x66ield\x18\x02 \x01(\t\x12\x0e\n\x06labels\x18\x03 \x03(\t\"c\n\rStreamRequest\x12(\n\x08shard_id\x18\x03 \x01(\x0b\x32\x16.noderesources.ShardId\x12(\n\x06\x66ilter\x18\x04 \x01(\x0b\x32\x18.nodereader.StreamFilter\"(\n\x14GetShardFilesRequest\x12\x10\n\x08shard_id\x18\x01 \x01(\t\"5\n\rShardFileList\x12$\n\x05\x66iles\x18\x02 \x03(\x0b\x32\x15.nodereader.ShardFile\"0\n\tShardFile\x12\x15\n\rrelative_path\x18\x01 \x01(\t\x12\x0c\n\x04size\x18\x02 \x01(\x04\"C\n\x18\x44ownloadShardFileRequest\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\x15\n\rrelative_path\x18\x02 \x01(\t\"-\n\x0eShardFileChunk\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\x0c\x12\r\n\x05index\x18\x02 \x01(\x05*!\n\x0e\x46ilterOperator\x12\x07\n\x03\x41ND\x10\x00\x12\x06\n\x02OR\x10\x01*/\n\x0fSuggestFeatures\x12\x0c\n\x08\x45NTITIES\x10\x00\x12\x0e\n\nPARAGRAPHS\x10\x01\x32\xdf\x06\n\nNodeReader\x12?\n\x08GetShard\x12\x1b.nodereader.GetShardRequest\x1a\x14.noderesources.Shard\"\x00\x12\x41\n\x0b\x44ocumentIds\x12\x16.noderesources.ShardId\x1a\x18.nodereader.IdCollection\"\x00\x12\x42\n\x0cParagraphIds\x12\x16.noderesources.ShardId\x1a\x18.nodereader.IdCollection\"\x00\x12\x43\n\tVectorIds\x12\x1a.noderesources.VectorSetID\x1a\x18.nodereader.IdCollection\"\x00\x12\x41\n\x0bRelationIds\x12\x16.noderesources.ShardId\x1a\x18.nodereader.IdCollection\"\x00\x12?\n\rRelationEdges\x12\x16.noderesources.ShardId\x1a\x14.nodereader.EdgeList\"\x00\x12\x41\n\x06Search\x12\x19.nodereader.SearchRequest\x1a\x1a.nodereader.SearchResponse\"\x00\x12\x44\n\x07Suggest\x12\x1a.nodereader.SuggestRequest\x1a\x1b.nodereader.SuggestResponse\"\x00\x12\x46\n\nParagraphs\x12\x19.nodereader.StreamRequest\x1a\x19.nodereader.ParagraphItem\"\x00\x30\x01\x12\x44\n\tDocuments\x12\x19.nodereader.StreamRequest\x1a\x18.nodereader.DocumentItem\"\x00\x30\x01\x12N\n\rGetShardFiles\x12 .nodereader.GetShardFilesRequest\x1a\x19.nodereader.ShardFileList\"\x00\x12Y\n\x11\x44ownloadShardFile\x12$.nodereader.DownloadShardFileRequest\x1a\x1a.nodereader.ShardFileChunk\"\x00\x30\x01P\x00P\x02\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.nodereader_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_ORDERBY'].fields_by_name['field']._options = None
  _globals['_ORDERBY'].fields_by_name['field']._serialized_options = b'\030\001'
  _globals['_DOCUMENTSEARCHRESPONSE_FACETSENTRY']._options = None
  _globals['_DOCUMENTSEARCHRESPONSE_FACETSENTRY']._serialized_options = b'8\001'
  _globals['_PARAGRAPHSEARCHRESPONSE_FACETSENTRY']._options = None
  _globals['_PARAGRAPHSEARCHRESPONSE_FACETSENTRY']._serialized_options = b'8\001'
  _globals['_GRAPHQUERY_NODE'].fields_by_name['match_kind']._options = None
  _globals['_GRAPHQUERY_NODE'].fields_by_name['match_kind']._serialized_options = b'\030\001'
  _globals['_SEARCHREQUEST'].fields_by_name['relation_subgraph']._options = None
  _globals['_SEARCHREQUEST'].fields_by_name['relation_subgraph']._serialized_options = b'\030\001'
  _globals['_FILTEROPERATOR']._serialized_start=8460
  _globals['_FILTEROPERATOR']._serialized_end=8493
  _globals['_SUGGESTFEATURES']._serialized_start=8495
  _globals['_SUGGESTFEATURES']._serialized_end=8542
  _globals['_STREAMFILTER']._serialized_start=148
  _globals['_STREAMFILTER']._serialized_end=278
  _globals['_STREAMFILTER_CONJUNCTION']._serialized_start=239
  _globals['_STREAMFILTER_CONJUNCTION']._serialized_end=278
  _globals['_FACETED']._serialized_start=280
  _globals['_FACETED']._serialized_end=305
  _globals['_ORDERBY']._serialized_start=308
  _globals['_ORDERBY']._serialized_end=503
  _globals['_ORDERBY_ORDERTYPE']._serialized_start=432
  _globals['_ORDERBY_ORDERTYPE']._serialized_end=462
  _globals['_ORDERBY_ORDERFIELD']._serialized_start=464
  _globals['_ORDERBY_ORDERFIELD']._serialized_end=503
  _globals['_FACETRESULT']._serialized_start=505
  _globals['_FACETRESULT']._serialized_end=546
  _globals['_FACETRESULTS']._serialized_start=548
  _globals['_FACETRESULTS']._serialized_end=609
  _globals['_RESULTSCORE']._serialized_start=611
  _globals['_RESULTSCORE']._serialized_end=655
  _globals['_DOCUMENTRESULT']._serialized_start=657
  _globals['_DOCUMENTRESULT']._serialized_end=758
  _globals['_DOCUMENTSEARCHRESPONSE']._serialized_start=761
  _globals['_DOCUMENTSEARCHRESPONSE']._serialized_end=1076
  _globals['_DOCUMENTSEARCHRESPONSE_FACETSENTRY']._serialized_start=1005
  _globals['_DOCUMENTSEARCHRESPONSE_FACETSENTRY']._serialized_end=1076
  _globals['_PARAGRAPHRESULT']._serialized_start=1079
  _globals['_PARAGRAPHRESULT']._serialized_end=1327
  _globals['_PARAGRAPHSEARCHRESPONSE']._serialized_start=1330
  _globals['_PARAGRAPHSEARCHRESPONSE']._serialized_end=1690
  _globals['_PARAGRAPHSEARCHRESPONSE_FACETSENTRY']._serialized_start=1005
  _globals['_PARAGRAPHSEARCHRESPONSE_FACETSENTRY']._serialized_end=1076
  _globals['_DOCUMENTVECTORIDENTIFIER']._serialized_start=1692
  _globals['_DOCUMENTVECTORIDENTIFIER']._serialized_end=1730
  _globals['_DOCUMENTSCORED']._serialized_start=1733
  _globals['_DOCUMENTSCORED']._serialized_end=1885
  _globals['_VECTORSEARCHRESPONSE']._serialized_start=1887
  _globals['_VECTORSEARCHRESPONSE']._serialized_end=2002
  _globals['_RELATIONNODEFILTER']._serialized_start=2004
  _globals['_RELATIONNODEFILTER']._serialized_end=2117
  _globals['_RELATIONEDGEFILTER']._serialized_start=2120
  _globals['_RELATIONEDGEFILTER']._serialized_end=2269
  _globals['_RELATIONPREFIXSEARCHREQUEST']._serialized_start=2272
  _globals['_RELATIONPREFIXSEARCHREQUEST']._serialized_end=2400
  _globals['_RELATIONPREFIXSEARCHRESPONSE']._serialized_start=2402
  _globals['_RELATIONPREFIXSEARCHRESPONSE']._serialized_end=2468
  _globals['_ENTITIESSUBGRAPHREQUEST']._serialized_start=2471
  _globals['_ENTITIESSUBGRAPHREQUEST']._serialized_end=2734
  _globals['_ENTITIESSUBGRAPHREQUEST_DELETEDENTITIES']._serialized_start=2664
  _globals['_ENTITIESSUBGRAPHREQUEST_DELETEDENTITIES']._serialized_end=2724
  _globals['_ENTITIESSUBGRAPHRESPONSE']._serialized_start=2736
  _globals['_ENTITIESSUBGRAPHRESPONSE']._serialized_end=2811
  _globals['_GRAPHQUERY']._serialized_start=2814
  _globals['_GRAPHQUERY']._serialized_end=4230
  _globals['_GRAPHQUERY_NODE']._serialized_start=2877
  _globals['_GRAPHQUERY_NODE']._serialized_end=3505
  _globals['_GRAPHQUERY_NODE_EXACTMATCH']._serialized_start=3163
  _globals['_GRAPHQUERY_NODE_EXACTMATCH']._serialized_end=3232
  _globals['_GRAPHQUERY_NODE_FUZZYMATCH']._serialized_start=3234
  _globals['_GRAPHQUERY_NODE_FUZZYMATCH']._serialized_end=3321
  _globals['_GRAPHQUERY_NODE_MATCHKIND']._serialized_start=3323
  _globals['_GRAPHQUERY_NODE_MATCHKIND']._serialized_end=3378
  _globals['_GRAPHQUERY_NODE_MATCHLOCATION']._serialized_start=3380
  _globals['_GRAPHQUERY_NODE_MATCHLOCATION']._serialized_end=3446
  _globals['_GRAPHQUERY_RELATION']._serialized_start=3507
  _globals['_GRAPHQUERY_RELATION']._serialized_end=3623
  _globals['_GRAPHQUERY_PATH']._serialized_start=3626
  _globals['_GRAPHQUERY_PATH']._serialized_end=3853
  _globals['_GRAPHQUERY_BOOLQUERY']._serialized_start=3855
  _globals['_GRAPHQUERY_BOOLQUERY']._serialized_end=3918
  _globals['_GRAPHQUERY_FACETFILTER']._serialized_start=3920
  _globals['_GRAPHQUERY_FACETFILTER']._serialized_end=3948
  _globals['_GRAPHQUERY_PATHQUERY']._serialized_start=3951
  _globals['_GRAPHQUERY_PATHQUERY']._serialized_end=4230
  _globals['_GRAPHSEARCHREQUEST']._serialized_start=4233
  _globals['_GRAPHSEARCHREQUEST']._serialized_end=4554
  _globals['_GRAPHSEARCHREQUEST_QUERYKIND']._serialized_start=4477
  _globals['_GRAPHSEARCHREQUEST_QUERYKIND']._serialized_end=4524
  _globals['_GRAPHSEARCHRESPONSE']._serialized_start=4557
  _globals['_GRAPHSEARCHRESPONSE']._serialized_end=4932
  _globals['_GRAPHSEARCHRESPONSE_RELATION']._serialized_start=4730
  _globals['_GRAPHSEARCHRESPONSE_RELATION']._serialized_end=4808
  _globals['_GRAPHSEARCHRESPONSE_PATH']._serialized_start=4810
  _globals['_GRAPHSEARCHRESPONSE_PATH']._serialized_end=4932
  _globals['_RELATIONSEARCHREQUEST']._serialized_start=4934
  _globals['_RELATIONSEARCHREQUEST']._serialized_end=5036
  _globals['_RELATIONSEARCHRESPONSE']._serialized_start=5039
  _globals['_RELATIONSEARCHRESPONSE']._serialized_end=5177
  _globals['_FILTEREXPRESSION']._serialized_start=5180
  _globals['_FILTEREXPRESSION']._serialized_end=6198
  _globals['_FILTEREXPRESSION_FILTEREXPRESSIONLIST']._serialized_start=5699
  _globals['_FILTEREXPRESSION_FILTEREXPRESSIONLIST']._serialized_end=5769
  _globals['_FILTEREXPRESSION_RESOURCEFILTER']._serialized_start=5771
  _globals['_FILTEREXPRESSION_RESOURCEFILTER']._serialized_end=5808
  _globals['_FILTEREXPRESSION_FIELDFILTER']._serialized_start=5810
  _globals['_FILTEREXPRESSION_FIELDFILTER']._serialized_end=5879
  _globals['_FILTEREXPRESSION_DATERANGEFILTER']._serialized_start=5882
  _globals['_FILTEREXPRESSION_DATERANGEFILTER']._serialized_end=6126
  _globals['_FILTEREXPRESSION_DATERANGEFILTER_DATEFIELD']._serialized_start=6068
  _globals['_FILTEREXPRESSION_DATERANGEFILTER_DATEFIELD']._serialized_end=6106
  _globals['_FILTEREXPRESSION_KEYWORDFILTER']._serialized_start=6128
  _globals['_FILTEREXPRESSION_KEYWORDFILTER']._serialized_end=6160
  _globals['_FILTEREXPRESSION_FACETFILTER']._serialized_start=3920
  _globals['_FILTEREXPRESSION_FACETFILTER']._serialized_end=3948
  _globals['_SEARCHREQUEST']._serialized_start=6201
  _globals['_SEARCHREQUEST']._serialized_end=6975
  _globals['_SUGGESTREQUEST']._serialized_start=6978
  _globals['_SUGGESTREQUEST']._serialized_end=7279
  _globals['_RELATEDENTITIES']._serialized_start=7281
  _globals['_RELATEDENTITIES']._serialized_end=7331
  _globals['_SUGGESTRESPONSE']._serialized_start=7334
  _globals['_SUGGESTRESPONSE']._serialized_end=7511
  _globals['_SEARCHRESPONSE']._serialized_start=7514
  _globals['_SEARCHRESPONSE']._serialized_end=7744
  _globals['_IDCOLLECTION']._serialized_start=7746
  _globals['_IDCOLLECTION']._serialized_end=7773
  _globals['_RELATIONEDGE']._serialized_start=7775
  _globals['_RELATIONEDGE']._serialized_end=7856
  _globals['_EDGELIST']._serialized_start=7858
  _globals['_EDGELIST']._serialized_end=7908
  _globals['_GETSHARDREQUEST']._serialized_start=7910
  _globals['_GETSHARDREQUEST']._serialized_end=7988
  _globals['_PARAGRAPHITEM']._serialized_start=7990
  _globals['_PARAGRAPHITEM']._serialized_end=8033
  _globals['_DOCUMENTITEM']._serialized_start=8035
  _globals['_DOCUMENTITEM']._serialized_end=8094
  _globals['_STREAMREQUEST']._serialized_start=8096
  _globals['_STREAMREQUEST']._serialized_end=8195
  _globals['_GETSHARDFILESREQUEST']._serialized_start=8197
  _globals['_GETSHARDFILESREQUEST']._serialized_end=8237
  _globals['_SHARDFILELIST']._serialized_start=8239
  _globals['_SHARDFILELIST']._serialized_end=8292
  _globals['_SHARDFILE']._serialized_start=8294
  _globals['_SHARDFILE']._serialized_end=8342
  _globals['_DOWNLOADSHARDFILEREQUEST']._serialized_start=8344
  _globals['_DOWNLOADSHARDFILEREQUEST']._serialized_end=8411
  _globals['_SHARDFILECHUNK']._serialized_start=8413
  _globals['_SHARDFILECHUNK']._serialized_end=8458
  _globals['_NODEREADER']._serialized_start=8545
  _globals['_NODEREADER']._serialized_end=9408
# @@protoc_insertion_point(module_scope)
