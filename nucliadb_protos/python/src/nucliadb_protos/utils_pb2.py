# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/utils.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1bnucliadb_protos/utils.proto\x12\x05utils\"\xad\x02\n\x08Relation\x12#\n\x06source\x18\x06 \x01(\x0b\x32\x13.utils.RelationNode\x12\x1f\n\x02to\x18\x07 \x01(\x0b\x32\x13.utils.RelationNode\x12.\n\x08relation\x18\x05 \x01(\x0e\x32\x1c.utils.Relation.RelationType\x12\x16\n\x0erelation_label\x18\x08 \x01(\t\x12)\n\x08metadata\x18\t \x01(\x0b\x32\x17.utils.RelationMetadata\x12\x13\n\x0bresource_id\x18\n \x01(\t\"S\n\x0cRelationType\x12\t\n\x05\x43HILD\x10\x00\x12\t\n\x05\x41\x42OUT\x10\x01\x12\n\n\x06\x45NTITY\x10\x02\x12\t\n\x05\x43OLAB\x10\x03\x12\x0b\n\x07SYNONYM\x10\x04\x12\t\n\x05OTHER\x10\x05\"\x9c\x02\n\x10RelationMetadata\x12\x19\n\x0cparagraph_id\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x19\n\x0csource_start\x18\x02 \x01(\x05H\x01\x88\x01\x01\x12\x17\n\nsource_end\x18\x03 \x01(\x05H\x02\x88\x01\x01\x12\x15\n\x08to_start\x18\x04 \x01(\x05H\x03\x88\x01\x01\x12\x13\n\x06to_end\x18\x05 \x01(\x05H\x04\x88\x01\x01\x12&\n\x19\x64\x61ta_augmentation_task_id\x18\x06 \x01(\tH\x05\x88\x01\x01\x42\x0f\n\r_paragraph_idB\x0f\n\r_source_startB\r\n\x0b_source_endB\x0b\n\t_to_startB\t\n\x07_to_endB\x1c\n\x1a_data_augmentation_task_id\"\x96\x01\n\x0cRelationNode\x12\r\n\x05value\x18\x04 \x01(\t\x12+\n\x05ntype\x18\x05 \x01(\x0e\x32\x1c.utils.RelationNode.NodeType\x12\x0f\n\x07subtype\x18\x06 \x01(\t\"9\n\x08NodeType\x12\n\n\x06\x45NTITY\x10\x00\x12\t\n\x05LABEL\x10\x01\x12\x0c\n\x08RESOURCE\x10\x02\x12\x08\n\x04USER\x10\x03\"\xa0\x01\n\rExtractedText\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x37\n\nsplit_text\x18\x02 \x03(\x0b\x32#.utils.ExtractedText.SplitTextEntry\x12\x16\n\x0e\x64\x65leted_splits\x18\x03 \x03(\t\x1a\x30\n\x0eSplitTextEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"d\n\x06Vector\x12\r\n\x05start\x18\x01 \x01(\x05\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x05\x12\x17\n\x0fstart_paragraph\x18\x03 \x01(\x05\x12\x15\n\rend_paragraph\x18\x04 \x01(\x05\x12\x0e\n\x06vector\x18\x05 \x03(\x02\")\n\x07Vectors\x12\x1e\n\x07vectors\x18\x01 \x03(\x0b\x32\r.utils.Vector\"\xca\x01\n\x0cVectorObject\x12\x1f\n\x07vectors\x18\x01 \x01(\x0b\x32\x0e.utils.Vectors\x12<\n\rsplit_vectors\x18\x02 \x03(\x0b\x32%.utils.VectorObject.SplitVectorsEntry\x12\x16\n\x0e\x64\x65leted_splits\x18\x03 \x03(\t\x1a\x43\n\x11SplitVectorsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x1d\n\x05value\x18\x02 \x01(\x0b\x32\x0e.utils.Vectors:\x02\x38\x01\"H\n\nUserVector\x12\x0e\n\x06vector\x18\x01 \x03(\x02\x12\x0e\n\x06labels\x18\x02 \x03(\t\x12\r\n\x05start\x18\x03 \x01(\x05\x12\x0b\n\x03\x65nd\x18\x04 \x01(\x05\"\x82\x01\n\x0bUserVectors\x12\x30\n\x07vectors\x18\x01 \x03(\x0b\x32\x1f.utils.UserVectors.VectorsEntry\x1a\x41\n\x0cVectorsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12 \n\x05value\x18\x02 \x01(\x0b\x32\x11.utils.UserVector:\x02\x38\x01\"\x87\x01\n\rUserVectorSet\x12\x32\n\x07vectors\x18\x01 \x03(\x0b\x32!.utils.UserVectorSet.VectorsEntry\x1a\x42\n\x0cVectorsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12!\n\x05value\x18\x02 \x01(\x0b\x32\x12.utils.UserVectors:\x02\x38\x01\"\"\n\x0fUserVectorsList\x12\x0f\n\x07vectors\x18\x01 \x03(\t\"!\n\x08Security\x12\x15\n\raccess_groups\x18\x01 \x03(\t*\'\n\x10VectorSimilarity\x12\n\n\x06\x43OSINE\x10\x00\x12\x07\n\x03\x44OT\x10\x01*.\n\x0eReleaseChannel\x12\n\n\x06STABLE\x10\x00\x12\x10\n\x0c\x45XPERIMENTAL\x10\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.utils_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_EXTRACTEDTEXT_SPLITTEXTENTRY']._options = None
  _globals['_EXTRACTEDTEXT_SPLITTEXTENTRY']._serialized_options = b'8\001'
  _globals['_VECTOROBJECT_SPLITVECTORSENTRY']._options = None
  _globals['_VECTOROBJECT_SPLITVECTORSENTRY']._serialized_options = b'8\001'
  _globals['_USERVECTORS_VECTORSENTRY']._options = None
  _globals['_USERVECTORS_VECTORSENTRY']._serialized_options = b'8\001'
  _globals['_USERVECTORSET_VECTORSENTRY']._options = None
  _globals['_USERVECTORSET_VECTORSENTRY']._serialized_options = b'8\001'
  _globals['_VECTORSIMILARITY']._serialized_start=1711
  _globals['_VECTORSIMILARITY']._serialized_end=1750
  _globals['_RELEASECHANNEL']._serialized_start=1752
  _globals['_RELEASECHANNEL']._serialized_end=1798
  _globals['_RELATION']._serialized_start=39
  _globals['_RELATION']._serialized_end=340
  _globals['_RELATION_RELATIONTYPE']._serialized_start=257
  _globals['_RELATION_RELATIONTYPE']._serialized_end=340
  _globals['_RELATIONMETADATA']._serialized_start=343
  _globals['_RELATIONMETADATA']._serialized_end=627
  _globals['_RELATIONNODE']._serialized_start=630
  _globals['_RELATIONNODE']._serialized_end=780
  _globals['_RELATIONNODE_NODETYPE']._serialized_start=723
  _globals['_RELATIONNODE_NODETYPE']._serialized_end=780
  _globals['_EXTRACTEDTEXT']._serialized_start=783
  _globals['_EXTRACTEDTEXT']._serialized_end=943
  _globals['_EXTRACTEDTEXT_SPLITTEXTENTRY']._serialized_start=895
  _globals['_EXTRACTEDTEXT_SPLITTEXTENTRY']._serialized_end=943
  _globals['_VECTOR']._serialized_start=945
  _globals['_VECTOR']._serialized_end=1045
  _globals['_VECTORS']._serialized_start=1047
  _globals['_VECTORS']._serialized_end=1088
  _globals['_VECTOROBJECT']._serialized_start=1091
  _globals['_VECTOROBJECT']._serialized_end=1293
  _globals['_VECTOROBJECT_SPLITVECTORSENTRY']._serialized_start=1226
  _globals['_VECTOROBJECT_SPLITVECTORSENTRY']._serialized_end=1293
  _globals['_USERVECTOR']._serialized_start=1295
  _globals['_USERVECTOR']._serialized_end=1367
  _globals['_USERVECTORS']._serialized_start=1370
  _globals['_USERVECTORS']._serialized_end=1500
  _globals['_USERVECTORS_VECTORSENTRY']._serialized_start=1435
  _globals['_USERVECTORS_VECTORSENTRY']._serialized_end=1500
  _globals['_USERVECTORSET']._serialized_start=1503
  _globals['_USERVECTORSET']._serialized_end=1638
  _globals['_USERVECTORSET_VECTORSENTRY']._serialized_start=1572
  _globals['_USERVECTORSET_VECTORSENTRY']._serialized_end=1638
  _globals['_USERVECTORSLIST']._serialized_start=1640
  _globals['_USERVECTORSLIST']._serialized_end=1674
  _globals['_SECURITY']._serialized_start=1676
  _globals['_SECURITY']._serialized_end=1709
# @@protoc_insertion_point(module_scope)
