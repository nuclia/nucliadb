# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/utils.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1bnucliadb_protos/utils.proto\x12\x05utils\"\x98\x02\n\x08Relation\x12#\n\x06source\x18\x06 \x01(\x0b\x32\x13.utils.RelationNode\x12\x1f\n\x02to\x18\x07 \x01(\x0b\x32\x13.utils.RelationNode\x12.\n\x08relation\x18\x05 \x01(\x0e\x32\x1c.utils.Relation.RelationType\x12\x16\n\x0erelation_label\x18\x08 \x01(\t\x12)\n\x08metadata\x18\t \x01(\x0b\x32\x17.utils.RelationMetadata\"S\n\x0cRelationType\x12\t\n\x05\x43HILD\x10\x00\x12\t\n\x05\x41\x42OUT\x10\x01\x12\n\n\x06\x45NTITY\x10\x02\x12\t\n\x05\x43OLAB\x10\x03\x12\x0b\n\x07SYNONYM\x10\x04\x12\t\n\x05OTHER\x10\x05\"\xd6\x01\n\x10RelationMetadata\x12\x19\n\x0cparagraph_id\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x19\n\x0csource_start\x18\x02 \x01(\x05H\x01\x88\x01\x01\x12\x17\n\nsource_end\x18\x03 \x01(\x05H\x02\x88\x01\x01\x12\x15\n\x08to_start\x18\x04 \x01(\x05H\x03\x88\x01\x01\x12\x13\n\x06to_end\x18\x05 \x01(\x05H\x04\x88\x01\x01\x42\x0f\n\r_paragraph_idB\x0f\n\r_source_startB\r\n\x0b_source_endB\x0b\n\t_to_startB\t\n\x07_to_end\"\x96\x01\n\x0cRelationNode\x12\r\n\x05value\x18\x04 \x01(\t\x12+\n\x05ntype\x18\x05 \x01(\x0e\x32\x1c.utils.RelationNode.NodeType\x12\x0f\n\x07subtype\x18\x06 \x01(\t\"9\n\x08NodeType\x12\n\n\x06\x45NTITY\x10\x00\x12\t\n\x05LABEL\x10\x01\x12\x0c\n\x08RESOURCE\x10\x02\x12\x08\n\x04USER\x10\x03\"m\n\x0cJoinGraphCnx\x12\x0e\n\x06source\x18\x04 \x01(\x05\x12\x0e\n\x06target\x18\x01 \x01(\x05\x12+\n\x05rtype\x18\x02 \x01(\x0e\x32\x1c.utils.Relation.RelationType\x12\x10\n\x08rsubtype\x18\x03 \x01(\t\"\x9e\x01\n\tJoinGraph\x12*\n\x05nodes\x18\x01 \x03(\x0b\x32\x1b.utils.JoinGraph.NodesEntry\x12\"\n\x05\x65\x64ges\x18\x02 \x03(\x0b\x32\x13.utils.JoinGraphCnx\x1a\x41\n\nNodesEntry\x12\x0b\n\x03key\x18\x01 \x01(\x05\x12\"\n\x05value\x18\x02 \x01(\x0b\x32\x13.utils.RelationNode:\x02\x38\x01\"\xa0\x01\n\rExtractedText\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x37\n\nsplit_text\x18\x02 \x03(\x0b\x32#.utils.ExtractedText.SplitTextEntry\x12\x16\n\x0e\x64\x65leted_splits\x18\x03 \x03(\t\x1a\x30\n\x0eSplitTextEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"d\n\x06Vector\x12\r\n\x05start\x18\x01 \x01(\x05\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x05\x12\x17\n\x0fstart_paragraph\x18\x03 \x01(\x05\x12\x15\n\rend_paragraph\x18\x04 \x01(\x05\x12\x0e\n\x06vector\x18\x05 \x03(\x02\")\n\x07Vectors\x12\x1e\n\x07vectors\x18\x01 \x03(\x0b\x32\r.utils.Vector\"\xca\x01\n\x0cVectorObject\x12\x1f\n\x07vectors\x18\x01 \x01(\x0b\x32\x0e.utils.Vectors\x12<\n\rsplit_vectors\x18\x02 \x03(\x0b\x32%.utils.VectorObject.SplitVectorsEntry\x12\x16\n\x0e\x64\x65leted_splits\x18\x03 \x03(\t\x1a\x43\n\x11SplitVectorsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x1d\n\x05value\x18\x02 \x01(\x0b\x32\x0e.utils.Vectors:\x02\x38\x01\"H\n\nUserVector\x12\x0e\n\x06vector\x18\x01 \x03(\x02\x12\x0e\n\x06labels\x18\x02 \x03(\t\x12\r\n\x05start\x18\x03 \x01(\x05\x12\x0b\n\x03\x65nd\x18\x04 \x01(\x05\"\x82\x01\n\x0bUserVectors\x12\x30\n\x07vectors\x18\x01 \x03(\x0b\x32\x1f.utils.UserVectors.VectorsEntry\x1a\x41\n\x0cVectorsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12 \n\x05value\x18\x02 \x01(\x0b\x32\x11.utils.UserVector:\x02\x38\x01\"\x87\x01\n\rUserVectorSet\x12\x32\n\x07vectors\x18\x01 \x03(\x0b\x32!.utils.UserVectorSet.VectorsEntry\x1a\x42\n\x0cVectorsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12!\n\x05value\x18\x02 \x01(\x0b\x32\x12.utils.UserVectors:\x02\x38\x01\"\"\n\x0fUserVectorsList\x12\x0f\n\x07vectors\x18\x01 \x03(\tb\x06proto3')



_RELATION = DESCRIPTOR.message_types_by_name['Relation']
_RELATIONMETADATA = DESCRIPTOR.message_types_by_name['RelationMetadata']
_RELATIONNODE = DESCRIPTOR.message_types_by_name['RelationNode']
_JOINGRAPHCNX = DESCRIPTOR.message_types_by_name['JoinGraphCnx']
_JOINGRAPH = DESCRIPTOR.message_types_by_name['JoinGraph']
_JOINGRAPH_NODESENTRY = _JOINGRAPH.nested_types_by_name['NodesEntry']
_EXTRACTEDTEXT = DESCRIPTOR.message_types_by_name['ExtractedText']
_EXTRACTEDTEXT_SPLITTEXTENTRY = _EXTRACTEDTEXT.nested_types_by_name['SplitTextEntry']
_VECTOR = DESCRIPTOR.message_types_by_name['Vector']
_VECTORS = DESCRIPTOR.message_types_by_name['Vectors']
_VECTOROBJECT = DESCRIPTOR.message_types_by_name['VectorObject']
_VECTOROBJECT_SPLITVECTORSENTRY = _VECTOROBJECT.nested_types_by_name['SplitVectorsEntry']
_USERVECTOR = DESCRIPTOR.message_types_by_name['UserVector']
_USERVECTORS = DESCRIPTOR.message_types_by_name['UserVectors']
_USERVECTORS_VECTORSENTRY = _USERVECTORS.nested_types_by_name['VectorsEntry']
_USERVECTORSET = DESCRIPTOR.message_types_by_name['UserVectorSet']
_USERVECTORSET_VECTORSENTRY = _USERVECTORSET.nested_types_by_name['VectorsEntry']
_USERVECTORSLIST = DESCRIPTOR.message_types_by_name['UserVectorsList']
_RELATION_RELATIONTYPE = _RELATION.enum_types_by_name['RelationType']
_RELATIONNODE_NODETYPE = _RELATIONNODE.enum_types_by_name['NodeType']
Relation = _reflection.GeneratedProtocolMessageType('Relation', (_message.Message,), {
  'DESCRIPTOR' : _RELATION,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.Relation)
  })
_sym_db.RegisterMessage(Relation)

RelationMetadata = _reflection.GeneratedProtocolMessageType('RelationMetadata', (_message.Message,), {
  'DESCRIPTOR' : _RELATIONMETADATA,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.RelationMetadata)
  })
_sym_db.RegisterMessage(RelationMetadata)

RelationNode = _reflection.GeneratedProtocolMessageType('RelationNode', (_message.Message,), {
  'DESCRIPTOR' : _RELATIONNODE,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.RelationNode)
  })
_sym_db.RegisterMessage(RelationNode)

JoinGraphCnx = _reflection.GeneratedProtocolMessageType('JoinGraphCnx', (_message.Message,), {
  'DESCRIPTOR' : _JOINGRAPHCNX,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.JoinGraphCnx)
  })
_sym_db.RegisterMessage(JoinGraphCnx)

JoinGraph = _reflection.GeneratedProtocolMessageType('JoinGraph', (_message.Message,), {

  'NodesEntry' : _reflection.GeneratedProtocolMessageType('NodesEntry', (_message.Message,), {
    'DESCRIPTOR' : _JOINGRAPH_NODESENTRY,
    '__module__' : 'nucliadb_protos.utils_pb2'
    # @@protoc_insertion_point(class_scope:utils.JoinGraph.NodesEntry)
    })
  ,
  'DESCRIPTOR' : _JOINGRAPH,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.JoinGraph)
  })
_sym_db.RegisterMessage(JoinGraph)
_sym_db.RegisterMessage(JoinGraph.NodesEntry)

ExtractedText = _reflection.GeneratedProtocolMessageType('ExtractedText', (_message.Message,), {

  'SplitTextEntry' : _reflection.GeneratedProtocolMessageType('SplitTextEntry', (_message.Message,), {
    'DESCRIPTOR' : _EXTRACTEDTEXT_SPLITTEXTENTRY,
    '__module__' : 'nucliadb_protos.utils_pb2'
    # @@protoc_insertion_point(class_scope:utils.ExtractedText.SplitTextEntry)
    })
  ,
  'DESCRIPTOR' : _EXTRACTEDTEXT,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.ExtractedText)
  })
_sym_db.RegisterMessage(ExtractedText)
_sym_db.RegisterMessage(ExtractedText.SplitTextEntry)

Vector = _reflection.GeneratedProtocolMessageType('Vector', (_message.Message,), {
  'DESCRIPTOR' : _VECTOR,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.Vector)
  })
_sym_db.RegisterMessage(Vector)

Vectors = _reflection.GeneratedProtocolMessageType('Vectors', (_message.Message,), {
  'DESCRIPTOR' : _VECTORS,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.Vectors)
  })
_sym_db.RegisterMessage(Vectors)

VectorObject = _reflection.GeneratedProtocolMessageType('VectorObject', (_message.Message,), {

  'SplitVectorsEntry' : _reflection.GeneratedProtocolMessageType('SplitVectorsEntry', (_message.Message,), {
    'DESCRIPTOR' : _VECTOROBJECT_SPLITVECTORSENTRY,
    '__module__' : 'nucliadb_protos.utils_pb2'
    # @@protoc_insertion_point(class_scope:utils.VectorObject.SplitVectorsEntry)
    })
  ,
  'DESCRIPTOR' : _VECTOROBJECT,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.VectorObject)
  })
_sym_db.RegisterMessage(VectorObject)
_sym_db.RegisterMessage(VectorObject.SplitVectorsEntry)

UserVector = _reflection.GeneratedProtocolMessageType('UserVector', (_message.Message,), {
  'DESCRIPTOR' : _USERVECTOR,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.UserVector)
  })
_sym_db.RegisterMessage(UserVector)

UserVectors = _reflection.GeneratedProtocolMessageType('UserVectors', (_message.Message,), {

  'VectorsEntry' : _reflection.GeneratedProtocolMessageType('VectorsEntry', (_message.Message,), {
    'DESCRIPTOR' : _USERVECTORS_VECTORSENTRY,
    '__module__' : 'nucliadb_protos.utils_pb2'
    # @@protoc_insertion_point(class_scope:utils.UserVectors.VectorsEntry)
    })
  ,
  'DESCRIPTOR' : _USERVECTORS,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.UserVectors)
  })
_sym_db.RegisterMessage(UserVectors)
_sym_db.RegisterMessage(UserVectors.VectorsEntry)

UserVectorSet = _reflection.GeneratedProtocolMessageType('UserVectorSet', (_message.Message,), {

  'VectorsEntry' : _reflection.GeneratedProtocolMessageType('VectorsEntry', (_message.Message,), {
    'DESCRIPTOR' : _USERVECTORSET_VECTORSENTRY,
    '__module__' : 'nucliadb_protos.utils_pb2'
    # @@protoc_insertion_point(class_scope:utils.UserVectorSet.VectorsEntry)
    })
  ,
  'DESCRIPTOR' : _USERVECTORSET,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.UserVectorSet)
  })
_sym_db.RegisterMessage(UserVectorSet)
_sym_db.RegisterMessage(UserVectorSet.VectorsEntry)

UserVectorsList = _reflection.GeneratedProtocolMessageType('UserVectorsList', (_message.Message,), {
  'DESCRIPTOR' : _USERVECTORSLIST,
  '__module__' : 'nucliadb_protos.utils_pb2'
  # @@protoc_insertion_point(class_scope:utils.UserVectorsList)
  })
_sym_db.RegisterMessage(UserVectorsList)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _JOINGRAPH_NODESENTRY._options = None
  _JOINGRAPH_NODESENTRY._serialized_options = b'8\001'
  _EXTRACTEDTEXT_SPLITTEXTENTRY._options = None
  _EXTRACTEDTEXT_SPLITTEXTENTRY._serialized_options = b'8\001'
  _VECTOROBJECT_SPLITVECTORSENTRY._options = None
  _VECTOROBJECT_SPLITVECTORSENTRY._serialized_options = b'8\001'
  _USERVECTORS_VECTORSENTRY._options = None
  _USERVECTORS_VECTORSENTRY._serialized_options = b'8\001'
  _USERVECTORSET_VECTORSENTRY._options = None
  _USERVECTORSET_VECTORSENTRY._serialized_options = b'8\001'
  _RELATION._serialized_start=39
  _RELATION._serialized_end=319
  _RELATION_RELATIONTYPE._serialized_start=236
  _RELATION_RELATIONTYPE._serialized_end=319
  _RELATIONMETADATA._serialized_start=322
  _RELATIONMETADATA._serialized_end=536
  _RELATIONNODE._serialized_start=539
  _RELATIONNODE._serialized_end=689
  _RELATIONNODE_NODETYPE._serialized_start=632
  _RELATIONNODE_NODETYPE._serialized_end=689
  _JOINGRAPHCNX._serialized_start=691
  _JOINGRAPHCNX._serialized_end=800
  _JOINGRAPH._serialized_start=803
  _JOINGRAPH._serialized_end=961
  _JOINGRAPH_NODESENTRY._serialized_start=896
  _JOINGRAPH_NODESENTRY._serialized_end=961
  _EXTRACTEDTEXT._serialized_start=964
  _EXTRACTEDTEXT._serialized_end=1124
  _EXTRACTEDTEXT_SPLITTEXTENTRY._serialized_start=1076
  _EXTRACTEDTEXT_SPLITTEXTENTRY._serialized_end=1124
  _VECTOR._serialized_start=1126
  _VECTOR._serialized_end=1226
  _VECTORS._serialized_start=1228
  _VECTORS._serialized_end=1269
  _VECTOROBJECT._serialized_start=1272
  _VECTOROBJECT._serialized_end=1474
  _VECTOROBJECT_SPLITVECTORSENTRY._serialized_start=1407
  _VECTOROBJECT_SPLITVECTORSENTRY._serialized_end=1474
  _USERVECTOR._serialized_start=1476
  _USERVECTOR._serialized_end=1548
  _USERVECTORS._serialized_start=1551
  _USERVECTORS._serialized_end=1681
  _USERVECTORS_VECTORSENTRY._serialized_start=1616
  _USERVECTORS_VECTORSENTRY._serialized_end=1681
  _USERVECTORSET._serialized_start=1684
  _USERVECTORSET._serialized_end=1819
  _USERVECTORSET_VECTORSENTRY._serialized_start=1753
  _USERVECTORSET_VECTORSENTRY._serialized_end=1819
  _USERVECTORSLIST._serialized_start=1821
  _USERVECTORSLIST._serialized_end=1855
# @@protoc_insertion_point(module_scope)
