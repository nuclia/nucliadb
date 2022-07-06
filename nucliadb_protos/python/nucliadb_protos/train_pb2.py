# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/train.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from nucliadb_protos import knowledgebox_pb2 as nucliadb__protos_dot_knowledgebox__pb2
from nucliadb_protos import resources_pb2 as nucliadb__protos_dot_resources__pb2
try:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_resources__pb2.nucliadb__protos_dot_utils__pb2
except AttributeError:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_resources__pb2.nucliadb_protos.utils_pb2

from nucliadb_protos.knowledgebox_pb2 import *
from nucliadb_protos.resources_pb2 import *

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1bnucliadb_protos/train.proto\x12\x05train\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\"nucliadb_protos/knowledgebox.proto\x1a\x1fnucliadb_protos/resources.proto\">\n\x12GetOntologyRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\"\xa2\x01\n\x08Ontology\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12$\n\x06labels\x18\x02 \x01(\x0b\x32\x14.knowledgebox.Labels\x12&\n\x06status\x18\x03 \x01(\x0e\x32\x16.train.Ontology.Status\"\x1e\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\">\n\x12GetEntitiesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\"\xf5\x01\n\x08\x45ntities\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12+\n\x06groups\x18\x02 \x03(\x0b\x32\x1b.train.Entities.GroupsEntry\x12&\n\x06status\x18\x03 \x01(\x0e\x32\x16.train.Entities.Status\x1aJ\n\x0bGroupsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12*\n\x05value\x18\x02 \x01(\x0b\x32\x1b.knowledgebox.EntitiesGroup:\x02\x38\x01\"\x1e\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08NOTFOUND\x10\x01\"Q\n\x0f\x45nabledMetadata\x12\x0c\n\x04text\x18\x01 \x01(\x08\x12\x10\n\x08\x65ntities\x18\x02 \x01(\x08\x12\x0e\n\x06labels\x18\x03 \x01(\x08\x12\x0e\n\x06vector\x18\x04 \x01(\x08\"\x8d\x01\n\x06Labels\x12+\n\x08resource\x18\x01 \x03(\x0b\x32\x19.resources.Classification\x12(\n\x05\x66ield\x18\x02 \x03(\x0b\x32\x19.resources.Classification\x12,\n\tparagraph\x18\x03 \x03(\x0b\x32\x19.resources.Classification\"\xa9\x01\n\x08Metadata\x12\x0c\n\x04text\x18\x01 \x01(\t\x12/\n\x08\x65ntities\x18\x02 \x03(\x0b\x32\x1d.train.Metadata.EntitiesEntry\x12\x1d\n\x06labels\x18\x03 \x01(\x0b\x32\r.train.Labels\x12\x0e\n\x06vector\x18\x04 \x03(\x02\x1a/\n\rEntitiesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"i\n\x13GetResourcesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12(\n\x08metadata\x18\x02 \x01(\x0b\x32\x16.train.EnabledMetadata\"\x9a\x01\n\x13GetParagraphRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldID\x12(\n\x08metadata\x18\x04 \x01(\x0b\x32\x16.train.EnabledMetadata\"\x99\x01\n\x12GetSentenceRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldID\x12(\n\x08metadata\x18\x04 \x01(\x0b\x32\x16.train.EnabledMetadata\"\x96\x01\n\x0fGetFieldRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldID\x12(\n\x08metadata\x18\x04 \x01(\x0b\x32\x16.train.EnabledMetadata\"\x83\x01\n\x08Sentence\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12!\n\x05\x66ield\x18\x02 \x01(\x0b\x32\x12.resources.FieldID\x12\x11\n\tparagraph\x18\x03 \x01(\t\x12\x10\n\x08sentence\x18\x04 \x01(\t\x12!\n\x08metadata\x18\x05 \x01(\x0b\x32\x0f.train.Metadata\"r\n\tParagraph\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12!\n\x05\x66ield\x18\x02 \x01(\x0b\x32\x12.resources.FieldID\x12\x11\n\tparagraph\x18\x03 \x01(\t\x12!\n\x08metadata\x18\x04 \x01(\x0b\x32\x0f.train.Metadata\"\\\n\x06\x46ields\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12!\n\x05\x66ield\x18\x02 \x01(\x0b\x32\x12.resources.FieldID\x12!\n\x08metadata\x18\x07 \x01(\x0b\x32\x0f.train.Metadata\"\xc1\x01\n\x08Resource\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12\r\n\x05title\x18\x02 \x01(\t\x12\x0c\n\x04icon\x18\x03 \x01(\t\x12\x0c\n\x04slug\x18\x04 \x01(\t\x12+\n\x07\x63reated\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12,\n\x08modified\x18\x06 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12!\n\x08metadata\x18\x07 \x01(\x0b\x32\x0f.train.Metadata2\xc5\x02\n\x05Train\x12>\n\x0cGetSentences\x12\x19.train.GetSentenceRequest\x1a\x0f.train.Sentence\"\x00\x30\x01\x12\x41\n\rGetParagraphs\x12\x1a.train.GetParagraphRequest\x1a\x10.train.Paragraph\"\x00\x30\x01\x12?\n\x0cGetResources\x12\x1a.train.GetResourcesRequest\x1a\x0f.train.Resource\"\x00\x30\x01\x12;\n\x0bGetOntology\x12\x19.train.GetOntologyRequest\x1a\x0f.train.Ontology\"\x00\x12;\n\x0bGetEntities\x12\x19.train.GetEntitiesRequest\x1a\x0f.train.Entities\"\x00P\x01P\x02\x62\x06proto3')



_GETONTOLOGYREQUEST = DESCRIPTOR.message_types_by_name['GetOntologyRequest']
_ONTOLOGY = DESCRIPTOR.message_types_by_name['Ontology']
_GETENTITIESREQUEST = DESCRIPTOR.message_types_by_name['GetEntitiesRequest']
_ENTITIES = DESCRIPTOR.message_types_by_name['Entities']
_ENTITIES_GROUPSENTRY = _ENTITIES.nested_types_by_name['GroupsEntry']
_ENABLEDMETADATA = DESCRIPTOR.message_types_by_name['EnabledMetadata']
_LABELS = DESCRIPTOR.message_types_by_name['Labels']
_METADATA = DESCRIPTOR.message_types_by_name['Metadata']
_METADATA_ENTITIESENTRY = _METADATA.nested_types_by_name['EntitiesEntry']
_GETRESOURCESREQUEST = DESCRIPTOR.message_types_by_name['GetResourcesRequest']
_GETPARAGRAPHREQUEST = DESCRIPTOR.message_types_by_name['GetParagraphRequest']
_GETSENTENCEREQUEST = DESCRIPTOR.message_types_by_name['GetSentenceRequest']
_GETFIELDREQUEST = DESCRIPTOR.message_types_by_name['GetFieldRequest']
_SENTENCE = DESCRIPTOR.message_types_by_name['Sentence']
_PARAGRAPH = DESCRIPTOR.message_types_by_name['Paragraph']
_FIELDS = DESCRIPTOR.message_types_by_name['Fields']
_RESOURCE = DESCRIPTOR.message_types_by_name['Resource']
_ONTOLOGY_STATUS = _ONTOLOGY.enum_types_by_name['Status']
_ENTITIES_STATUS = _ENTITIES.enum_types_by_name['Status']
GetOntologyRequest = _reflection.GeneratedProtocolMessageType('GetOntologyRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETONTOLOGYREQUEST,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.GetOntologyRequest)
  })
_sym_db.RegisterMessage(GetOntologyRequest)

Ontology = _reflection.GeneratedProtocolMessageType('Ontology', (_message.Message,), {
  'DESCRIPTOR' : _ONTOLOGY,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.Ontology)
  })
_sym_db.RegisterMessage(Ontology)

GetEntitiesRequest = _reflection.GeneratedProtocolMessageType('GetEntitiesRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETENTITIESREQUEST,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.GetEntitiesRequest)
  })
_sym_db.RegisterMessage(GetEntitiesRequest)

Entities = _reflection.GeneratedProtocolMessageType('Entities', (_message.Message,), {

  'GroupsEntry' : _reflection.GeneratedProtocolMessageType('GroupsEntry', (_message.Message,), {
    'DESCRIPTOR' : _ENTITIES_GROUPSENTRY,
    '__module__' : 'nucliadb_protos.train_pb2'
    # @@protoc_insertion_point(class_scope:train.Entities.GroupsEntry)
    })
  ,
  'DESCRIPTOR' : _ENTITIES,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.Entities)
  })
_sym_db.RegisterMessage(Entities)
_sym_db.RegisterMessage(Entities.GroupsEntry)

EnabledMetadata = _reflection.GeneratedProtocolMessageType('EnabledMetadata', (_message.Message,), {
  'DESCRIPTOR' : _ENABLEDMETADATA,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.EnabledMetadata)
  })
_sym_db.RegisterMessage(EnabledMetadata)

Labels = _reflection.GeneratedProtocolMessageType('Labels', (_message.Message,), {
  'DESCRIPTOR' : _LABELS,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.Labels)
  })
_sym_db.RegisterMessage(Labels)

Metadata = _reflection.GeneratedProtocolMessageType('Metadata', (_message.Message,), {

  'EntitiesEntry' : _reflection.GeneratedProtocolMessageType('EntitiesEntry', (_message.Message,), {
    'DESCRIPTOR' : _METADATA_ENTITIESENTRY,
    '__module__' : 'nucliadb_protos.train_pb2'
    # @@protoc_insertion_point(class_scope:train.Metadata.EntitiesEntry)
    })
  ,
  'DESCRIPTOR' : _METADATA,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.Metadata)
  })
_sym_db.RegisterMessage(Metadata)
_sym_db.RegisterMessage(Metadata.EntitiesEntry)

GetResourcesRequest = _reflection.GeneratedProtocolMessageType('GetResourcesRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETRESOURCESREQUEST,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.GetResourcesRequest)
  })
_sym_db.RegisterMessage(GetResourcesRequest)

GetParagraphRequest = _reflection.GeneratedProtocolMessageType('GetParagraphRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETPARAGRAPHREQUEST,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.GetParagraphRequest)
  })
_sym_db.RegisterMessage(GetParagraphRequest)

GetSentenceRequest = _reflection.GeneratedProtocolMessageType('GetSentenceRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETSENTENCEREQUEST,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.GetSentenceRequest)
  })
_sym_db.RegisterMessage(GetSentenceRequest)

GetFieldRequest = _reflection.GeneratedProtocolMessageType('GetFieldRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETFIELDREQUEST,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.GetFieldRequest)
  })
_sym_db.RegisterMessage(GetFieldRequest)

Sentence = _reflection.GeneratedProtocolMessageType('Sentence', (_message.Message,), {
  'DESCRIPTOR' : _SENTENCE,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.Sentence)
  })
_sym_db.RegisterMessage(Sentence)

Paragraph = _reflection.GeneratedProtocolMessageType('Paragraph', (_message.Message,), {
  'DESCRIPTOR' : _PARAGRAPH,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.Paragraph)
  })
_sym_db.RegisterMessage(Paragraph)

Fields = _reflection.GeneratedProtocolMessageType('Fields', (_message.Message,), {
  'DESCRIPTOR' : _FIELDS,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.Fields)
  })
_sym_db.RegisterMessage(Fields)

Resource = _reflection.GeneratedProtocolMessageType('Resource', (_message.Message,), {
  'DESCRIPTOR' : _RESOURCE,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.Resource)
  })
_sym_db.RegisterMessage(Resource)

_TRAIN = DESCRIPTOR.services_by_name['Train']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _ENTITIES_GROUPSENTRY._options = None
  _ENTITIES_GROUPSENTRY._serialized_options = b'8\001'
  _METADATA_ENTITIESENTRY._options = None
  _METADATA_ENTITIESENTRY._serialized_options = b'8\001'
  _GETONTOLOGYREQUEST._serialized_start=140
  _GETONTOLOGYREQUEST._serialized_end=202
  _ONTOLOGY._serialized_start=205
  _ONTOLOGY._serialized_end=367
  _ONTOLOGY_STATUS._serialized_start=337
  _ONTOLOGY_STATUS._serialized_end=367
  _GETENTITIESREQUEST._serialized_start=369
  _GETENTITIESREQUEST._serialized_end=431
  _ENTITIES._serialized_start=434
  _ENTITIES._serialized_end=679
  _ENTITIES_GROUPSENTRY._serialized_start=573
  _ENTITIES_GROUPSENTRY._serialized_end=647
  _ENTITIES_STATUS._serialized_start=337
  _ENTITIES_STATUS._serialized_end=367
  _ENABLEDMETADATA._serialized_start=681
  _ENABLEDMETADATA._serialized_end=762
  _LABELS._serialized_start=765
  _LABELS._serialized_end=906
  _METADATA._serialized_start=909
  _METADATA._serialized_end=1078
  _METADATA_ENTITIESENTRY._serialized_start=1031
  _METADATA_ENTITIESENTRY._serialized_end=1078
  _GETRESOURCESREQUEST._serialized_start=1080
  _GETRESOURCESREQUEST._serialized_end=1185
  _GETPARAGRAPHREQUEST._serialized_start=1188
  _GETPARAGRAPHREQUEST._serialized_end=1342
  _GETSENTENCEREQUEST._serialized_start=1345
  _GETSENTENCEREQUEST._serialized_end=1498
  _GETFIELDREQUEST._serialized_start=1501
  _GETFIELDREQUEST._serialized_end=1651
  _SENTENCE._serialized_start=1654
  _SENTENCE._serialized_end=1785
  _PARAGRAPH._serialized_start=1787
  _PARAGRAPH._serialized_end=1901
  _FIELDS._serialized_start=1903
  _FIELDS._serialized_end=1995
  _RESOURCE._serialized_start=1998
  _RESOURCE._serialized_end=2191
  _TRAIN._serialized_start=2194
  _TRAIN._serialized_end=2519
# @@protoc_insertion_point(module_scope)
