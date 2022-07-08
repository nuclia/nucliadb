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
from nucliadb_protos import writer_pb2 as nucliadb__protos_dot_writer__pb2
try:
  nucliadb__protos_dot_noderesources__pb2 = nucliadb__protos_dot_writer__pb2.nucliadb__protos_dot_noderesources__pb2
except AttributeError:
  nucliadb__protos_dot_noderesources__pb2 = nucliadb__protos_dot_writer__pb2.nucliadb_protos.noderesources_pb2
try:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_writer__pb2.nucliadb__protos_dot_utils__pb2
except AttributeError:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_writer__pb2.nucliadb_protos.utils_pb2
try:
  nucliadb__protos_dot_resources__pb2 = nucliadb__protos_dot_writer__pb2.nucliadb__protos_dot_resources__pb2
except AttributeError:
  nucliadb__protos_dot_resources__pb2 = nucliadb__protos_dot_writer__pb2.nucliadb_protos.resources_pb2
try:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_writer__pb2.nucliadb__protos_dot_utils__pb2
except AttributeError:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_writer__pb2.nucliadb_protos.utils_pb2
try:
  nucliadb__protos_dot_knowledgebox__pb2 = nucliadb__protos_dot_writer__pb2.nucliadb__protos_dot_knowledgebox__pb2
except AttributeError:
  nucliadb__protos_dot_knowledgebox__pb2 = nucliadb__protos_dot_writer__pb2.nucliadb_protos.knowledgebox_pb2

from nucliadb_protos.knowledgebox_pb2 import *
from nucliadb_protos.resources_pb2 import *
from nucliadb_protos.writer_pb2 import *

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1bnucliadb_protos/train.proto\x12\x05train\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\"nucliadb_protos/knowledgebox.proto\x1a\x1fnucliadb_protos/resources.proto\x1a\x1cnucliadb_protos/writer.proto\"Q\n\x0f\x45nabledMetadata\x12\x0c\n\x04text\x18\x01 \x01(\x08\x12\x10\n\x08\x65ntities\x18\x02 \x01(\x08\x12\x0e\n\x06labels\x18\x03 \x01(\x08\x12\x0e\n\x06vector\x18\x04 \x01(\x08\"\x92\x01\n\x0bTrainLabels\x12+\n\x08resource\x18\x01 \x03(\x0b\x32\x19.resources.Classification\x12(\n\x05\x66ield\x18\x02 \x03(\x0b\x32\x19.resources.Classification\x12,\n\tparagraph\x18\x03 \x03(\x0b\x32\x19.resources.Classification\"\xb8\x01\n\rTrainMetadata\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x34\n\x08\x65ntities\x18\x02 \x03(\x0b\x32\".train.TrainMetadata.EntitiesEntry\x12\"\n\x06labels\x18\x03 \x01(\x0b\x32\x12.train.TrainLabels\x12\x0e\n\x06vector\x18\x04 \x03(\x02\x1a/\n\rEntitiesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"i\n\x13GetResourcesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12(\n\x08metadata\x18\x02 \x01(\x0b\x32\x16.train.EnabledMetadata\"\x9b\x01\n\x14GetParagraphsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldID\x12(\n\x08metadata\x18\x04 \x01(\x0b\x32\x16.train.EnabledMetadata\"\x9a\x01\n\x13GetSentencesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldID\x12(\n\x08metadata\x18\x04 \x01(\x0b\x32\x16.train.EnabledMetadata\"\x97\x01\n\x10GetFieldsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldID\x12(\n\x08metadata\x18\x04 \x01(\x0b\x32\x16.train.EnabledMetadata\"\x8d\x01\n\rTrainSentence\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12!\n\x05\x66ield\x18\x02 \x01(\x0b\x32\x12.resources.FieldID\x12\x11\n\tparagraph\x18\x03 \x01(\t\x12\x10\n\x08sentence\x18\x04 \x01(\t\x12&\n\x08metadata\x18\x05 \x01(\x0b\x32\x14.train.TrainMetadata\"|\n\x0eTrainParagraph\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12!\n\x05\x66ield\x18\x02 \x01(\x0b\x32\x12.resources.FieldID\x12\x11\n\tparagraph\x18\x03 \x01(\t\x12&\n\x08metadata\x18\x04 \x01(\x0b\x32\x14.train.TrainMetadata\"w\n\nTrainField\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12!\n\x05\x66ield\x18\x02 \x01(\x0b\x32\x12.resources.FieldID\x12\x10\n\x08subfield\x18\x03 \x01(\t\x12&\n\x08metadata\x18\x04 \x01(\x0b\x32\x14.train.TrainMetadata\"\xcb\x01\n\rTrainResource\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12\r\n\x05title\x18\x02 \x01(\t\x12\x0c\n\x04icon\x18\x03 \x01(\t\x12\x0c\n\x04slug\x18\x04 \x01(\t\x12+\n\x07\x63reated\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12,\n\x08modified\x18\x06 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12&\n\x08metadata\x18\x07 \x01(\x0b\x32\x14.train.TrainMetadata\"\x1e\n\x0fRegisterRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\"\"\n\x10RegisterResponse\x12\x0e\n\x06status\x18\x01 \x01(\x08\"\x0e\n\x0c\x43loseRequest\"\xa3\x03\n\x0fMessageToServer\x12\x32\n\x10register_request\x18\x01 \x01(\x0b\x32\x16.train.RegisterRequestH\x00\x12,\n\rclose_request\x18\x02 \x01(\x0b\x32\x13.train.CloseRequestH\x00\x12(\n\x08resource\x18\x03 \x01(\x0b\x32\x14.train.TrainResourceH\x00\x12\"\n\x05\x66ield\x18\x04 \x01(\x0b\x32\x11.train.TrainFieldH\x00\x12*\n\tparagraph\x18\x05 \x01(\x0b\x32\x15.train.TrainParagraphH\x00\x12(\n\x08sentence\x18\x06 \x01(\x0b\x32\x14.train.TrainSentenceH\x00\x12\x17\n\rend_of_stream\x18\x07 \x01(\x08H\x00\x12.\n\x06labels\x18\x08 \x01(\x0b\x32\x1c.fdbwriter.GetLabelsResponseH\x00\x12\x32\n\x08\x65ntities\x18\t \x01(\x0b\x32\x1e.fdbwriter.GetEntitiesResponseH\x00\x42\r\n\x0bMessageBody\"\xf0\x03\n\x11MessageFromServer\x12\x34\n\x11register_response\x18\x01 \x01(\x0b\x32\x17.train.RegisterResponseH\x00\x12;\n\x15get_sentences_request\x18\x02 \x01(\x0b\x32\x1a.train.GetSentencesRequestH\x00\x12=\n\x16get_paragraphs_request\x18\x03 \x01(\x0b\x32\x1b.train.GetParagraphsRequestH\x00\x12\x35\n\x12get_fields_request\x18\x04 \x01(\x0b\x32\x17.train.GetFieldsRequestH\x00\x12;\n\x15get_resources_request\x18\x05 \x01(\x0b\x32\x1a.train.GetResourcesRequestH\x00\x12\x39\n\x12get_labels_request\x18\x06 \x01(\x0b\x32\x1b.fdbwriter.GetLabelsRequestH\x00\x12=\n\x14get_entities_request\x18\x07 \x01(\x0b\x32\x1d.fdbwriter.GetEntitiesRequestH\x00\x12,\n\rclose_request\x18\x08 \x01(\x0b\x32\x13.train.CloseRequestH\x00\x42\r\n\x0bMessageBody2\xb5\x03\n\x05Train\x12\x44\n\x0cGetSentences\x12\x1a.train.GetSentencesRequest\x1a\x14.train.TrainSentence\"\x00\x30\x01\x12G\n\rGetParagraphs\x12\x1b.train.GetParagraphsRequest\x1a\x15.train.TrainParagraph\"\x00\x30\x01\x12;\n\tGetFields\x12\x17.train.GetFieldsRequest\x1a\x11.train.TrainField\"\x00\x30\x01\x12\x44\n\x0cGetResources\x12\x1a.train.GetResourcesRequest\x1a\x14.train.TrainResource\"\x00\x30\x01\x12J\n\x0bGetOntology\x12\x1b.fdbwriter.GetLabelsRequest\x1a\x1c.fdbwriter.GetLabelsResponse\"\x00\x12N\n\x0bGetEntities\x12\x1d.fdbwriter.GetEntitiesRequest\x1a\x1e.fdbwriter.GetEntitiesResponse\"\x00\x32H\n\x06Tunnel\x12>\n\x06Tunnel\x12\x16.train.MessageToServer\x1a\x18.train.MessageFromServer(\x01\x30\x01P\x01P\x02P\x03\x62\x06proto3')



_ENABLEDMETADATA = DESCRIPTOR.message_types_by_name['EnabledMetadata']
_TRAINLABELS = DESCRIPTOR.message_types_by_name['TrainLabels']
_TRAINMETADATA = DESCRIPTOR.message_types_by_name['TrainMetadata']
_TRAINMETADATA_ENTITIESENTRY = _TRAINMETADATA.nested_types_by_name['EntitiesEntry']
_GETRESOURCESREQUEST = DESCRIPTOR.message_types_by_name['GetResourcesRequest']
_GETPARAGRAPHSREQUEST = DESCRIPTOR.message_types_by_name['GetParagraphsRequest']
_GETSENTENCESREQUEST = DESCRIPTOR.message_types_by_name['GetSentencesRequest']
_GETFIELDSREQUEST = DESCRIPTOR.message_types_by_name['GetFieldsRequest']
_TRAINSENTENCE = DESCRIPTOR.message_types_by_name['TrainSentence']
_TRAINPARAGRAPH = DESCRIPTOR.message_types_by_name['TrainParagraph']
_TRAINFIELD = DESCRIPTOR.message_types_by_name['TrainField']
_TRAINRESOURCE = DESCRIPTOR.message_types_by_name['TrainResource']
_REGISTERREQUEST = DESCRIPTOR.message_types_by_name['RegisterRequest']
_REGISTERRESPONSE = DESCRIPTOR.message_types_by_name['RegisterResponse']
_CLOSEREQUEST = DESCRIPTOR.message_types_by_name['CloseRequest']
_MESSAGETOSERVER = DESCRIPTOR.message_types_by_name['MessageToServer']
_MESSAGEFROMSERVER = DESCRIPTOR.message_types_by_name['MessageFromServer']
EnabledMetadata = _reflection.GeneratedProtocolMessageType('EnabledMetadata', (_message.Message,), {
  'DESCRIPTOR' : _ENABLEDMETADATA,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.EnabledMetadata)
  })
_sym_db.RegisterMessage(EnabledMetadata)

TrainLabels = _reflection.GeneratedProtocolMessageType('TrainLabels', (_message.Message,), {
  'DESCRIPTOR' : _TRAINLABELS,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.TrainLabels)
  })
_sym_db.RegisterMessage(TrainLabels)

TrainMetadata = _reflection.GeneratedProtocolMessageType('TrainMetadata', (_message.Message,), {

  'EntitiesEntry' : _reflection.GeneratedProtocolMessageType('EntitiesEntry', (_message.Message,), {
    'DESCRIPTOR' : _TRAINMETADATA_ENTITIESENTRY,
    '__module__' : 'nucliadb_protos.train_pb2'
    # @@protoc_insertion_point(class_scope:train.TrainMetadata.EntitiesEntry)
    })
  ,
  'DESCRIPTOR' : _TRAINMETADATA,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.TrainMetadata)
  })
_sym_db.RegisterMessage(TrainMetadata)
_sym_db.RegisterMessage(TrainMetadata.EntitiesEntry)

GetResourcesRequest = _reflection.GeneratedProtocolMessageType('GetResourcesRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETRESOURCESREQUEST,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.GetResourcesRequest)
  })
_sym_db.RegisterMessage(GetResourcesRequest)

GetParagraphsRequest = _reflection.GeneratedProtocolMessageType('GetParagraphsRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETPARAGRAPHSREQUEST,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.GetParagraphsRequest)
  })
_sym_db.RegisterMessage(GetParagraphsRequest)

GetSentencesRequest = _reflection.GeneratedProtocolMessageType('GetSentencesRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETSENTENCESREQUEST,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.GetSentencesRequest)
  })
_sym_db.RegisterMessage(GetSentencesRequest)

GetFieldsRequest = _reflection.GeneratedProtocolMessageType('GetFieldsRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETFIELDSREQUEST,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.GetFieldsRequest)
  })
_sym_db.RegisterMessage(GetFieldsRequest)

TrainSentence = _reflection.GeneratedProtocolMessageType('TrainSentence', (_message.Message,), {
  'DESCRIPTOR' : _TRAINSENTENCE,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.TrainSentence)
  })
_sym_db.RegisterMessage(TrainSentence)

TrainParagraph = _reflection.GeneratedProtocolMessageType('TrainParagraph', (_message.Message,), {
  'DESCRIPTOR' : _TRAINPARAGRAPH,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.TrainParagraph)
  })
_sym_db.RegisterMessage(TrainParagraph)

TrainField = _reflection.GeneratedProtocolMessageType('TrainField', (_message.Message,), {
  'DESCRIPTOR' : _TRAINFIELD,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.TrainField)
  })
_sym_db.RegisterMessage(TrainField)

TrainResource = _reflection.GeneratedProtocolMessageType('TrainResource', (_message.Message,), {
  'DESCRIPTOR' : _TRAINRESOURCE,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.TrainResource)
  })
_sym_db.RegisterMessage(TrainResource)

RegisterRequest = _reflection.GeneratedProtocolMessageType('RegisterRequest', (_message.Message,), {
  'DESCRIPTOR' : _REGISTERREQUEST,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.RegisterRequest)
  })
_sym_db.RegisterMessage(RegisterRequest)

RegisterResponse = _reflection.GeneratedProtocolMessageType('RegisterResponse', (_message.Message,), {
  'DESCRIPTOR' : _REGISTERRESPONSE,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.RegisterResponse)
  })
_sym_db.RegisterMessage(RegisterResponse)

CloseRequest = _reflection.GeneratedProtocolMessageType('CloseRequest', (_message.Message,), {
  'DESCRIPTOR' : _CLOSEREQUEST,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.CloseRequest)
  })
_sym_db.RegisterMessage(CloseRequest)

MessageToServer = _reflection.GeneratedProtocolMessageType('MessageToServer', (_message.Message,), {
  'DESCRIPTOR' : _MESSAGETOSERVER,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.MessageToServer)
  })
_sym_db.RegisterMessage(MessageToServer)

MessageFromServer = _reflection.GeneratedProtocolMessageType('MessageFromServer', (_message.Message,), {
  'DESCRIPTOR' : _MESSAGEFROMSERVER,
  '__module__' : 'nucliadb_protos.train_pb2'
  # @@protoc_insertion_point(class_scope:train.MessageFromServer)
  })
_sym_db.RegisterMessage(MessageFromServer)

_TRAIN = DESCRIPTOR.services_by_name['Train']
_TUNNEL = DESCRIPTOR.services_by_name['Tunnel']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _TRAINMETADATA_ENTITIESENTRY._options = None
  _TRAINMETADATA_ENTITIESENTRY._serialized_options = b'8\001'
  _ENABLEDMETADATA._serialized_start=170
  _ENABLEDMETADATA._serialized_end=251
  _TRAINLABELS._serialized_start=254
  _TRAINLABELS._serialized_end=400
  _TRAINMETADATA._serialized_start=403
  _TRAINMETADATA._serialized_end=587
  _TRAINMETADATA_ENTITIESENTRY._serialized_start=540
  _TRAINMETADATA_ENTITIESENTRY._serialized_end=587
  _GETRESOURCESREQUEST._serialized_start=589
  _GETRESOURCESREQUEST._serialized_end=694
  _GETPARAGRAPHSREQUEST._serialized_start=697
  _GETPARAGRAPHSREQUEST._serialized_end=852
  _GETSENTENCESREQUEST._serialized_start=855
  _GETSENTENCESREQUEST._serialized_end=1009
  _GETFIELDSREQUEST._serialized_start=1012
  _GETFIELDSREQUEST._serialized_end=1163
  _TRAINSENTENCE._serialized_start=1166
  _TRAINSENTENCE._serialized_end=1307
  _TRAINPARAGRAPH._serialized_start=1309
  _TRAINPARAGRAPH._serialized_end=1433
  _TRAINFIELD._serialized_start=1435
  _TRAINFIELD._serialized_end=1554
  _TRAINRESOURCE._serialized_start=1557
  _TRAINRESOURCE._serialized_end=1760
  _REGISTERREQUEST._serialized_start=1762
  _REGISTERREQUEST._serialized_end=1792
  _REGISTERRESPONSE._serialized_start=1794
  _REGISTERRESPONSE._serialized_end=1828
  _CLOSEREQUEST._serialized_start=1830
  _CLOSEREQUEST._serialized_end=1844
  _MESSAGETOSERVER._serialized_start=1847
  _MESSAGETOSERVER._serialized_end=2266
  _MESSAGEFROMSERVER._serialized_start=2269
  _MESSAGEFROMSERVER._serialized_end=2765
  _TRAIN._serialized_start=2768
  _TRAIN._serialized_end=3205
  _TUNNEL._serialized_start=3207
  _TUNNEL._serialized_end=3279
# @@protoc_insertion_point(module_scope)
