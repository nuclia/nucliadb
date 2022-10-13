# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/knowledgebox.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\"nucliadb_protos/knowledgebox.proto\x12\x0cknowledgebox\",\n\x0eKnowledgeBoxID\x12\x0c\n\x04slug\x18\x01 \x01(\t\x12\x0c\n\x04uuid\x18\x02 \x01(\t\"\x96\x01\n\x0cKnowledgeBox\x12\x0c\n\x04slug\x18\x01 \x01(\t\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12\x38\n\x06status\x18\x03 \x01(\x0e\x32(.knowledgebox.KnowledgeBoxResponseStatus\x12\x30\n\x06\x63onfig\x18\x04 \x01(\x0b\x32 .knowledgebox.KnowledgeBoxConfig\"\x92\x01\n\x12KnowledgeBoxConfig\x12\r\n\x05title\x18\x01 \x01(\t\x12\x13\n\x0b\x64\x65scription\x18\x02 \x01(\t\x12\x17\n\x0f\x65nabled_filters\x18\x03 \x03(\t\x12\x18\n\x10\x65nabled_insights\x18\x04 \x03(\t\x12\x0c\n\x04slug\x18\x05 \x01(\t\x12\x17\n\x0f\x64isable_vectors\x18\x06 \x01(\x08\"d\n\x0fKnowledgeBoxNew\x12\x0c\n\x04slug\x18\x01 \x01(\t\x12\x30\n\x06\x63onfig\x18\x02 \x01(\x0b\x32 .knowledgebox.KnowledgeBoxConfig\x12\x11\n\tforceuuid\x18\x03 \x01(\t\"a\n\x17NewKnowledgeBoxResponse\x12\x38\n\x06status\x18\x01 \x01(\x0e\x32(.knowledgebox.KnowledgeBoxResponseStatus\x12\x0c\n\x04uuid\x18\x02 \x01(\t\"$\n\x12KnowledgeBoxPrefix\x12\x0e\n\x06prefix\x18\x01 \x01(\t\"b\n\x12KnowledgeBoxUpdate\x12\x0c\n\x04slug\x18\x01 \x01(\t\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12\x30\n\x06\x63onfig\x18\x03 \x01(\x0b\x32 .knowledgebox.KnowledgeBoxConfig\"d\n\x1aUpdateKnowledgeBoxResponse\x12\x38\n\x06status\x18\x01 \x01(\x0e\x32(.knowledgebox.KnowledgeBoxResponseStatus\x12\x0c\n\x04uuid\x18\x02 \x01(\t\"\x18\n\x16GCKnowledgeBoxResponse\"V\n\x1a\x44\x65leteKnowledgeBoxResponse\x12\x38\n\x06status\x18\x01 \x01(\x0e\x32(.knowledgebox.KnowledgeBoxResponseStatus\"B\n\x05Label\x12\r\n\x05title\x18\x02 \x01(\t\x12\x0f\n\x07related\x18\x03 \x01(\t\x12\x0c\n\x04text\x18\x04 \x01(\t\x12\x0b\n\x03uri\x18\x05 \x01(\t\"\xd0\x01\n\x08LabelSet\x12\r\n\x05title\x18\x01 \x01(\t\x12\r\n\x05\x63olor\x18\x02 \x01(\t\x12#\n\x06labels\x18\x03 \x03(\x0b\x32\x13.knowledgebox.Label\x12\x10\n\x08multiple\x18\x04 \x01(\x08\x12\x31\n\x04kind\x18\x05 \x03(\x0e\x32#.knowledgebox.LabelSet.LabelSetKind\"<\n\x0cLabelSetKind\x12\r\n\tRESOURCES\x10\x00\x12\x0e\n\nPARAGRAPHS\x10\x01\x12\r\n\tSENTENCES\x10\x02\"\x87\x01\n\x06Labels\x12\x34\n\x08labelset\x18\x01 \x03(\x0b\x32\".knowledgebox.Labels.LabelsetEntry\x1aG\n\rLabelsetEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12%\n\x05value\x18\x02 \x01(\x0b\x32\x16.knowledgebox.LabelSet:\x02\x38\x01\";\n\x06\x45ntity\x12\r\n\x05value\x18\x02 \x01(\t\x12\x0e\n\x06merged\x18\x03 \x01(\x08\x12\x12\n\nrepresents\x18\x04 \x03(\t\"\xc1\x01\n\rEntitiesGroup\x12;\n\x08\x65ntities\x18\x01 \x03(\x0b\x32).knowledgebox.EntitiesGroup.EntitiesEntry\x12\r\n\x05title\x18\x02 \x01(\t\x12\r\n\x05\x63olor\x18\x03 \x01(\t\x12\x0e\n\x06\x63ustom\x18\x04 \x01(\x08\x1a\x45\n\rEntitiesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.knowledgebox.Entity:\x02\x38\x01\"\xfc\x03\n\x06Widget\x12\n\n\x02id\x18\x01 \x01(\t\x12\x13\n\x0b\x64\x65scription\x18\x02 \x01(\t\x12-\n\x04mode\x18\x03 \x01(\x0e\x32\x1f.knowledgebox.Widget.WidgetMode\x12\x35\n\x08\x66\x65\x61tures\x18\x04 \x01(\x0b\x32#.knowledgebox.Widget.WidgetFeatures\x12\x0f\n\x07\x66ilters\x18\x05 \x03(\t\x12\x13\n\x0btopEntities\x18\x06 \x03(\t\x12.\n\x05style\x18\x07 \x03(\x0b\x32\x1f.knowledgebox.Widget.StyleEntry\x1a\xb7\x01\n\x0eWidgetFeatures\x12\x12\n\nuseFilters\x18\x01 \x01(\x08\x12\x17\n\x0fsuggestEntities\x18\x02 \x01(\x08\x12\x18\n\x10suggestSentences\x18\x03 \x01(\x08\x12\x19\n\x11suggestParagraphs\x18\x04 \x01(\x08\x12\x15\n\rsuggestLabels\x18\x05 \x01(\x08\x12\x12\n\neditLabels\x18\x06 \x01(\x08\x12\x18\n\x10\x65ntityAnnotation\x18\x07 \x01(\x08\x1a,\n\nStyleEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"-\n\nWidgetMode\x12\n\n\x06\x42UTTON\x10\x00\x12\t\n\x05INPUT\x10\x01\x12\x08\n\x04\x46ORM\x10\x02*K\n\x1aKnowledgeBoxResponseStatus\x12\x06\n\x02OK\x10\x00\x12\x0c\n\x08\x43ONFLICT\x10\x01\x12\x0c\n\x08NOTFOUND\x10\x02\x12\t\n\x05\x45RROR\x10\x03\x62\x06proto3')

_KNOWLEDGEBOXRESPONSESTATUS = DESCRIPTOR.enum_types_by_name['KnowledgeBoxResponseStatus']
KnowledgeBoxResponseStatus = enum_type_wrapper.EnumTypeWrapper(_KNOWLEDGEBOXRESPONSESTATUS)
OK = 0
CONFLICT = 1
NOTFOUND = 2
ERROR = 3


_KNOWLEDGEBOXID = DESCRIPTOR.message_types_by_name['KnowledgeBoxID']
_KNOWLEDGEBOX = DESCRIPTOR.message_types_by_name['KnowledgeBox']
_KNOWLEDGEBOXCONFIG = DESCRIPTOR.message_types_by_name['KnowledgeBoxConfig']
_KNOWLEDGEBOXNEW = DESCRIPTOR.message_types_by_name['KnowledgeBoxNew']
_NEWKNOWLEDGEBOXRESPONSE = DESCRIPTOR.message_types_by_name['NewKnowledgeBoxResponse']
_KNOWLEDGEBOXPREFIX = DESCRIPTOR.message_types_by_name['KnowledgeBoxPrefix']
_KNOWLEDGEBOXUPDATE = DESCRIPTOR.message_types_by_name['KnowledgeBoxUpdate']
_UPDATEKNOWLEDGEBOXRESPONSE = DESCRIPTOR.message_types_by_name['UpdateKnowledgeBoxResponse']
_GCKNOWLEDGEBOXRESPONSE = DESCRIPTOR.message_types_by_name['GCKnowledgeBoxResponse']
_DELETEKNOWLEDGEBOXRESPONSE = DESCRIPTOR.message_types_by_name['DeleteKnowledgeBoxResponse']
_LABEL = DESCRIPTOR.message_types_by_name['Label']
_LABELSET = DESCRIPTOR.message_types_by_name['LabelSet']
_LABELS = DESCRIPTOR.message_types_by_name['Labels']
_LABELS_LABELSETENTRY = _LABELS.nested_types_by_name['LabelsetEntry']
_ENTITY = DESCRIPTOR.message_types_by_name['Entity']
_ENTITIESGROUP = DESCRIPTOR.message_types_by_name['EntitiesGroup']
_ENTITIESGROUP_ENTITIESENTRY = _ENTITIESGROUP.nested_types_by_name['EntitiesEntry']
_WIDGET = DESCRIPTOR.message_types_by_name['Widget']
_WIDGET_WIDGETFEATURES = _WIDGET.nested_types_by_name['WidgetFeatures']
_WIDGET_STYLEENTRY = _WIDGET.nested_types_by_name['StyleEntry']
_LABELSET_LABELSETKIND = _LABELSET.enum_types_by_name['LabelSetKind']
_WIDGET_WIDGETMODE = _WIDGET.enum_types_by_name['WidgetMode']
KnowledgeBoxID = _reflection.GeneratedProtocolMessageType('KnowledgeBoxID', (_message.Message,), {
  'DESCRIPTOR' : _KNOWLEDGEBOXID,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.KnowledgeBoxID)
  })
_sym_db.RegisterMessage(KnowledgeBoxID)

KnowledgeBox = _reflection.GeneratedProtocolMessageType('KnowledgeBox', (_message.Message,), {
  'DESCRIPTOR' : _KNOWLEDGEBOX,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.KnowledgeBox)
  })
_sym_db.RegisterMessage(KnowledgeBox)

KnowledgeBoxConfig = _reflection.GeneratedProtocolMessageType('KnowledgeBoxConfig', (_message.Message,), {
  'DESCRIPTOR' : _KNOWLEDGEBOXCONFIG,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.KnowledgeBoxConfig)
  })
_sym_db.RegisterMessage(KnowledgeBoxConfig)

KnowledgeBoxNew = _reflection.GeneratedProtocolMessageType('KnowledgeBoxNew', (_message.Message,), {
  'DESCRIPTOR' : _KNOWLEDGEBOXNEW,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.KnowledgeBoxNew)
  })
_sym_db.RegisterMessage(KnowledgeBoxNew)

NewKnowledgeBoxResponse = _reflection.GeneratedProtocolMessageType('NewKnowledgeBoxResponse', (_message.Message,), {
  'DESCRIPTOR' : _NEWKNOWLEDGEBOXRESPONSE,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.NewKnowledgeBoxResponse)
  })
_sym_db.RegisterMessage(NewKnowledgeBoxResponse)

KnowledgeBoxPrefix = _reflection.GeneratedProtocolMessageType('KnowledgeBoxPrefix', (_message.Message,), {
  'DESCRIPTOR' : _KNOWLEDGEBOXPREFIX,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.KnowledgeBoxPrefix)
  })
_sym_db.RegisterMessage(KnowledgeBoxPrefix)

KnowledgeBoxUpdate = _reflection.GeneratedProtocolMessageType('KnowledgeBoxUpdate', (_message.Message,), {
  'DESCRIPTOR' : _KNOWLEDGEBOXUPDATE,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.KnowledgeBoxUpdate)
  })
_sym_db.RegisterMessage(KnowledgeBoxUpdate)

UpdateKnowledgeBoxResponse = _reflection.GeneratedProtocolMessageType('UpdateKnowledgeBoxResponse', (_message.Message,), {
  'DESCRIPTOR' : _UPDATEKNOWLEDGEBOXRESPONSE,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.UpdateKnowledgeBoxResponse)
  })
_sym_db.RegisterMessage(UpdateKnowledgeBoxResponse)

GCKnowledgeBoxResponse = _reflection.GeneratedProtocolMessageType('GCKnowledgeBoxResponse', (_message.Message,), {
  'DESCRIPTOR' : _GCKNOWLEDGEBOXRESPONSE,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.GCKnowledgeBoxResponse)
  })
_sym_db.RegisterMessage(GCKnowledgeBoxResponse)

DeleteKnowledgeBoxResponse = _reflection.GeneratedProtocolMessageType('DeleteKnowledgeBoxResponse', (_message.Message,), {
  'DESCRIPTOR' : _DELETEKNOWLEDGEBOXRESPONSE,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.DeleteKnowledgeBoxResponse)
  })
_sym_db.RegisterMessage(DeleteKnowledgeBoxResponse)

Label = _reflection.GeneratedProtocolMessageType('Label', (_message.Message,), {
  'DESCRIPTOR' : _LABEL,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.Label)
  })
_sym_db.RegisterMessage(Label)

LabelSet = _reflection.GeneratedProtocolMessageType('LabelSet', (_message.Message,), {
  'DESCRIPTOR' : _LABELSET,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.LabelSet)
  })
_sym_db.RegisterMessage(LabelSet)

Labels = _reflection.GeneratedProtocolMessageType('Labels', (_message.Message,), {

  'LabelsetEntry' : _reflection.GeneratedProtocolMessageType('LabelsetEntry', (_message.Message,), {
    'DESCRIPTOR' : _LABELS_LABELSETENTRY,
    '__module__' : 'nucliadb_protos.knowledgebox_pb2'
    # @@protoc_insertion_point(class_scope:knowledgebox.Labels.LabelsetEntry)
    })
  ,
  'DESCRIPTOR' : _LABELS,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.Labels)
  })
_sym_db.RegisterMessage(Labels)
_sym_db.RegisterMessage(Labels.LabelsetEntry)

Entity = _reflection.GeneratedProtocolMessageType('Entity', (_message.Message,), {
  'DESCRIPTOR' : _ENTITY,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.Entity)
  })
_sym_db.RegisterMessage(Entity)

EntitiesGroup = _reflection.GeneratedProtocolMessageType('EntitiesGroup', (_message.Message,), {

  'EntitiesEntry' : _reflection.GeneratedProtocolMessageType('EntitiesEntry', (_message.Message,), {
    'DESCRIPTOR' : _ENTITIESGROUP_ENTITIESENTRY,
    '__module__' : 'nucliadb_protos.knowledgebox_pb2'
    # @@protoc_insertion_point(class_scope:knowledgebox.EntitiesGroup.EntitiesEntry)
    })
  ,
  'DESCRIPTOR' : _ENTITIESGROUP,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.EntitiesGroup)
  })
_sym_db.RegisterMessage(EntitiesGroup)
_sym_db.RegisterMessage(EntitiesGroup.EntitiesEntry)

Widget = _reflection.GeneratedProtocolMessageType('Widget', (_message.Message,), {

  'WidgetFeatures' : _reflection.GeneratedProtocolMessageType('WidgetFeatures', (_message.Message,), {
    'DESCRIPTOR' : _WIDGET_WIDGETFEATURES,
    '__module__' : 'nucliadb_protos.knowledgebox_pb2'
    # @@protoc_insertion_point(class_scope:knowledgebox.Widget.WidgetFeatures)
    })
  ,

  'StyleEntry' : _reflection.GeneratedProtocolMessageType('StyleEntry', (_message.Message,), {
    'DESCRIPTOR' : _WIDGET_STYLEENTRY,
    '__module__' : 'nucliadb_protos.knowledgebox_pb2'
    # @@protoc_insertion_point(class_scope:knowledgebox.Widget.StyleEntry)
    })
  ,
  'DESCRIPTOR' : _WIDGET,
  '__module__' : 'nucliadb_protos.knowledgebox_pb2'
  # @@protoc_insertion_point(class_scope:knowledgebox.Widget)
  })
_sym_db.RegisterMessage(Widget)
_sym_db.RegisterMessage(Widget.WidgetFeatures)
_sym_db.RegisterMessage(Widget.StyleEntry)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _LABELS_LABELSETENTRY._options = None
  _LABELS_LABELSETENTRY._serialized_options = b'8\001'
  _ENTITIESGROUP_ENTITIESENTRY._options = None
  _ENTITIESGROUP_ENTITIESENTRY._serialized_options = b'8\001'
  _WIDGET_STYLEENTRY._options = None
  _WIDGET_STYLEENTRY._serialized_options = b'8\001'
  _KNOWLEDGEBOXRESPONSESTATUS._serialized_start=2140
  _KNOWLEDGEBOXRESPONSESTATUS._serialized_end=2215
  _KNOWLEDGEBOXID._serialized_start=52
  _KNOWLEDGEBOXID._serialized_end=96
  _KNOWLEDGEBOX._serialized_start=99
  _KNOWLEDGEBOX._serialized_end=249
  _KNOWLEDGEBOXCONFIG._serialized_start=252
  _KNOWLEDGEBOXCONFIG._serialized_end=398
  _KNOWLEDGEBOXNEW._serialized_start=400
  _KNOWLEDGEBOXNEW._serialized_end=500
  _NEWKNOWLEDGEBOXRESPONSE._serialized_start=502
  _NEWKNOWLEDGEBOXRESPONSE._serialized_end=599
  _KNOWLEDGEBOXPREFIX._serialized_start=601
  _KNOWLEDGEBOXPREFIX._serialized_end=637
  _KNOWLEDGEBOXUPDATE._serialized_start=639
  _KNOWLEDGEBOXUPDATE._serialized_end=737
  _UPDATEKNOWLEDGEBOXRESPONSE._serialized_start=739
  _UPDATEKNOWLEDGEBOXRESPONSE._serialized_end=839
  _GCKNOWLEDGEBOXRESPONSE._serialized_start=841
  _GCKNOWLEDGEBOXRESPONSE._serialized_end=865
  _DELETEKNOWLEDGEBOXRESPONSE._serialized_start=867
  _DELETEKNOWLEDGEBOXRESPONSE._serialized_end=953
  _LABEL._serialized_start=955
  _LABEL._serialized_end=1021
  _LABELSET._serialized_start=1024
  _LABELSET._serialized_end=1232
  _LABELSET_LABELSETKIND._serialized_start=1172
  _LABELSET_LABELSETKIND._serialized_end=1232
  _LABELS._serialized_start=1235
  _LABELS._serialized_end=1370
  _LABELS_LABELSETENTRY._serialized_start=1299
  _LABELS_LABELSETENTRY._serialized_end=1370
  _ENTITY._serialized_start=1372
  _ENTITY._serialized_end=1431
  _ENTITIESGROUP._serialized_start=1434
  _ENTITIESGROUP._serialized_end=1627
  _ENTITIESGROUP_ENTITIESENTRY._serialized_start=1558
  _ENTITIESGROUP_ENTITIESENTRY._serialized_end=1627
  _WIDGET._serialized_start=1630
  _WIDGET._serialized_end=2138
  _WIDGET_WIDGETFEATURES._serialized_start=1862
  _WIDGET_WIDGETFEATURES._serialized_end=2045
  _WIDGET_STYLEENTRY._serialized_start=2047
  _WIDGET_STYLEENTRY._serialized_end=2091
  _WIDGET_WIDGETMODE._serialized_start=2093
  _WIDGET_WIDGETMODE._serialized_end=2138
# @@protoc_insertion_point(module_scope)
