# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/train.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
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

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1bnucliadb_protos/train.proto\x12\x05train\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\"nucliadb_protos/knowledgebox.proto\x1a\x1fnucliadb_protos/resources.proto\x1a\x1cnucliadb_protos/writer.proto\"Q\n\x0f\x45nabledMetadata\x12\x0c\n\x04text\x18\x01 \x01(\x08\x12\x10\n\x08\x65ntities\x18\x02 \x01(\x08\x12\x0e\n\x06labels\x18\x03 \x01(\x08\x12\x0e\n\x06vector\x18\x04 \x01(\x08\"\x92\x01\n\x0bTrainLabels\x12+\n\x08resource\x18\x01 \x03(\x0b\x32\x19.resources.Classification\x12(\n\x05\x66ield\x18\x02 \x03(\x0b\x32\x19.resources.Classification\x12,\n\tparagraph\x18\x03 \x03(\x0b\x32\x19.resources.Classification\"&\n\x08Position\x12\r\n\x05start\x18\x01 \x01(\x03\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x03\"E\n\x0f\x45ntityPositions\x12\x0e\n\x06\x65ntity\x18\x01 \x01(\t\x12\"\n\tpositions\x18\x02 \x03(\x0b\x32\x0f.train.Position\"\xcd\x02\n\rTrainMetadata\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x34\n\x08\x65ntities\x18\x02 \x03(\x0b\x32\".train.TrainMetadata.EntitiesEntry\x12\x43\n\x10\x65ntity_positions\x18\x05 \x03(\x0b\x32).train.TrainMetadata.EntityPositionsEntry\x12\"\n\x06labels\x18\x03 \x01(\x0b\x32\x12.train.TrainLabels\x12\x0e\n\x06vector\x18\x04 \x03(\x02\x1a/\n\rEntitiesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1aN\n\x14\x45ntityPositionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12%\n\x05value\x18\x02 \x01(\x0b\x32\x16.train.EntityPositions:\x02\x38\x01\":\n\x0eGetInfoRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\"}\n\x18GetLabelsetsCountRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x1b\n\x13paragraph_labelsets\x18\x02 \x03(\t\x12\x1a\n\x12resource_labelsets\x18\x03 \x03(\t\"\x87\x01\n\x13GetResourcesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12(\n\x08metadata\x18\x02 \x01(\x0b\x32\x16.train.EnabledMetadata\x12\x0c\n\x04size\x18\x03 \x01(\x04\x12\x0e\n\x06random\x18\x04 \x01(\x08\"\xb9\x01\n\x14GetParagraphsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldID\x12(\n\x08metadata\x18\x04 \x01(\x0b\x32\x16.train.EnabledMetadata\x12\x0c\n\x04size\x18\x05 \x01(\x04\x12\x0e\n\x06random\x18\x06 \x01(\x08\"\xb8\x01\n\x13GetSentencesRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldID\x12(\n\x08metadata\x18\x04 \x01(\x0b\x32\x16.train.EnabledMetadata\x12\x0c\n\x04size\x18\x05 \x01(\x04\x12\x0e\n\x06random\x18\x06 \x01(\x08\"\xb5\x01\n\x10GetFieldsRequest\x12(\n\x02kb\x18\x01 \x01(\x0b\x32\x1c.knowledgebox.KnowledgeBoxID\x12\x0c\n\x04uuid\x18\x02 \x01(\t\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldID\x12(\n\x08metadata\x18\x04 \x01(\x0b\x32\x16.train.EnabledMetadata\x12\x0c\n\x04size\x18\x05 \x01(\x04\x12\x0e\n\x06random\x18\x06 \x01(\x08\"U\n\tTrainInfo\x12\x11\n\tresources\x18\x01 \x01(\x04\x12\x0e\n\x06\x66ields\x18\x02 \x01(\x04\x12\x12\n\nparagraphs\x18\x03 \x01(\x04\x12\x11\n\tsentences\x18\x04 \x01(\x04\"\x8d\x01\n\rTrainSentence\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12!\n\x05\x66ield\x18\x02 \x01(\x0b\x32\x12.resources.FieldID\x12\x11\n\tparagraph\x18\x03 \x01(\t\x12\x10\n\x08sentence\x18\x04 \x01(\t\x12&\n\x08metadata\x18\x05 \x01(\x0b\x32\x14.train.TrainMetadata\"|\n\x0eTrainParagraph\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12!\n\x05\x66ield\x18\x02 \x01(\x0b\x32\x12.resources.FieldID\x12\x11\n\tparagraph\x18\x03 \x01(\t\x12&\n\x08metadata\x18\x04 \x01(\x0b\x32\x14.train.TrainMetadata\"w\n\nTrainField\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12!\n\x05\x66ield\x18\x02 \x01(\x0b\x32\x12.resources.FieldID\x12\x10\n\x08subfield\x18\x03 \x01(\t\x12&\n\x08metadata\x18\x04 \x01(\x0b\x32\x14.train.TrainMetadata\"\xcb\x01\n\rTrainResource\x12\x0c\n\x04uuid\x18\x01 \x01(\t\x12\r\n\x05title\x18\x02 \x01(\t\x12\x0c\n\x04icon\x18\x03 \x01(\t\x12\x0c\n\x04slug\x18\x04 \x01(\t\x12+\n\x07\x63reated\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12,\n\x08modified\x18\x06 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12&\n\x08metadata\x18\x07 \x01(\x0b\x32\x14.train.TrainMetadata\"\xe6\x01\n\rLabelsetCount\x12\x38\n\nparagraphs\x18\x01 \x03(\x0b\x32$.train.LabelsetCount.ParagraphsEntry\x12\x36\n\tresources\x18\x02 \x03(\x0b\x32#.train.LabelsetCount.ResourcesEntry\x1a\x31\n\x0fParagraphsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x03:\x02\x38\x01\x1a\x30\n\x0eResourcesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x03:\x02\x38\x01\"\x91\x01\n\x0eLabelsetsCount\x12\x37\n\tlabelsets\x18\x02 \x03(\x0b\x32$.train.LabelsetsCount.LabelsetsEntry\x1a\x46\n\x0eLabelsetsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.train.LabelsetCount:\x02\x38\x01\"\xd3\x01\n\x08TrainSet\x12\x19\n\x04type\x18\x01 \x01(\x0e\x32\x0b.train.Type\x12&\n\x06\x66ilter\x18\x02 \x01(\x0b\x32\x16.train.TrainSet.Filter\x12\x12\n\nbatch_size\x18\x03 \x01(\x05\x12\x0c\n\x04seed\x18\x04 \x01(\x05\x12\r\n\x05split\x18\x05 \x01(\x02\x12\x14\n\x0cminresources\x18\x06 \x01(\x05\x12\r\n\x05nones\x18\x07 \x01(\x08\x12\x14\n\x0coversampling\x18\x08 \x01(\x02\x1a\x18\n\x06\x46ilter\x12\x0e\n\x06labels\x18\x01 \x03(\t\"]\n\x05Label\x12\x10\n\x08labelset\x18\x01 \x01(\t\x12\r\n\x05label\x18\x02 \x01(\t\x12 \n\x06origin\x18\x03 \x01(\x0e\x32\x10.train.LabelFrom\x12\x11\n\tannotated\x18\x04 \x01(\x08\"7\n\tTextLabel\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x1c\n\x06labels\x18\x02 \x03(\x0b\x32\x0c.train.Label\"=\n\x1bResourceClassificationBatch\x12\x1e\n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32\x10.train.TextLabel\">\n\x1cParagraphClassificationBatch\x12\x1e\n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32\x10.train.TextLabel\"=\n\x1bSentenceClassificationBatch\x12\x1e\n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32\x10.train.TextLabel\"H\n\x05Token\x12\r\n\x05group\x18\x01 \x01(\t\x12\x11\n\tannotated\x18\x02 \x01(\x08\"\x1d\n\x06Schema\x12\x05\n\x01O\x10\x00\x12\x05\n\x01\x42\x10\x01\x12\x05\n\x01I\x10\x02\"B\n\x14TokensClassification\x12\x0c\n\x04text\x18\x01 \x03(\t\x12\x1c\n\x06labels\x18\x02 \x03(\x0b\x32\x0c.train.Token\"E\n\x18TokenClassificationBatch\x12)\n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32\x1b.train.TokensClassification\"G\n\rTrainResponse\x12\r\n\x05train\x18\x01 \x01(\x03\x12\x0c\n\x04test\x18\x02 \x01(\x03\x12\x19\n\x04type\x18\x03 \x01(\x0e\x32\x0b.train.Type*x\n\x04Type\x12\x1b\n\x17RESOURCE_CLASSIFICATION\x10\x00\x12\x1c\n\x18PARAGRAPH_CLASSIFICATION\x10\x01\x12\x1b\n\x17SENTENCE_CLASSIFICATION\x10\x02\x12\x18\n\x14TOKEN_CLASSIFICATION\x10\x03*(\n\tLabelFrom\x12\r\n\tPARAGRAPH\x10\x00\x12\x0c\n\x08RESOURCE\x10\x01\x32\xb9\x04\n\x05Train\x12\x34\n\x07GetInfo\x12\x15.train.GetInfoRequest\x1a\x10.train.TrainInfo\"\x00\x12\x44\n\x0cGetSentences\x12\x1a.train.GetSentencesRequest\x1a\x14.train.TrainSentence\"\x00\x30\x01\x12G\n\rGetParagraphs\x12\x1b.train.GetParagraphsRequest\x1a\x15.train.TrainParagraph\"\x00\x30\x01\x12;\n\tGetFields\x12\x17.train.GetFieldsRequest\x1a\x11.train.TrainField\"\x00\x30\x01\x12\x44\n\x0cGetResources\x12\x1a.train.GetResourcesRequest\x1a\x14.train.TrainResource\"\x00\x30\x01\x12J\n\x0bGetOntology\x12\x1b.fdbwriter.GetLabelsRequest\x1a\x1c.fdbwriter.GetLabelsResponse\"\x00\x12N\n\x0bGetEntities\x12\x1d.fdbwriter.GetEntitiesRequest\x1a\x1e.fdbwriter.GetEntitiesResponse\"\x00\x12L\n\x10GetOntologyCount\x12\x1f.train.GetLabelsetsCountRequest\x1a\x15.train.LabelsetsCount\"\x00P\x01P\x02P\x03\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.train_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _TRAINMETADATA_ENTITIESENTRY._options = None
  _TRAINMETADATA_ENTITIESENTRY._serialized_options = b'8\001'
  _TRAINMETADATA_ENTITYPOSITIONSENTRY._options = None
  _TRAINMETADATA_ENTITYPOSITIONSENTRY._serialized_options = b'8\001'
  _LABELSETCOUNT_PARAGRAPHSENTRY._options = None
  _LABELSETCOUNT_PARAGRAPHSENTRY._serialized_options = b'8\001'
  _LABELSETCOUNT_RESOURCESENTRY._options = None
  _LABELSETCOUNT_RESOURCESENTRY._serialized_options = b'8\001'
  _LABELSETSCOUNT_LABELSETSENTRY._options = None
  _LABELSETSCOUNT_LABELSETSENTRY._serialized_options = b'8\001'
  _TYPE._serialized_start=3640
  _TYPE._serialized_end=3760
  _LABELFROM._serialized_start=3762
  _LABELFROM._serialized_end=3802
  _ENABLEDMETADATA._serialized_start=170
  _ENABLEDMETADATA._serialized_end=251
  _TRAINLABELS._serialized_start=254
  _TRAINLABELS._serialized_end=400
  _POSITION._serialized_start=402
  _POSITION._serialized_end=440
  _ENTITYPOSITIONS._serialized_start=442
  _ENTITYPOSITIONS._serialized_end=511
  _TRAINMETADATA._serialized_start=514
  _TRAINMETADATA._serialized_end=847
  _TRAINMETADATA_ENTITIESENTRY._serialized_start=720
  _TRAINMETADATA_ENTITIESENTRY._serialized_end=767
  _TRAINMETADATA_ENTITYPOSITIONSENTRY._serialized_start=769
  _TRAINMETADATA_ENTITYPOSITIONSENTRY._serialized_end=847
  _GETINFOREQUEST._serialized_start=849
  _GETINFOREQUEST._serialized_end=907
  _GETLABELSETSCOUNTREQUEST._serialized_start=909
  _GETLABELSETSCOUNTREQUEST._serialized_end=1034
  _GETRESOURCESREQUEST._serialized_start=1037
  _GETRESOURCESREQUEST._serialized_end=1172
  _GETPARAGRAPHSREQUEST._serialized_start=1175
  _GETPARAGRAPHSREQUEST._serialized_end=1360
  _GETSENTENCESREQUEST._serialized_start=1363
  _GETSENTENCESREQUEST._serialized_end=1547
  _GETFIELDSREQUEST._serialized_start=1550
  _GETFIELDSREQUEST._serialized_end=1731
  _TRAININFO._serialized_start=1733
  _TRAININFO._serialized_end=1818
  _TRAINSENTENCE._serialized_start=1821
  _TRAINSENTENCE._serialized_end=1962
  _TRAINPARAGRAPH._serialized_start=1964
  _TRAINPARAGRAPH._serialized_end=2088
  _TRAINFIELD._serialized_start=2090
  _TRAINFIELD._serialized_end=2209
  _TRAINRESOURCE._serialized_start=2212
  _TRAINRESOURCE._serialized_end=2415
  _LABELSETCOUNT._serialized_start=2418
  _LABELSETCOUNT._serialized_end=2648
  _LABELSETCOUNT_PARAGRAPHSENTRY._serialized_start=2549
  _LABELSETCOUNT_PARAGRAPHSENTRY._serialized_end=2598
  _LABELSETCOUNT_RESOURCESENTRY._serialized_start=2600
  _LABELSETCOUNT_RESOURCESENTRY._serialized_end=2648
  _LABELSETSCOUNT._serialized_start=2651
  _LABELSETSCOUNT._serialized_end=2796
  _LABELSETSCOUNT_LABELSETSENTRY._serialized_start=2726
  _LABELSETSCOUNT_LABELSETSENTRY._serialized_end=2796
  _TRAINSET._serialized_start=2799
  _TRAINSET._serialized_end=3010
  _TRAINSET_FILTER._serialized_start=2986
  _TRAINSET_FILTER._serialized_end=3010
  _LABEL._serialized_start=3012
  _LABEL._serialized_end=3105
  _TEXTLABEL._serialized_start=3107
  _TEXTLABEL._serialized_end=3162
  _RESOURCECLASSIFICATIONBATCH._serialized_start=3164
  _RESOURCECLASSIFICATIONBATCH._serialized_end=3225
  _PARAGRAPHCLASSIFICATIONBATCH._serialized_start=3227
  _PARAGRAPHCLASSIFICATIONBATCH._serialized_end=3289
  _SENTENCECLASSIFICATIONBATCH._serialized_start=3291
  _SENTENCECLASSIFICATIONBATCH._serialized_end=3352
  _TOKEN._serialized_start=3354
  _TOKEN._serialized_end=3426
  _TOKEN_SCHEMA._serialized_start=3397
  _TOKEN_SCHEMA._serialized_end=3426
  _TOKENSCLASSIFICATION._serialized_start=3428
  _TOKENSCLASSIFICATION._serialized_end=3494
  _TOKENCLASSIFICATIONBATCH._serialized_start=3496
  _TOKENCLASSIFICATIONBATCH._serialized_end=3565
  _TRAINRESPONSE._serialized_start=3567
  _TRAINRESPONSE._serialized_end=3638
  _TRAIN._serialized_start=3805
  _TRAIN._serialized_end=4374
# @@protoc_insertion_point(module_scope)
