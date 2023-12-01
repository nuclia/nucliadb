# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/dataset.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1dnucliadb_protos/dataset.proto\x12\x07\x64\x61taset\"\x83\x01\n\x08TrainSet\x12\x1f\n\x04type\x18\x01 \x01(\x0e\x32\x11.dataset.TaskType\x12(\n\x06\x66ilter\x18\x02 \x01(\x0b\x32\x18.dataset.TrainSet.Filter\x12\x12\n\nbatch_size\x18\x03 \x01(\x05\x1a\x18\n\x06\x46ilter\x12\x0e\n\x06labels\x18\x01 \x03(\t\"L\n\x05Label\x12\x10\n\x08labelset\x18\x01 \x01(\t\x12\r\n\x05label\x18\x02 \x01(\t\x12\"\n\x06origin\x18\x03 \x01(\x0e\x32\x12.dataset.LabelFrom\"9\n\tTextLabel\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x1e\n\x06labels\x18\x02 \x03(\x0b\x32\x0e.dataset.Label\"F\n\x16MultipleTextSameLabels\x12\x0c\n\x04text\x18\x01 \x03(\t\x12\x1e\n\x06labels\x18\x02 \x03(\x0b\x32\x0e.dataset.Label\"<\n\x18\x46ieldClassificationBatch\x12 \n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32\x12.dataset.TextLabel\"@\n\x1cParagraphClassificationBatch\x12 \n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32\x12.dataset.TextLabel\"L\n\x1bSentenceClassificationBatch\x12-\n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32\x1f.dataset.MultipleTextSameLabels\"4\n\x14TokensClassification\x12\r\n\x05token\x18\x01 \x03(\t\x12\r\n\x05label\x18\x02 \x03(\t\"G\n\x18TokenClassificationBatch\x12+\n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32\x1d.dataset.TokensClassification\";\n\x13ImageClassification\x12\x12\n\nselections\x18\x01 \x01(\t\x12\x10\n\x08page_uri\x18\x02 \x01(\t\"F\n\x18ImageClassificationBatch\x12*\n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32\x1c.dataset.ImageClassification\"/\n\x13ParagraphStreamItem\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04text\x18\x02 \x01(\t\"E\n\x17ParagraphStreamingBatch\x12*\n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32\x1c.dataset.ParagraphStreamItem\"%\n\tParagraph\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04text\x18\x02 \x01(\t\"*\n\x08Question\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x10\n\x08language\x18\x02 \x01(\t\"(\n\x06\x41nswer\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x10\n\x08language\x18\x02 \x01(\t\"\x88\x01\n\x18QuestionAnswerStreamItem\x12#\n\x08question\x18\x01 \x01(\x0b\x32\x11.dataset.Question\x12\x1f\n\x06\x61nswer\x18\x02 \x01(\x0b\x32\x0f.dataset.Answer\x12&\n\nparagraphs\x18\x03 \x03(\x0b\x32\x12.dataset.Paragraph\"O\n\x1cQuestionAnswerStreamingBatch\x12/\n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32!.dataset.QuestionAnswerStreamItem*\xcb\x01\n\x08TaskType\x12\x18\n\x14\x46IELD_CLASSIFICATION\x10\x00\x12\x1c\n\x18PARAGRAPH_CLASSIFICATION\x10\x01\x12\x1b\n\x17SENTENCE_CLASSIFICATION\x10\x02\x12\x18\n\x14TOKEN_CLASSIFICATION\x10\x03\x12\x18\n\x14IMAGE_CLASSIFICATION\x10\x04\x12\x17\n\x13PARAGRAPH_STREAMING\x10\x05\x12\x1d\n\x19QUESTION_ANSWER_STREAMING\x10\x06*3\n\tLabelFrom\x12\r\n\tPARAGRAPH\x10\x00\x12\t\n\x05\x46IELD\x10\x01\x12\x0c\n\x08RESOURCE\x10\x02\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.dataset_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _globals['_TASKTYPE']._serialized_start=1317
  _globals['_TASKTYPE']._serialized_end=1520
  _globals['_LABELFROM']._serialized_start=1522
  _globals['_LABELFROM']._serialized_end=1573
  _globals['_TRAINSET']._serialized_start=43
  _globals['_TRAINSET']._serialized_end=174
  _globals['_TRAINSET_FILTER']._serialized_start=150
  _globals['_TRAINSET_FILTER']._serialized_end=174
  _globals['_LABEL']._serialized_start=176
  _globals['_LABEL']._serialized_end=252
  _globals['_TEXTLABEL']._serialized_start=254
  _globals['_TEXTLABEL']._serialized_end=311
  _globals['_MULTIPLETEXTSAMELABELS']._serialized_start=313
  _globals['_MULTIPLETEXTSAMELABELS']._serialized_end=383
  _globals['_FIELDCLASSIFICATIONBATCH']._serialized_start=385
  _globals['_FIELDCLASSIFICATIONBATCH']._serialized_end=445
  _globals['_PARAGRAPHCLASSIFICATIONBATCH']._serialized_start=447
  _globals['_PARAGRAPHCLASSIFICATIONBATCH']._serialized_end=511
  _globals['_SENTENCECLASSIFICATIONBATCH']._serialized_start=513
  _globals['_SENTENCECLASSIFICATIONBATCH']._serialized_end=589
  _globals['_TOKENSCLASSIFICATION']._serialized_start=591
  _globals['_TOKENSCLASSIFICATION']._serialized_end=643
  _globals['_TOKENCLASSIFICATIONBATCH']._serialized_start=645
  _globals['_TOKENCLASSIFICATIONBATCH']._serialized_end=716
  _globals['_IMAGECLASSIFICATION']._serialized_start=718
  _globals['_IMAGECLASSIFICATION']._serialized_end=777
  _globals['_IMAGECLASSIFICATIONBATCH']._serialized_start=779
  _globals['_IMAGECLASSIFICATIONBATCH']._serialized_end=849
  _globals['_PARAGRAPHSTREAMITEM']._serialized_start=851
  _globals['_PARAGRAPHSTREAMITEM']._serialized_end=898
  _globals['_PARAGRAPHSTREAMINGBATCH']._serialized_start=900
  _globals['_PARAGRAPHSTREAMINGBATCH']._serialized_end=969
  _globals['_PARAGRAPH']._serialized_start=971
  _globals['_PARAGRAPH']._serialized_end=1008
  _globals['_QUESTION']._serialized_start=1010
  _globals['_QUESTION']._serialized_end=1052
  _globals['_ANSWER']._serialized_start=1054
  _globals['_ANSWER']._serialized_end=1094
  _globals['_QUESTIONANSWERSTREAMITEM']._serialized_start=1097
  _globals['_QUESTIONANSWERSTREAMITEM']._serialized_end=1233
  _globals['_QUESTIONANSWERSTREAMINGBATCH']._serialized_start=1235
  _globals['_QUESTIONANSWERSTREAMINGBATCH']._serialized_end=1314
# @@protoc_insertion_point(module_scope)
