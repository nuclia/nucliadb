# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/kb_usage.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1enucliadb_protos/kb_usage.proto\x12\x08kb_usage\x1a\x1fgoogle/protobuf/timestamp.proto\"\xf2\x01\n\x07Process\x12$\n\x06\x63lient\x18\x01 \x01(\x0e\x32\x14.kb_usage.ClientType\x12\x1c\n\x14slow_processing_time\x18\x02 \x01(\x02\x12\x1b\n\x13pre_processing_time\x18\x03 \x01(\x02\x12\r\n\x05\x62ytes\x18\x04 \x01(\x04\x12\r\n\x05\x63hars\x18\x05 \x01(\r\x12\x15\n\rmedia_seconds\x18\x06 \x01(\r\x12\r\n\x05pages\x18\x07 \x01(\r\x12\x12\n\nparagraphs\x18\x08 \x01(\r\x12\x17\n\x0bmedia_files\x18\t \x01(\rB\x02\x18\x01\x12\x15\n\rnum_processed\x18\n \x01(\r\"\x95\x01\n\x07Storage\x12\x17\n\nparagraphs\x18\x01 \x01(\x04H\x00\x88\x01\x01\x12\x13\n\x06\x66ields\x18\x02 \x01(\x04H\x01\x88\x01\x01\x12\x16\n\tresources\x18\x03 \x01(\x04H\x02\x88\x01\x01\x12\x12\n\x05\x62ytes\x18\x04 \x01(\x04H\x03\x88\x01\x01\x42\r\n\x0b_paragraphsB\t\n\x07_fieldsB\x0c\n\n_resourcesB\x08\n\x06_bytes\"x\n\x06Search\x12$\n\x06\x63lient\x18\x01 \x01(\x0e\x32\x14.kb_usage.ClientType\x12\"\n\x04type\x18\x02 \x01(\x0e\x32\x14.kb_usage.SearchType\x12\x0e\n\x06tokens\x18\x03 \x01(\r\x12\x14\n\x0cnum_searches\x18\x04 \x01(\r\"\xa7\x01\n\x07Predict\x12$\n\x06\x63lient\x18\x01 \x01(\x0e\x32\x14.kb_usage.ClientType\x12#\n\x04type\x18\x02 \x01(\x0e\x32\x15.kb_usage.PredictType\x12\r\n\x05model\x18\x03 \x01(\t\x12\r\n\x05input\x18\x04 \x01(\r\x12\x0e\n\x06output\x18\x05 \x01(\r\x12\r\n\x05image\x18\x06 \x01(\r\x12\x14\n\x0cnum_predicts\x18\x07 \x01(\r\"\xed\x02\n\x07KbUsage\x12\"\n\x07service\x18\x01 \x01(\x0e\x32\x11.kb_usage.Service\x12-\n\ttimestamp\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x17\n\naccount_id\x18\x03 \x01(\tH\x00\x88\x01\x01\x12\x12\n\x05kb_id\x18\x04 \x01(\tH\x01\x88\x01\x01\x12%\n\tkb_source\x18\x05 \x01(\x0e\x32\x12.kb_usage.KBSource\x12$\n\tprocesses\x18\x06 \x03(\x0b\x32\x11.kb_usage.Process\x12#\n\x08predicts\x18\x07 \x03(\x0b\x32\x11.kb_usage.Predict\x12\"\n\x08searches\x18\x08 \x03(\x0b\x32\x10.kb_usage.Search\x12\'\n\x07storage\x18\t \x01(\x0b\x32\x11.kb_usage.StorageH\x02\x88\x01\x01\x42\r\n\x0b_account_idB\x08\n\x06_kb_idB\n\n\x08_storage\"9\n\x11KbUsageAggregated\x12$\n\tkb_usages\x18\x01 \x03(\x0b\x32\x11.kb_usage.KbUsage*\"\n\x08KBSource\x12\n\n\x06HOSTED\x10\x00\x12\n\n\x06ONPREM\x10\x01*?\n\x07Service\x12\x0b\n\x07PREDICT\x10\x00\x12\x0e\n\nPROCESSING\x10\x01\x12\r\n\tNUCLIA_DB\x10\x02\x12\x08\n\x04TASK\x10\x03*%\n\nSearchType\x12\n\n\x06SEARCH\x10\x00\x12\x0b\n\x07SUGGEST\x10\x01*\x99\x02\n\x0bPredictType\x12\x0c\n\x08SENTENCE\x10\x00\x12\t\n\x05TOKEN\x10\x01\x12\x13\n\x0fQUESTION_ANSWER\x10\x02\x12\x0c\n\x08REPHRASE\x10\x03\x12\x16\n\x12RETRIEVAL_REPHRASE\x10\x10\x12\r\n\tSUMMARIZE\x10\x04\x12\x12\n\x0e\x45XTRACT_TABLES\x10\x05\x12\n\n\x06RERANK\x10\x06\x12\r\n\tRELATIONS\x10\x07\x12\n\n\x06SPEECH\x10\x08\x12\x0b\n\x07\x43\x41PTION\x10\t\x12\r\n\tDA_LABELS\x10\n\x12\x0c\n\x08\x44\x41_GRAPH\x10\x0b\x12\n\n\x06\x44\x41_ASK\x10\x0c\x12\t\n\x05\x44\x41_QA\x10\r\x12\x0c\n\x08\x44\x41_GUARD\x10\x0e\x12\x08\n\x04REMI\x10\x0f\x12\x13\n\x0fVLLM_EXTRACTION\x10\x11*j\n\nClientType\x12\x07\n\x03\x41PI\x10\x00\x12\x07\n\x03WEB\x10\x01\x12\n\n\x06WIDGET\x10\x02\x12\x0b\n\x07\x44\x45SKTOP\x10\x03\x12\r\n\tDASHBOARD\x10\x04\x12\x14\n\x10\x43HROME_EXTENSION\x10\x05\x12\x0c\n\x08INTERNAL\x10\x06\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.kb_usage_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_PROCESS'].fields_by_name['media_files']._options = None
  _globals['_PROCESS'].fields_by_name['media_files']._serialized_options = b'\030\001'
  _globals['_KBSOURCE']._serialized_start=1193
  _globals['_KBSOURCE']._serialized_end=1227
  _globals['_SERVICE']._serialized_start=1229
  _globals['_SERVICE']._serialized_end=1292
  _globals['_SEARCHTYPE']._serialized_start=1294
  _globals['_SEARCHTYPE']._serialized_end=1331
  _globals['_PREDICTTYPE']._serialized_start=1334
  _globals['_PREDICTTYPE']._serialized_end=1615
  _globals['_CLIENTTYPE']._serialized_start=1617
  _globals['_CLIENTTYPE']._serialized_end=1723
  _globals['_PROCESS']._serialized_start=78
  _globals['_PROCESS']._serialized_end=320
  _globals['_STORAGE']._serialized_start=323
  _globals['_STORAGE']._serialized_end=472
  _globals['_SEARCH']._serialized_start=474
  _globals['_SEARCH']._serialized_end=594
  _globals['_PREDICT']._serialized_start=597
  _globals['_PREDICT']._serialized_end=764
  _globals['_KBUSAGE']._serialized_start=767
  _globals['_KBUSAGE']._serialized_end=1132
  _globals['_KBUSAGEAGGREGATED']._serialized_start=1134
  _globals['_KBUSAGEAGGREGATED']._serialized_end=1191
# @@protoc_insertion_point(module_scope)
