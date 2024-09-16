# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/audit.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from nucliadb_protos import nodereader_pb2 as nucliadb__protos_dot_nodereader__pb2
try:
  nucliadb__protos_dot_noderesources__pb2 = nucliadb__protos_dot_nodereader__pb2.nucliadb__protos_dot_noderesources__pb2
except AttributeError:
  nucliadb__protos_dot_noderesources__pb2 = nucliadb__protos_dot_nodereader__pb2.nucliadb_protos.noderesources_pb2
try:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_nodereader__pb2.nucliadb__protos_dot_utils__pb2
except AttributeError:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_nodereader__pb2.nucliadb_protos.utils_pb2
try:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_nodereader__pb2.nucliadb__protos_dot_utils__pb2
except AttributeError:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_nodereader__pb2.nucliadb_protos.utils_pb2
from nucliadb_protos import resources_pb2 as nucliadb__protos_dot_resources__pb2
try:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_resources__pb2.nucliadb__protos_dot_utils__pb2
except AttributeError:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_resources__pb2.nucliadb_protos.utils_pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1bnucliadb_protos/audit.proto\x12\x05\x61udit\x1a\x1fgoogle/protobuf/timestamp.proto\x1a nucliadb_protos/nodereader.proto\x1a\x1fnucliadb_protos/resources.proto\"\xe4\x01\n\nAuditField\x12-\n\x06\x61\x63tion\x18\x01 \x01(\x0e\x32\x1d.audit.AuditField.FieldAction\x12\x0c\n\x04size\x18\x02 \x01(\x05\x12\x16\n\nsize_delta\x18\x03 \x01(\x05\x42\x02\x18\x01\x12\x10\n\x08\x66ield_id\x18\x04 \x01(\t\x12(\n\nfield_type\x18\x05 \x01(\x0e\x32\x14.resources.FieldType\x12\x10\n\x08\x66ilename\x18\x06 \x01(\t\"3\n\x0b\x46ieldAction\x12\t\n\x05\x41\x44\x44\x45\x44\x10\x00\x12\x0c\n\x08MODIFIED\x10\x01\x12\x0b\n\x07\x44\x45LETED\x10\x02\"4\n\x0e\x41uditKBCounter\x12\x12\n\nparagraphs\x18\x02 \x01(\x03\x12\x0e\n\x06\x66ields\x18\x03 \x01(\x03\"+\n\x0b\x43hatContext\x12\x0e\n\x06\x61uthor\x18\x01 \x01(\t\x12\x0c\n\x04text\x18\x02 \x01(\t\"7\n\x10RetrievedContext\x12\x15\n\rtext_block_id\x18\x01 \x01(\t\x12\x0c\n\x04text\x18\x02 \x01(\t\"\xb5\x02\n\tChatAudit\x12\x10\n\x08question\x18\x01 \x01(\t\x12\x13\n\x06\x61nswer\x18\x02 \x01(\tH\x00\x88\x01\x01\x12\x1f\n\x12rephrased_question\x18\x03 \x01(\tH\x01\x88\x01\x01\x12\'\n\x07\x63ontext\x18\x04 \x03(\x0b\x32\x12.audit.ChatContextB\x02\x18\x01\x12(\n\x0c\x63hat_context\x18\x06 \x03(\x0b\x32\x12.audit.ChatContext\x12\x32\n\x11retrieved_context\x18\x08 \x03(\x0b\x32\x17.audit.RetrievedContext\x12\x13\n\x0blearning_id\x18\x05 \x01(\t\x12\x13\n\x0bstatus_code\x18\t \x01(\x05\x12\r\n\x05model\x18\n \x01(\tB\t\n\x07_answerB\x15\n\x13_rephrased_question\"u\n\rFeedbackAudit\x12\x13\n\x0blearning_id\x18\x01 \x01(\t\x12\x0c\n\x04good\x18\x02 \x01(\x08\x12\x1d\n\x04task\x18\x03 \x01(\x0e\x32\x0f.audit.TaskType\x12\x15\n\x08\x66\x65\x65\x64\x62\x61\x63k\x18\x04 \x01(\tH\x00\x88\x01\x01\x42\x0b\n\t_feedback\"\x9b\x08\n\x0c\x41uditRequest\x12+\n\x04type\x18\x01 \x01(\x0e\x32\x1d.audit.AuditRequest.AuditType\x12\x0c\n\x04kbid\x18\x02 \x01(\t\x12\x0e\n\x06userid\x18\x04 \x01(\t\x12(\n\x04time\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0e\n\x06\x66ields\x18\x06 \x03(\t\x12)\n\x06search\x18\x07 \x01(\x0b\x32\x19.nodereader.SearchRequest\x12\x12\n\x06timeit\x18\x08 \x01(\x02\x42\x02\x18\x01\x12\x0e\n\x06origin\x18\t \x01(\t\x12\x0b\n\x03rid\x18\n \x01(\t\x12\x0c\n\x04task\x18\x0b \x01(\t\x12\x11\n\tresources\x18\x0c \x01(\x05\x12*\n\x0e\x66ield_metadata\x18\r \x03(\x0b\x32\x12.resources.FieldID\x12\'\n\x0c\x66ields_audit\x18\x0e \x03(\x0b\x32\x11.audit.AuditField\x12&\n\x0b\x63lient_type\x18\x10 \x01(\x0e\x32\x11.audit.ClientType\x12\x10\n\x08trace_id\x18\x11 \x01(\t\x12)\n\nkb_counter\x18\x12 \x01(\x0b\x32\x15.audit.AuditKBCounter\x12\x1e\n\x04\x63hat\x18\x13 \x01(\x0b\x32\x10.audit.ChatAudit\x12\x0f\n\x07success\x18\x14 \x01(\x08\x12\x14\n\x0crequest_time\x18\x15 \x01(\x02\x12\x1b\n\x0eretrieval_time\x18\x16 \x01(\x02H\x00\x88\x01\x01\x12#\n\x16generative_answer_time\x18\x17 \x01(\x02H\x01\x88\x01\x01\x12/\n\"generative_answer_first_chunk_time\x18\x18 \x01(\x02H\x02\x88\x01\x01\x12\x1a\n\rrephrase_time\x18\x19 \x01(\x02H\x03\x88\x01\x01\x12&\n\x08\x66\x65\x65\x64\x62\x61\x63k\x18\x1a \x01(\x0b\x32\x14.audit.FeedbackAudit\x12\x18\n\x0braw_request\x18\x1b \x01(\tH\x04\x88\x01\x01\"\xbf\x01\n\tAuditType\x12\x0b\n\x07VISITED\x10\x00\x12\x0c\n\x08MODIFIED\x10\x01\x12\x0b\n\x07\x44\x45LETED\x10\x02\x12\x07\n\x03NEW\x10\x03\x12\x0b\n\x07STARTED\x10\x04\x12\x0b\n\x07STOPPED\x10\x05\x12\n\n\x06SEARCH\x10\x06\x12\r\n\tPROCESSED\x10\x07\x12\x12\n\nKB_DELETED\x10\x08\x1a\x02\x08\x01\x12\x0f\n\x07SUGGEST\x10\t\x1a\x02\x08\x01\x12\x0f\n\x07INDEXED\x10\n\x1a\x02\x08\x01\x12\x08\n\x04\x43HAT\x10\x0b\x12\x0c\n\x08\x46\x45\x45\x44\x42\x41\x43K\x10\x0c\x42\x11\n\x0f_retrieval_timeB\x19\n\x17_generative_answer_timeB%\n#_generative_answer_first_chunk_timeB\x10\n\x0e_rephrase_timeB\x0e\n\x0c_raw_request*\\\n\nClientType\x12\x07\n\x03\x41PI\x10\x00\x12\x07\n\x03WEB\x10\x01\x12\n\n\x06WIDGET\x10\x02\x12\x0b\n\x07\x44\x45SKTOP\x10\x03\x12\r\n\tDASHBOARD\x10\x04\x12\x14\n\x10\x43HROME_EXTENSION\x10\x05*\x14\n\x08TaskType\x12\x08\n\x04\x43HAT\x10\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.audit_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_AUDITFIELD'].fields_by_name['size_delta']._options = None
  _globals['_AUDITFIELD'].fields_by_name['size_delta']._serialized_options = b'\030\001'
  _globals['_CHATAUDIT'].fields_by_name['context']._options = None
  _globals['_CHATAUDIT'].fields_by_name['context']._serialized_options = b'\030\001'
  _globals['_AUDITREQUEST_AUDITTYPE'].values_by_name["KB_DELETED"]._options = None
  _globals['_AUDITREQUEST_AUDITTYPE'].values_by_name["KB_DELETED"]._serialized_options = b'\010\001'
  _globals['_AUDITREQUEST_AUDITTYPE'].values_by_name["SUGGEST"]._options = None
  _globals['_AUDITREQUEST_AUDITTYPE'].values_by_name["SUGGEST"]._serialized_options = b'\010\001'
  _globals['_AUDITREQUEST_AUDITTYPE'].values_by_name["INDEXED"]._options = None
  _globals['_AUDITREQUEST_AUDITTYPE'].values_by_name["INDEXED"]._serialized_options = b'\010\001'
  _globals['_AUDITREQUEST'].fields_by_name['timeit']._options = None
  _globals['_AUDITREQUEST'].fields_by_name['timeit']._serialized_options = b'\030\001'
  _globals['_CLIENTTYPE']._serialized_start=2010
  _globals['_CLIENTTYPE']._serialized_end=2102
  _globals['_TASKTYPE']._serialized_start=2104
  _globals['_TASKTYPE']._serialized_end=2124
  _globals['_AUDITFIELD']._serialized_start=139
  _globals['_AUDITFIELD']._serialized_end=367
  _globals['_AUDITFIELD_FIELDACTION']._serialized_start=316
  _globals['_AUDITFIELD_FIELDACTION']._serialized_end=367
  _globals['_AUDITKBCOUNTER']._serialized_start=369
  _globals['_AUDITKBCOUNTER']._serialized_end=421
  _globals['_CHATCONTEXT']._serialized_start=423
  _globals['_CHATCONTEXT']._serialized_end=466
  _globals['_RETRIEVEDCONTEXT']._serialized_start=468
  _globals['_RETRIEVEDCONTEXT']._serialized_end=523
  _globals['_CHATAUDIT']._serialized_start=526
  _globals['_CHATAUDIT']._serialized_end=835
  _globals['_FEEDBACKAUDIT']._serialized_start=837
  _globals['_FEEDBACKAUDIT']._serialized_end=954
  _globals['_AUDITREQUEST']._serialized_start=957
  _globals['_AUDITREQUEST']._serialized_end=2008
  _globals['_AUDITREQUEST_AUDITTYPE']._serialized_start=1698
  _globals['_AUDITREQUEST_AUDITTYPE']._serialized_end=1889
# @@protoc_insertion_point(module_scope)
