# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/backups.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1dnucliadb_protos/backups.proto\x12\x07\x62\x61\x63kups\x1a\x1fgoogle/protobuf/timestamp.proto\"6\n\x13\x43reateBackupRequest\x12\x11\n\tbackup_id\x18\x01 \x01(\t\x12\x0c\n\x04kbid\x18\x02 \x01(\t\"{\n\x14\x43reateBackupResponse\x12\x34\n\x06status\x18\x01 \x01(\x0e\x32$.backups.CreateBackupResponse.Status\"-\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\t\n\x05\x45RROR\x10\x01\x12\x10\n\x0cKB_NOT_FOUND\x10\x02\"6\n\x13\x44\x65leteBackupRequest\x12\x11\n\tbackup_id\x18\x01 \x01(\t\x12\x0c\n\x04kbid\x18\x02 \x01(\t\"i\n\x14\x44\x65leteBackupResponse\x12\x34\n\x06status\x18\x01 \x01(\x0e\x32$.backups.DeleteBackupResponse.Status\"\x1b\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\t\n\x05\x45RROR\x10\x01\"7\n\x14RestoreBackupRequest\x12\x11\n\tbackup_id\x18\x01 \x01(\t\x12\x0c\n\x04kbid\x18\x02 \x01(\t\"z\n\x15RestoreBackupResponse\x12\x35\n\x06status\x18\x01 \x01(\x0e\x32%.backups.RestoreBackupResponse.Status\"*\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\t\n\x05\x45RROR\x10\x01\x12\r\n\tNOT_FOUND\x10\x02\"\x8e\x01\n\x19\x42\x61\x63kupCreatedNotification\x12/\n\x0b\x66inished_at\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x11\n\tbackup_id\x18\x02 \x01(\t\x12\x0c\n\x04kbid\x18\x03 \x01(\t\x12\x0c\n\x04size\x18\x04 \x01(\x04\x12\x11\n\tresources\x18\x05 \x01(\x04\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.backups_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_CREATEBACKUPREQUEST']._serialized_start=75
  _globals['_CREATEBACKUPREQUEST']._serialized_end=129
  _globals['_CREATEBACKUPRESPONSE']._serialized_start=131
  _globals['_CREATEBACKUPRESPONSE']._serialized_end=254
  _globals['_CREATEBACKUPRESPONSE_STATUS']._serialized_start=209
  _globals['_CREATEBACKUPRESPONSE_STATUS']._serialized_end=254
  _globals['_DELETEBACKUPREQUEST']._serialized_start=256
  _globals['_DELETEBACKUPREQUEST']._serialized_end=310
  _globals['_DELETEBACKUPRESPONSE']._serialized_start=312
  _globals['_DELETEBACKUPRESPONSE']._serialized_end=417
  _globals['_DELETEBACKUPRESPONSE_STATUS']._serialized_start=209
  _globals['_DELETEBACKUPRESPONSE_STATUS']._serialized_end=236
  _globals['_RESTOREBACKUPREQUEST']._serialized_start=419
  _globals['_RESTOREBACKUPREQUEST']._serialized_end=474
  _globals['_RESTOREBACKUPRESPONSE']._serialized_start=476
  _globals['_RESTOREBACKUPRESPONSE']._serialized_end=598
  _globals['_RESTOREBACKUPRESPONSE_STATUS']._serialized_start=556
  _globals['_RESTOREBACKUPRESPONSE_STATUS']._serialized_end=598
  _globals['_BACKUPCREATEDNOTIFICATION']._serialized_start=601
  _globals['_BACKUPCREATEDNOTIFICATION']._serialized_end=743
# @@protoc_insertion_point(module_scope)
