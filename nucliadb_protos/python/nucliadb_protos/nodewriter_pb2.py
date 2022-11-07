# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/nodewriter.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from nucliadb_protos import noderesources_pb2 as nucliadb__protos_dot_noderesources__pb2
try:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_noderesources__pb2.nucliadb__protos_dot_utils__pb2
except AttributeError:
  nucliadb__protos_dot_utils__pb2 = nucliadb__protos_dot_noderesources__pb2.nucliadb_protos.utils_pb2

from nucliadb_protos.noderesources_pb2 import *

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n nucliadb_protos/nodewriter.proto\x12\nnodewriter\x1a#nucliadb_protos/noderesources.proto\"\x92\x01\n\x08OpStatus\x12+\n\x06status\x18\x01 \x01(\x0e\x32\x1b.nodewriter.OpStatus.Status\x12\x0e\n\x06\x64\x65tail\x18\x02 \x01(\t\x12\r\n\x05\x63ount\x18\x03 \x01(\x04\x12\x10\n\x08shard_id\x18\x04 \x01(\t\"(\n\x06Status\x12\x06\n\x02OK\x10\x00\x12\x0b\n\x07WARNING\x10\x01\x12\t\n\x05\x45RROR\x10\x02\"\xc5\x01\n\x0cIndexMessage\x12\x0c\n\x04node\x18\x01 \x01(\t\x12\r\n\x05shard\x18\x02 \x01(\t\x12\x0c\n\x04txid\x18\x03 \x01(\x04\x12\x10\n\x08resource\x18\x04 \x01(\t\x12\x39\n\x0btypemessage\x18\x05 \x01(\x0e\x32$.nodewriter.IndexMessage.TypeMessage\x12\x12\n\nreindex_id\x18\x06 \x01(\t\")\n\x0bTypeMessage\x12\x0c\n\x08\x43REATION\x10\x00\x12\x0c\n\x08\x44\x45LETION\x10\x01\"\x1c\n\x07\x43ounter\x12\x11\n\tresources\x18\x01 \x01(\x04\x32\xa6\x04\n\nNodeWriter\x12<\n\x08GetShard\x12\x16.noderesources.ShardId\x1a\x16.noderesources.ShardId\"\x00\x12\x44\n\x08NewShard\x12\x19.noderesources.EmptyQuery\x1a\x1b.noderesources.ShardCreated\"\x00\x12L\n\x13UpdateAndCleanShard\x12\x16.noderesources.ShardId\x1a\x1b.noderesources.ShardCleaned\"\x00\x12?\n\x0b\x44\x65leteShard\x12\x16.noderesources.ShardId\x1a\x16.noderesources.ShardId\"\x00\x12\x42\n\nListShards\x12\x19.noderesources.EmptyQuery\x1a\x17.noderesources.ShardIds\"\x00\x12<\n\x02GC\x12\x16.noderesources.ShardId\x1a\x1c.noderesources.EmptyResponse\"\x00\x12>\n\x0bSetResource\x12\x17.noderesources.Resource\x1a\x14.nodewriter.OpStatus\"\x00\x12\x43\n\x0eRemoveResource\x12\x19.noderesources.ResourceID\x1a\x14.nodewriter.OpStatus\"\x00\x32H\n\x0bNodeSidecar\x12\x39\n\x08GetCount\x12\x16.noderesources.ShardId\x1a\x13.nodewriter.Counter\"\x00P\x00\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.nodewriter_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _OPSTATUS._serialized_start=86
  _OPSTATUS._serialized_end=232
  _OPSTATUS_STATUS._serialized_start=192
  _OPSTATUS_STATUS._serialized_end=232
  _INDEXMESSAGE._serialized_start=235
  _INDEXMESSAGE._serialized_end=432
  _INDEXMESSAGE_TYPEMESSAGE._serialized_start=391
  _INDEXMESSAGE_TYPEMESSAGE._serialized_end=432
  _COUNTER._serialized_start=434
  _COUNTER._serialized_end=462
  _NODEWRITER._serialized_start=465
  _NODEWRITER._serialized_end=1015
  _NODESIDECAR._serialized_start=1017
  _NODESIDECAR._serialized_end=1089
# @@protoc_insertion_point(module_scope)
