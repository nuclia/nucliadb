# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/unified.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1dnucliadb_protos/unified.proto\x12\x07unified\">\n\x18ShardReplicationPosition\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\x10\n\x08position\x18\x02 \x01(\x04\"g\n\x19SecondaryReplicateRequest\x12\x14\n\x0csecondary_id\x18\x01 \x01(\t\x12\x34\n\tpositions\x18\x02 \x03(\x0b\x32!.unified.ShardReplicationPosition\"N\n\x16PrimaryReplicateCommit\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\x10\n\x08position\x18\x02 \x01(\x04\x12\x10\n\x08segments\x18\x03 \x03(\t\"L\n\x18PrimaryReplicateResponse\x12\x30\n\x07\x63ommits\x18\x01 \x03(\x0b\x32\x1f.unified.PrimaryReplicateCommit\"R\n\x16\x44ownloadSegmentRequest\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\x12\n\nsegment_id\x18\x02 \x01(\t\x12\x12\n\nchunk_size\x18\x03 \x01(\x04\"a\n\x17\x44ownloadSegmentResponse\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\x0c\x12\r\n\x05\x63hunk\x18\x02 \x01(\r\x12\x15\n\rread_position\x18\x03 \x01(\x04\x12\x12\n\ntotal_size\x18\x04 \x01(\x04\"&\n\x12\x43reateShardRequest\x12\x10\n\x08shard_id\x18\x01 \x01(\t\"\x15\n\x13\x43reateShardResponse\"&\n\x12\x44\x65leteShardRequest\x12\x10\n\x08shard_id\x18\x01 \x01(\t\"\x15\n\x13\x44\x65leteShardResponse\"_\n\rSearchRequest\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\r\n\x05query\x18\x02 \x01(\t\x12\x0e\n\x06vector\x18\x03 \x03(\x02\x12\r\n\x05limit\x18\x04 \x01(\x04\x12\x0e\n\x06offset\x18\x05 \x01(\x04\"V\n\nResultItem\x12\x13\n\x0bresource_id\x18\x01 \x01(\t\x12\x10\n\x08\x66ield_id\x18\x02 \x01(\t\x12\r\n\x05score\x18\x03 \x01(\x02\x12\x12\n\nscore_type\x18\x04 \x01(\t\"C\n\x0eSearchResponse\x12\"\n\x05items\x18\x01 \x03(\x0b\x32\x13.unified.ResultItem\x12\r\n\x05total\x18\x02 \x01(\x04\"5\n\x0cIndexRequest\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\x13\n\x0bresource_id\x18\x02 \x01(\t\"\x0f\n\rIndexResponse\"6\n\rDeleteRequest\x12\x10\n\x08shard_id\x18\x01 \x01(\t\x12\x13\n\x0bresource_id\x18\x02 \x01(\t\"\x10\n\x0e\x44\x65leteResponse2\x8d\x04\n\x0bNodeService\x12X\n\tReplicate\x12\".unified.SecondaryReplicateRequest\x1a!.unified.PrimaryReplicateResponse\"\x00(\x01\x30\x01\x12X\n\x0f\x44ownloadSegment\x12\x1f.unified.DownloadSegmentRequest\x1a .unified.DownloadSegmentResponse\"\x00\x30\x01\x12J\n\x0b\x43reateShard\x12\x1b.unified.CreateShardRequest\x1a\x1c.unified.CreateShardResponse\"\x00\x12J\n\x0b\x44\x65leteShard\x12\x1b.unified.DeleteShardRequest\x1a\x1c.unified.DeleteShardResponse\"\x00\x12\x38\n\x05Index\x12\x15.unified.IndexRequest\x1a\x16.unified.IndexResponse\"\x00\x12;\n\x06\x44\x65lete\x12\x16.unified.DeleteRequest\x1a\x17.unified.DeleteResponse\"\x00\x12;\n\x06Search\x12\x16.unified.SearchRequest\x1a\x17.unified.SearchResponse\"\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.unified_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _globals['_SHARDREPLICATIONPOSITION']._serialized_start=42
  _globals['_SHARDREPLICATIONPOSITION']._serialized_end=104
  _globals['_SECONDARYREPLICATEREQUEST']._serialized_start=106
  _globals['_SECONDARYREPLICATEREQUEST']._serialized_end=209
  _globals['_PRIMARYREPLICATECOMMIT']._serialized_start=211
  _globals['_PRIMARYREPLICATECOMMIT']._serialized_end=289
  _globals['_PRIMARYREPLICATERESPONSE']._serialized_start=291
  _globals['_PRIMARYREPLICATERESPONSE']._serialized_end=367
  _globals['_DOWNLOADSEGMENTREQUEST']._serialized_start=369
  _globals['_DOWNLOADSEGMENTREQUEST']._serialized_end=451
  _globals['_DOWNLOADSEGMENTRESPONSE']._serialized_start=453
  _globals['_DOWNLOADSEGMENTRESPONSE']._serialized_end=550
  _globals['_CREATESHARDREQUEST']._serialized_start=552
  _globals['_CREATESHARDREQUEST']._serialized_end=590
  _globals['_CREATESHARDRESPONSE']._serialized_start=592
  _globals['_CREATESHARDRESPONSE']._serialized_end=613
  _globals['_DELETESHARDREQUEST']._serialized_start=615
  _globals['_DELETESHARDREQUEST']._serialized_end=653
  _globals['_DELETESHARDRESPONSE']._serialized_start=655
  _globals['_DELETESHARDRESPONSE']._serialized_end=676
  _globals['_SEARCHREQUEST']._serialized_start=678
  _globals['_SEARCHREQUEST']._serialized_end=773
  _globals['_RESULTITEM']._serialized_start=775
  _globals['_RESULTITEM']._serialized_end=861
  _globals['_SEARCHRESPONSE']._serialized_start=863
  _globals['_SEARCHRESPONSE']._serialized_end=930
  _globals['_INDEXREQUEST']._serialized_start=932
  _globals['_INDEXREQUEST']._serialized_end=985
  _globals['_INDEXRESPONSE']._serialized_start=987
  _globals['_INDEXRESPONSE']._serialized_end=1002
  _globals['_DELETEREQUEST']._serialized_start=1004
  _globals['_DELETEREQUEST']._serialized_end=1058
  _globals['_DELETERESPONSE']._serialized_start=1060
  _globals['_DELETERESPONSE']._serialized_end=1076
  _globals['_NODESERVICE']._serialized_start=1079
  _globals['_NODESERVICE']._serialized_end=1604
# @@protoc_insertion_point(module_scope)
