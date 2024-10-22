# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: nucliadb_protos/resources.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from nucliadb_protos import utils_pb2 as nucliadb__protos_dot_utils__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import struct_pb2 as google_dot_protobuf_dot_struct__pb2

from nucliadb_protos.utils_pb2 import *

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1fnucliadb_protos/resources.proto\x12\tresources\x1a\x1bnucliadb_protos/utils.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1cgoogle/protobuf/struct.proto\"\xfb\x02\n\tCloudFile\x12\x0b\n\x03uri\x18\x01 \x01(\t\x12\x0c\n\x04size\x18\x02 \x01(\x04\x12\x14\n\x0c\x63ontent_type\x18\x03 \x01(\t\x12\x13\n\x0b\x62ucket_name\x18\x04 \x01(\t\x12+\n\x06source\x18\x05 \x01(\x0e\x32\x1b.resources.CloudFile.Source\x12\x10\n\x08\x66ilename\x18\x06 \x01(\t\x12\x15\n\rresumable_uri\x18\x07 \x01(\t\x12\x0e\n\x06offset\x18\x08 \x01(\x04\x12\x12\n\nupload_uri\x18\t \x01(\t\x12\r\n\x05parts\x18\n \x03(\t\x12\x0f\n\x07old_uri\x18\x0b \x01(\t\x12\x12\n\nold_bucket\x18\x0c \x01(\t\x12\x0b\n\x03md5\x18\r \x01(\t\"m\n\x06Source\x12\t\n\x05\x46LAPS\x10\x00\x12\x07\n\x03GCS\x10\x01\x12\x06\n\x02S3\x10\x02\x12\t\n\x05LOCAL\x10\x03\x12\x0c\n\x08\x45XTERNAL\x10\x04\x12\t\n\x05\x45MPTY\x10\x05\x12\n\n\x06\x45XPORT\x10\x06\x12\x0c\n\x08POSTGRES\x10\x07\x12\t\n\x05\x41ZURE\x10\x08\"\xc4\x04\n\x05\x42\x61sic\x12\x0c\n\x04slug\x18\x01 \x01(\t\x12\x0c\n\x04icon\x18\x02 \x01(\t\x12\r\n\x05title\x18\x03 \x01(\t\x12\x0f\n\x07summary\x18\x04 \x01(\t\x12\x11\n\tthumbnail\x18\x05 \x01(\t\x12\x12\n\x06layout\x18\x06 \x01(\tB\x02\x18\x01\x12+\n\x07\x63reated\x18\x07 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12,\n\x08modified\x18\x08 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12%\n\x08metadata\x18\t \x01(\x0b\x32\x13.resources.Metadata\x12-\n\x0cusermetadata\x18\n \x01(\x0b\x32\x17.resources.UserMetadata\x12\x33\n\rfieldmetadata\x18\x0b \x03(\x0b\x32\x1c.resources.UserFieldMetadata\x12\x35\n\x10\x63omputedmetadata\x18\x0f \x01(\x0b\x32\x1b.resources.ComputedMetadata\x12\x0c\n\x04uuid\x18\x0c \x01(\t\x12\x0e\n\x06labels\x18\r \x03(\t\x12\x12\n\nlast_seqid\x18\x0e \x01(\x03\x12\x18\n\x10last_account_seq\x18# \x01(\x03\x12)\n\x05queue\x18$ \x01(\x0e\x32\x1a.resources.Basic.QueueType\x12\x13\n\x06hidden\x18% \x01(\x08H\x00\x88\x01\x01\"$\n\tQueueType\x12\x0b\n\x07PRIVATE\x10\x00\x12\n\n\x06SHARED\x10\x01\x42\t\n\x07_hidden\"\x8f\x03\n\x06Origin\x12(\n\x06source\x18\x01 \x01(\x0e\x32\x18.resources.Origin.Source\x12\x11\n\tsource_id\x18\x02 \x01(\t\x12\x0b\n\x03url\x18\x03 \x01(\t\x12+\n\x07\x63reated\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12,\n\x08modified\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x31\n\x08metadata\x18\x06 \x03(\x0b\x32\x1f.resources.Origin.MetadataEntry\x12\x0c\n\x04tags\x18\x07 \x03(\t\x12\x14\n\x0c\x63olaborators\x18\x08 \x03(\t\x12\x10\n\x08\x66ilename\x18\t \x01(\t\x12\x0f\n\x07related\x18\n \x03(\t\x12\x0c\n\x04path\x18\x0b \x01(\t\x1a/\n\rMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\'\n\x06Source\x12\x07\n\x03WEB\x10\x00\x12\x0b\n\x07\x44\x45SKTOP\x10\x01\x12\x07\n\x03\x41PI\x10\x02\"2\n\x05\x45xtra\x12)\n\x08metadata\x18\x01 \x01(\x0b\x32\x17.google.protobuf.Struct\"/\n\tRelations\x12\"\n\trelations\x18\x01 \x03(\x0b\x32\x0f.utils.Relation\"\xc4\x01\n\x0eMessageContent\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x30\n\x06\x66ormat\x18\x02 \x01(\x0e\x32 .resources.MessageContent.Format\x12)\n\x0b\x61ttachments\x18\x04 \x03(\x0b\x32\x14.resources.CloudFile\"G\n\x06\x46ormat\x12\t\n\x05PLAIN\x10\x00\x12\x08\n\x04HTML\x10\x01\x12\x0c\n\x08MARKDOWN\x10\x02\x12\x07\n\x03RST\x10\x03\x12\x11\n\rKEEP_MARKDOWN\x10\x04\"\xee\x01\n\x07Message\x12-\n\ttimestamp\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0b\n\x03who\x18\x02 \x01(\t\x12\n\n\x02to\x18\x03 \x03(\t\x12*\n\x07\x63ontent\x18\x04 \x01(\x0b\x32\x19.resources.MessageContent\x12\r\n\x05ident\x18\x05 \x01(\t\x12,\n\x04type\x18\x06 \x01(\x0e\x32\x1e.resources.Message.MessageType\"2\n\x0bMessageType\x12\t\n\x05UNSET\x10\x00\x12\x0c\n\x08QUESTION\x10\x01\x12\n\n\x06\x41NSWER\x10\x02\"4\n\x0c\x43onversation\x12$\n\x08messages\x18\x01 \x03(\x0b\x32\x12.resources.Message\"?\n\x11\x46ieldConversation\x12\r\n\x05pages\x18\x01 \x01(\x05\x12\x0c\n\x04size\x18\x02 \x01(\x05\x12\r\n\x05total\x18\x03 \x01(\x05\":\n\x0eNestedPosition\x12\r\n\x05start\x18\x01 \x01(\x03\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x03\x12\x0c\n\x04page\x18\x03 \x01(\x03\"B\n\x12NestedListPosition\x12,\n\tpositions\x18\x01 \x03(\x0b\x32\x19.resources.NestedPosition\"\xc8\x08\n\x11\x46ileExtractedData\x12\x10\n\x08language\x18\x01 \x01(\t\x12\x0b\n\x03md5\x18\x02 \x01(\t\x12<\n\x08metadata\x18\x03 \x03(\x0b\x32*.resources.FileExtractedData.MetadataEntry\x12\x38\n\x06nested\x18\x04 \x03(\x0b\x32(.resources.FileExtractedData.NestedEntry\x12G\n\x0e\x66ile_generated\x18\x05 \x03(\x0b\x32/.resources.FileExtractedData.FileGeneratedEntry\x12N\n\x12\x66ile_rows_previews\x18\x06 \x03(\x0b\x32\x32.resources.FileExtractedData.FileRowsPreviewsEntry\x12*\n\x0c\x66ile_preview\x18\x07 \x01(\x0b\x32\x14.resources.CloudFile\x12\x31\n\x13\x66ile_pages_previews\x18\x08 \x01(\x0b\x32\x14.resources.FilePages\x12,\n\x0e\x66ile_thumbnail\x18\t \x01(\x0b\x32\x14.resources.CloudFile\x12\r\n\x05\x66ield\x18\n \x01(\t\x12\x0c\n\x04icon\x18\x0b \x01(\t\x12M\n\x0fnested_position\x18\x0c \x03(\x0b\x32\x30.resources.FileExtractedData.NestedPositionEntryB\x02\x18\x01\x12R\n\x14nested_list_position\x18\r \x03(\x0b\x32\x34.resources.FileExtractedData.NestedListPositionEntry\x12\r\n\x05title\x18\x0e \x01(\t\x1a/\n\rMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a-\n\x0bNestedEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1aJ\n\x12\x46ileGeneratedEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.resources.CloudFile:\x02\x38\x01\x1aO\n\x15\x46ileRowsPreviewsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12%\n\x05value\x18\x02 \x01(\x0b\x32\x16.resources.RowsPreview:\x02\x38\x01\x1aP\n\x13NestedPositionEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12(\n\x05value\x18\x02 \x01(\x0b\x32\x19.resources.NestedPosition:\x02\x38\x01\x1aX\n\x17NestedListPositionEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.resources.NestedListPosition:\x02\x38\x01\"\xd8\x04\n\x11LinkExtractedData\x12(\n\x04\x64\x61te\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x10\n\x08language\x18\x02 \x01(\t\x12\r\n\x05title\x18\x04 \x01(\t\x12<\n\x08metadata\x18\x05 \x03(\x0b\x32*.resources.LinkExtractedData.MetadataEntry\x12,\n\x0elink_thumbnail\x18\x06 \x01(\x0b\x32\x14.resources.CloudFile\x12*\n\x0clink_preview\x18\x07 \x01(\x0b\x32\x14.resources.CloudFile\x12\r\n\x05\x66ield\x18\x08 \x01(\t\x12(\n\nlink_image\x18\t \x01(\x0b\x32\x14.resources.CloudFile\x12\x13\n\x0b\x64\x65scription\x18\n \x01(\t\x12\x0c\n\x04type\x18\x0b \x01(\t\x12\r\n\x05\x65mbed\x18\x0c \x01(\t\x12/\n\rpdf_structure\x18\r \x01(\x0b\x32\x18.resources.PageStructure\x12G\n\x0e\x66ile_generated\x18\x0e \x03(\x0b\x32/.resources.LinkExtractedData.FileGeneratedEntry\x1a/\n\rMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1aJ\n\x12\x46ileGeneratedEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.resources.CloudFile:\x02\x38\x01\"\x95\x01\n\x14\x45xtractedTextWrapper\x12$\n\x04\x62ody\x18\x01 \x01(\x0b\x32\x14.utils.ExtractedTextH\x00\x12$\n\x04\x66ile\x18\x02 \x01(\x0b\x32\x14.resources.CloudFileH\x00\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldIDB\x0e\n\x0c\x66ile_or_data\"\xb0\x01\n\x17\x45xtractedVectorsWrapper\x12&\n\x07vectors\x18\x01 \x01(\x0b\x32\x13.utils.VectorObjectH\x00\x12$\n\x04\x66ile\x18\x02 \x01(\x0b\x32\x14.resources.CloudFileH\x00\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldID\x12\x14\n\x0cvectorset_id\x18\x04 \x01(\tB\x0e\n\x0c\x66ile_or_data\"\xfd\x01\n\x12UserVectorsWrapper\x12%\n\x07vectors\x18\x01 \x01(\x0b\x32\x14.utils.UserVectorSet\x12M\n\x11vectors_to_delete\x18\r \x03(\x0b\x32\x32.resources.UserVectorsWrapper.VectorsToDeleteEntry\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldID\x1aN\n\x14VectorsToDeleteEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12%\n\x05value\x18\x02 \x01(\x0b\x32\x16.utils.UserVectorsList:\x02\x38\x01\"3\n\x08Sentence\x12\r\n\x05start\x18\x01 \x01(\x05\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x05\x12\x0b\n\x03key\x18\x03 \x01(\t\"9\n\x0fPageInformation\x12\x0c\n\x04page\x18\x01 \x01(\r\x12\x18\n\x10page_with_visual\x18\x02 \x01(\x08\"<\n\x0eRepresentation\x12\x12\n\nis_a_table\x18\x01 \x01(\x08\x12\x16\n\x0ereference_file\x18\x02 \x01(\t\"M\n\x12ParagraphRelations\x12\x0f\n\x07parents\x18\x01 \x03(\t\x12\x10\n\x08siblings\x18\x02 \x03(\t\x12\x14\n\x0creplacements\x18\x03 \x03(\t\"\xf5\x03\n\tParagraph\x12\r\n\x05start\x18\x01 \x01(\r\x12\x0b\n\x03\x65nd\x18\x02 \x01(\r\x12\x15\n\rstart_seconds\x18\x03 \x03(\r\x12\x13\n\x0b\x65nd_seconds\x18\x04 \x03(\r\x12\x30\n\x04kind\x18\x05 \x01(\x0e\x32\".resources.Paragraph.TypeParagraph\x12\x32\n\x0f\x63lassifications\x18\x06 \x03(\x0b\x32\x19.resources.Classification\x12&\n\tsentences\x18\x07 \x03(\x0b\x32\x13.resources.Sentence\x12\x0b\n\x03key\x18\x08 \x01(\t\x12\x0c\n\x04text\x18\t \x01(\t\x12(\n\x04page\x18\n \x01(\x0b\x32\x1a.resources.PageInformation\x12\x31\n\x0erepresentation\x18\x0b \x01(\x0b\x32\x19.resources.Representation\x12\x30\n\trelations\x18\x0c \x01(\x0b\x32\x1d.resources.ParagraphRelations\"h\n\rTypeParagraph\x12\x08\n\x04TEXT\x10\x00\x12\x07\n\x03OCR\x10\x01\x12\r\n\tINCEPTION\x10\x02\x12\x0f\n\x0b\x44\x45SCRIPTION\x10\x03\x12\x0e\n\nTRANSCRIPT\x10\x04\x12\t\n\x05TITLE\x10\x05\x12\t\n\x05TABLE\x10\x06\"&\n\x08Position\x12\r\n\x05start\x18\x01 \x01(\x03\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x03\"B\n\tPositions\x12%\n\x08position\x18\x01 \x03(\x0b\x32\x13.resources.Position\x12\x0e\n\x06\x65ntity\x18\x02 \x01(\t\"\xb0\x05\n\rFieldMetadata\x12\r\n\x05links\x18\x01 \x03(\t\x12(\n\nparagraphs\x18\x02 \x03(\x0b\x32\x14.resources.Paragraph\x12.\n\x03ner\x18\x03 \x03(\x0b\x32!.resources.FieldMetadata.NerEntry\x12\x32\n\x0f\x63lassifications\x18\x04 \x03(\x0b\x32\x19.resources.Classification\x12.\n\nlast_index\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x36\n\x12last_understanding\x18\x06 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x30\n\x0clast_extract\x18\x07 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x30\n\x0clast_summary\x18\x08 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\'\n\tthumbnail\x18\t \x01(\x0b\x32\x14.resources.CloudFile\x12\x10\n\x08language\x18\n \x01(\t\x12\x0f\n\x07summary\x18\x0b \x01(\t\x12:\n\tpositions\x18\x0c \x03(\x0b\x32\'.resources.FieldMetadata.PositionsEntry\x12\'\n\trelations\x18\r \x03(\x0b\x32\x14.resources.Relations\x12\x11\n\tmime_type\x18\x0e \x01(\t\x1a*\n\x08NerEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a\x46\n\x0ePositionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12#\n\x05value\x18\x02 \x01(\x0b\x32\x14.resources.Positions:\x02\x38\x01\"B\n\x08Question\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x10\n\x08language\x18\x02 \x01(\t\x12\x16\n\x0eids_paragraphs\x18\x03 \x03(\t\"A\n\x07\x41nswers\x12\x0c\n\x04text\x18\x01 \x01(\t\x12\x10\n\x08language\x18\x02 \x01(\t\x12\x16\n\x0eids_paragraphs\x18\x03 \x03(\t\"\\\n\x0eQuestionAnswer\x12%\n\x08question\x18\x01 \x01(\x0b\x32\x13.resources.Question\x12#\n\x07\x61nswers\x18\x02 \x03(\x0b\x32\x12.resources.Answers\"E\n\x0fQuestionAnswers\x12\x32\n\x0fquestion_answer\x18\x01 \x03(\x0b\x32\x19.resources.QuestionAnswer\"\x98\x02\n\x14\x46ieldQuestionAnswers\x12\x34\n\x10question_answers\x18\x01 \x01(\x0b\x32\x1a.resources.QuestionAnswers\x12Y\n\x16split_question_answers\x18\x02 \x03(\x0b\x32\x39.resources.FieldQuestionAnswers.SplitQuestionAnswersEntry\x12\x16\n\x0e\x64\x65leted_splits\x18\x03 \x03(\t\x1aW\n\x19SplitQuestionAnswersEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12)\n\x05value\x18\x02 \x01(\x0b\x32\x1a.resources.QuestionAnswers:\x02\x38\x01\"\xb2\x01\n\x1a\x46ieldQuestionAnswerWrapper\x12;\n\x10question_answers\x18\x01 \x01(\x0b\x32\x1f.resources.FieldQuestionAnswersH\x00\x12$\n\x04\x66ile\x18\x02 \x01(\x0b\x32\x14.resources.CloudFileH\x00\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldIDB\x0e\n\x0c\x66ile_or_data\"\xf8\x01\n\x15\x46ieldComputedMetadata\x12*\n\x08metadata\x18\x01 \x01(\x0b\x32\x18.resources.FieldMetadata\x12K\n\x0esplit_metadata\x18\x02 \x03(\x0b\x32\x33.resources.FieldComputedMetadata.SplitMetadataEntry\x12\x16\n\x0e\x64\x65leted_splits\x18\x03 \x03(\t\x1aN\n\x12SplitMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\'\n\x05value\x18\x02 \x01(\x0b\x32\x18.resources.FieldMetadata:\x02\x38\x01\"u\n\x1c\x46ieldComputedMetadataWrapper\x12\x32\n\x08metadata\x18\x01 \x01(\x0b\x32 .resources.FieldComputedMetadata\x12!\n\x05\x66ield\x18\x04 \x01(\x0b\x32\x12.resources.FieldID\"\x9c\x02\n\x08Metadata\x12\x33\n\x08metadata\x18\x01 \x03(\x0b\x32!.resources.Metadata.MetadataEntry\x12\x10\n\x08language\x18\x02 \x01(\t\x12\x11\n\tlanguages\x18\x03 \x03(\t\x12\x0e\n\x06useful\x18\x04 \x01(\x08\x12*\n\x06status\x18\x05 \x01(\x0e\x32\x1a.resources.Metadata.Status\x1a/\n\rMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"I\n\x06Status\x12\x0b\n\x07PENDING\x10\x00\x12\r\n\tPROCESSED\x10\x01\x12\t\n\x05\x45RROR\x10\x02\x12\x0b\n\x07\x42LOCKED\x10\x03\x12\x0b\n\x07\x45XPIRED\x10\x04\"\xa6\x01\n\tFieldText\x12\x0c\n\x04\x62ody\x18\x01 \x01(\t\x12+\n\x06\x66ormat\x18\x02 \x01(\x0e\x32\x1b.resources.FieldText.Format\x12\x0b\n\x03md5\x18\x03 \x01(\t\"Q\n\x06\x46ormat\x12\t\n\x05PLAIN\x10\x00\x12\x08\n\x04HTML\x10\x01\x12\x07\n\x03RST\x10\x02\x12\x0c\n\x08MARKDOWN\x10\x03\x12\x08\n\x04JSON\x10\x04\x12\x11\n\rKEEP_MARKDOWN\x10\x05\"\x9c\x02\n\x05\x42lock\x12\t\n\x01x\x18\x01 \x01(\x05\x12\t\n\x01y\x18\x02 \x01(\x05\x12\x0c\n\x04\x63ols\x18\x03 \x01(\x05\x12\x0c\n\x04rows\x18\x04 \x01(\x05\x12(\n\x04type\x18\x05 \x01(\x0e\x32\x1a.resources.Block.TypeBlock\x12\r\n\x05ident\x18\x06 \x01(\t\x12\x0f\n\x07payload\x18\x07 \x01(\t\x12\"\n\x04\x66ile\x18\x08 \x01(\x0b\x32\x14.resources.CloudFile\"s\n\tTypeBlock\x12\t\n\x05TITLE\x10\x00\x12\x0f\n\x0b\x44\x45SCRIPTION\x10\x01\x12\x0c\n\x08RICHTEXT\x10\x02\x12\x08\n\x04TEXT\x10\x03\x12\x0f\n\x0b\x41TTACHMENTS\x10\x04\x12\x0c\n\x08\x43OMMENTS\x10\x05\x12\x13\n\x0f\x43LASSIFICATIONS\x10\x06\"[\n\x0e\x43lassification\x12\x10\n\x08labelset\x18\x01 \x01(\t\x12\r\n\x05label\x18\x02 \x01(\t\x12\x19\n\x11\x63\x61ncelled_by_user\x18\x03 \x01(\x08\x12\r\n\x05split\x18\x04 \x01(\t\"f\n\x0cUserMetadata\x12\x32\n\x0f\x63lassifications\x18\x01 \x03(\x0b\x32\x19.resources.Classification\x12\"\n\trelations\x18\x03 \x03(\x0b\x32\x0f.utils.Relation\"m\n\x14\x46ieldClassifications\x12!\n\x05\x66ield\x18\x01 \x01(\x0b\x32\x12.resources.FieldID\x12\x32\n\x0f\x63lassifications\x18\x02 \x03(\x0b\x32\x19.resources.Classification\"R\n\x10\x43omputedMetadata\x12>\n\x15\x66ield_classifications\x18\x01 \x03(\x0b\x32\x1f.resources.FieldClassifications\"p\n\nTokenSplit\x12\r\n\x05token\x18\x01 \x01(\t\x12\r\n\x05klass\x18\x02 \x01(\t\x12\r\n\x05start\x18\x03 \x01(\r\x12\x0b\n\x03\x65nd\x18\x04 \x01(\r\x12\x19\n\x11\x63\x61ncelled_by_user\x18\x05 \x01(\x08\x12\r\n\x05split\x18\x06 \x01(\t\"V\n\x13ParagraphAnnotation\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x32\n\x0f\x63lassifications\x18\x02 \x03(\x0b\x32\x19.resources.Classification\"i\n\x18QuestionAnswerAnnotation\x12\x32\n\x0fquestion_answer\x18\x01 \x01(\x0b\x32\x19.resources.QuestionAnswer\x12\x19\n\x11\x63\x61ncelled_by_user\x18\x02 \x01(\x08\"m\n\x0fVisualSelection\x12\r\n\x05label\x18\x01 \x01(\t\x12\x0b\n\x03top\x18\x02 \x01(\x02\x12\x0c\n\x04left\x18\x03 \x01(\x02\x12\r\n\x05right\x18\x04 \x01(\x02\x12\x0e\n\x06\x62ottom\x18\x05 \x01(\x02\x12\x11\n\ttoken_ids\x18\x06 \x03(\r\"J\n\x0ePageSelections\x12\x0c\n\x04page\x18\x01 \x01(\r\x12*\n\x06visual\x18\x02 \x03(\x0b\x32\x1a.resources.VisualSelection\"\x83\x02\n\x11UserFieldMetadata\x12$\n\x05token\x18\x01 \x03(\x0b\x32\x15.resources.TokenSplit\x12\x32\n\nparagraphs\x18\x02 \x03(\x0b\x32\x1e.resources.ParagraphAnnotation\x12\x32\n\x0fpage_selections\x18\x04 \x03(\x0b\x32\x19.resources.PageSelections\x12=\n\x10question_answers\x18\x05 \x03(\x0b\x32#.resources.QuestionAnswerAnnotation\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldID\"\xb5\x03\n\tFieldLink\x12)\n\x05\x61\x64\x64\x65\x64\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x32\n\x07headers\x18\x02 \x03(\x0b\x32!.resources.FieldLink.HeadersEntry\x12\x32\n\x07\x63ookies\x18\x03 \x03(\x0b\x32!.resources.FieldLink.CookiesEntry\x12\x0b\n\x03uri\x18\x04 \x01(\t\x12\x10\n\x08language\x18\x05 \x01(\t\x12<\n\x0clocalstorage\x18\x06 \x03(\x0b\x32&.resources.FieldLink.LocalstorageEntry\x12\x14\n\x0c\x63ss_selector\x18\x07 \x01(\t\x12\r\n\x05xpath\x18\x08 \x01(\t\x1a.\n\x0cHeadersEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a.\n\x0c\x43ookiesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a\x33\n\x11LocalstorageEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\xef\x01\n\tFieldFile\x12)\n\x05\x61\x64\x64\x65\x64\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\"\n\x04\x66ile\x18\x02 \x01(\x0b\x32\x14.resources.CloudFile\x12\x10\n\x08language\x18\x03 \x01(\t\x12\x10\n\x08password\x18\x04 \x01(\t\x12\x0b\n\x03url\x18\x05 \x01(\t\x12\x32\n\x07headers\x18\x06 \x03(\x0b\x32!.resources.FieldFile.HeadersEntry\x1a.\n\x0cHeadersEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"3\n\x06\x45ntity\x12\r\n\x05token\x18\x01 \x01(\t\x12\x0c\n\x04root\x18\x02 \x01(\t\x12\x0c\n\x04type\x18\x03 \x01(\t\"\xa3\x01\n\x12\x46ieldLargeMetadata\x12#\n\x08\x65ntities\x18\x01 \x03(\x0b\x32\x11.resources.Entity\x12\x39\n\x06tokens\x18\x02 \x03(\x0b\x32).resources.FieldLargeMetadata.TokensEntry\x1a-\n\x0bTokensEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x05:\x02\x38\x01\"\x82\x02\n\x15LargeComputedMetadata\x12/\n\x08metadata\x18\x01 \x01(\x0b\x32\x1d.resources.FieldLargeMetadata\x12K\n\x0esplit_metadata\x18\x02 \x03(\x0b\x32\x33.resources.LargeComputedMetadata.SplitMetadataEntry\x12\x16\n\x0e\x64\x65leted_splits\x18\x03 \x03(\t\x1aS\n\x12SplitMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.resources.FieldLargeMetadata:\x02\x38\x01\"\xa9\x01\n\x1cLargeComputedMetadataWrapper\x12\x30\n\x04real\x18\x01 \x01(\x0b\x32 .resources.LargeComputedMetadataH\x00\x12$\n\x04\x66ile\x18\x02 \x01(\x0b\x32\x14.resources.CloudFileH\x00\x12!\n\x05\x66ield\x18\x03 \x01(\x0b\x32\x12.resources.FieldIDB\x0e\n\x0c\x66ile_or_data\"+\n\rPagePositions\x12\r\n\x05start\x18\x01 \x01(\x03\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x03\"2\n\x11PageStructurePage\x12\r\n\x05width\x18\x01 \x01(\x03\x12\x0e\n\x06height\x18\x02 \x01(\x03\"e\n\x12PageStructureToken\x12\t\n\x01x\x18\x07 \x01(\x02\x12\t\n\x01y\x18\x08 \x01(\x02\x12\r\n\x05width\x18\t \x01(\x02\x12\x0e\n\x06height\x18\n \x01(\x02\x12\x0c\n\x04text\x18\x05 \x01(\t\x12\x0c\n\x04line\x18\x06 \x01(\x02\"j\n\rPageStructure\x12*\n\x04page\x18\x01 \x01(\x0b\x32\x1c.resources.PageStructurePage\x12-\n\x06tokens\x18\x02 \x03(\x0b\x32\x1d.resources.PageStructureToken\"\x8b\x01\n\tFilePages\x12#\n\x05pages\x18\x01 \x03(\x0b\x32\x14.resources.CloudFile\x12+\n\tpositions\x18\x02 \x03(\x0b\x32\x18.resources.PagePositions\x12,\n\nstructures\x18\x03 \x03(\x0b\x32\x18.resources.PageStructure\"\xdc\x01\n\x0bRowsPreview\x12\x32\n\x06sheets\x18\x01 \x03(\x0b\x32\".resources.RowsPreview.SheetsEntry\x1aL\n\x05Sheet\x12.\n\x04rows\x18\x01 \x03(\x0b\x32 .resources.RowsPreview.Sheet.Row\x1a\x13\n\x03Row\x12\x0c\n\x04\x63\x65ll\x18\x01 \x03(\t\x1aK\n\x0bSheetsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12+\n\x05value\x18\x02 \x01(\x0b\x32\x1c.resources.RowsPreview.Sheet:\x02\x38\x01\"B\n\x07\x46ieldID\x12(\n\nfield_type\x18\x01 \x01(\x0e\x32\x14.resources.FieldType\x12\r\n\x05\x66ield\x18\x02 \x01(\t\"1\n\x0b\x41llFieldIDs\x12\"\n\x06\x66ields\x18\x01 \x03(\x0b\x32\x12.resources.FieldID*Z\n\tFieldType\x12\x08\n\x04\x46ILE\x10\x00\x12\x08\n\x04LINK\x10\x01\x12\x08\n\x04TEXT\x10\x04\x12\x0b\n\x07GENERIC\x10\x06\x12\x10\n\x0c\x43ONVERSATION\x10\x07\"\x04\x08\x02\x10\x02\"\x04\x08\x03\x10\x03\"\x04\x08\x05\x10\x05P\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'nucliadb_protos.resources_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_BASIC'].fields_by_name['layout']._options = None
  _globals['_BASIC'].fields_by_name['layout']._serialized_options = b'\030\001'
  _globals['_ORIGIN_METADATAENTRY']._options = None
  _globals['_ORIGIN_METADATAENTRY']._serialized_options = b'8\001'
  _globals['_FILEEXTRACTEDDATA_METADATAENTRY']._options = None
  _globals['_FILEEXTRACTEDDATA_METADATAENTRY']._serialized_options = b'8\001'
  _globals['_FILEEXTRACTEDDATA_NESTEDENTRY']._options = None
  _globals['_FILEEXTRACTEDDATA_NESTEDENTRY']._serialized_options = b'8\001'
  _globals['_FILEEXTRACTEDDATA_FILEGENERATEDENTRY']._options = None
  _globals['_FILEEXTRACTEDDATA_FILEGENERATEDENTRY']._serialized_options = b'8\001'
  _globals['_FILEEXTRACTEDDATA_FILEROWSPREVIEWSENTRY']._options = None
  _globals['_FILEEXTRACTEDDATA_FILEROWSPREVIEWSENTRY']._serialized_options = b'8\001'
  _globals['_FILEEXTRACTEDDATA_NESTEDPOSITIONENTRY']._options = None
  _globals['_FILEEXTRACTEDDATA_NESTEDPOSITIONENTRY']._serialized_options = b'8\001'
  _globals['_FILEEXTRACTEDDATA_NESTEDLISTPOSITIONENTRY']._options = None
  _globals['_FILEEXTRACTEDDATA_NESTEDLISTPOSITIONENTRY']._serialized_options = b'8\001'
  _globals['_FILEEXTRACTEDDATA'].fields_by_name['nested_position']._options = None
  _globals['_FILEEXTRACTEDDATA'].fields_by_name['nested_position']._serialized_options = b'\030\001'
  _globals['_LINKEXTRACTEDDATA_METADATAENTRY']._options = None
  _globals['_LINKEXTRACTEDDATA_METADATAENTRY']._serialized_options = b'8\001'
  _globals['_LINKEXTRACTEDDATA_FILEGENERATEDENTRY']._options = None
  _globals['_LINKEXTRACTEDDATA_FILEGENERATEDENTRY']._serialized_options = b'8\001'
  _globals['_USERVECTORSWRAPPER_VECTORSTODELETEENTRY']._options = None
  _globals['_USERVECTORSWRAPPER_VECTORSTODELETEENTRY']._serialized_options = b'8\001'
  _globals['_FIELDMETADATA_NERENTRY']._options = None
  _globals['_FIELDMETADATA_NERENTRY']._serialized_options = b'8\001'
  _globals['_FIELDMETADATA_POSITIONSENTRY']._options = None
  _globals['_FIELDMETADATA_POSITIONSENTRY']._serialized_options = b'8\001'
  _globals['_FIELDQUESTIONANSWERS_SPLITQUESTIONANSWERSENTRY']._options = None
  _globals['_FIELDQUESTIONANSWERS_SPLITQUESTIONANSWERSENTRY']._serialized_options = b'8\001'
  _globals['_FIELDCOMPUTEDMETADATA_SPLITMETADATAENTRY']._options = None
  _globals['_FIELDCOMPUTEDMETADATA_SPLITMETADATAENTRY']._serialized_options = b'8\001'
  _globals['_METADATA_METADATAENTRY']._options = None
  _globals['_METADATA_METADATAENTRY']._serialized_options = b'8\001'
  _globals['_FIELDLINK_HEADERSENTRY']._options = None
  _globals['_FIELDLINK_HEADERSENTRY']._serialized_options = b'8\001'
  _globals['_FIELDLINK_COOKIESENTRY']._options = None
  _globals['_FIELDLINK_COOKIESENTRY']._serialized_options = b'8\001'
  _globals['_FIELDLINK_LOCALSTORAGEENTRY']._options = None
  _globals['_FIELDLINK_LOCALSTORAGEENTRY']._serialized_options = b'8\001'
  _globals['_FIELDFILE_HEADERSENTRY']._options = None
  _globals['_FIELDFILE_HEADERSENTRY']._serialized_options = b'8\001'
  _globals['_FIELDLARGEMETADATA_TOKENSENTRY']._options = None
  _globals['_FIELDLARGEMETADATA_TOKENSENTRY']._serialized_options = b'8\001'
  _globals['_LARGECOMPUTEDMETADATA_SPLITMETADATAENTRY']._options = None
  _globals['_LARGECOMPUTEDMETADATA_SPLITMETADATAENTRY']._serialized_options = b'8\001'
  _globals['_ROWSPREVIEW_SHEETSENTRY']._options = None
  _globals['_ROWSPREVIEW_SHEETSENTRY']._serialized_options = b'8\001'
  _globals['_FIELDTYPE']._serialized_start=11291
  _globals['_FIELDTYPE']._serialized_end=11381
  _globals['_CLOUDFILE']._serialized_start=139
  _globals['_CLOUDFILE']._serialized_end=518
  _globals['_CLOUDFILE_SOURCE']._serialized_start=409
  _globals['_CLOUDFILE_SOURCE']._serialized_end=518
  _globals['_BASIC']._serialized_start=521
  _globals['_BASIC']._serialized_end=1101
  _globals['_BASIC_QUEUETYPE']._serialized_start=1054
  _globals['_BASIC_QUEUETYPE']._serialized_end=1090
  _globals['_ORIGIN']._serialized_start=1104
  _globals['_ORIGIN']._serialized_end=1503
  _globals['_ORIGIN_METADATAENTRY']._serialized_start=1415
  _globals['_ORIGIN_METADATAENTRY']._serialized_end=1462
  _globals['_ORIGIN_SOURCE']._serialized_start=1464
  _globals['_ORIGIN_SOURCE']._serialized_end=1503
  _globals['_EXTRA']._serialized_start=1505
  _globals['_EXTRA']._serialized_end=1555
  _globals['_RELATIONS']._serialized_start=1557
  _globals['_RELATIONS']._serialized_end=1604
  _globals['_MESSAGECONTENT']._serialized_start=1607
  _globals['_MESSAGECONTENT']._serialized_end=1803
  _globals['_MESSAGECONTENT_FORMAT']._serialized_start=1732
  _globals['_MESSAGECONTENT_FORMAT']._serialized_end=1803
  _globals['_MESSAGE']._serialized_start=1806
  _globals['_MESSAGE']._serialized_end=2044
  _globals['_MESSAGE_MESSAGETYPE']._serialized_start=1994
  _globals['_MESSAGE_MESSAGETYPE']._serialized_end=2044
  _globals['_CONVERSATION']._serialized_start=2046
  _globals['_CONVERSATION']._serialized_end=2098
  _globals['_FIELDCONVERSATION']._serialized_start=2100
  _globals['_FIELDCONVERSATION']._serialized_end=2163
  _globals['_NESTEDPOSITION']._serialized_start=2165
  _globals['_NESTEDPOSITION']._serialized_end=2223
  _globals['_NESTEDLISTPOSITION']._serialized_start=2225
  _globals['_NESTEDLISTPOSITION']._serialized_end=2291
  _globals['_FILEEXTRACTEDDATA']._serialized_start=2294
  _globals['_FILEEXTRACTEDDATA']._serialized_end=3390
  _globals['_FILEEXTRACTEDDATA_METADATAENTRY']._serialized_start=1415
  _globals['_FILEEXTRACTEDDATA_METADATAENTRY']._serialized_end=1462
  _globals['_FILEEXTRACTEDDATA_NESTEDENTRY']._serialized_start=3016
  _globals['_FILEEXTRACTEDDATA_NESTEDENTRY']._serialized_end=3061
  _globals['_FILEEXTRACTEDDATA_FILEGENERATEDENTRY']._serialized_start=3063
  _globals['_FILEEXTRACTEDDATA_FILEGENERATEDENTRY']._serialized_end=3137
  _globals['_FILEEXTRACTEDDATA_FILEROWSPREVIEWSENTRY']._serialized_start=3139
  _globals['_FILEEXTRACTEDDATA_FILEROWSPREVIEWSENTRY']._serialized_end=3218
  _globals['_FILEEXTRACTEDDATA_NESTEDPOSITIONENTRY']._serialized_start=3220
  _globals['_FILEEXTRACTEDDATA_NESTEDPOSITIONENTRY']._serialized_end=3300
  _globals['_FILEEXTRACTEDDATA_NESTEDLISTPOSITIONENTRY']._serialized_start=3302
  _globals['_FILEEXTRACTEDDATA_NESTEDLISTPOSITIONENTRY']._serialized_end=3390
  _globals['_LINKEXTRACTEDDATA']._serialized_start=3393
  _globals['_LINKEXTRACTEDDATA']._serialized_end=3993
  _globals['_LINKEXTRACTEDDATA_METADATAENTRY']._serialized_start=1415
  _globals['_LINKEXTRACTEDDATA_METADATAENTRY']._serialized_end=1462
  _globals['_LINKEXTRACTEDDATA_FILEGENERATEDENTRY']._serialized_start=3063
  _globals['_LINKEXTRACTEDDATA_FILEGENERATEDENTRY']._serialized_end=3137
  _globals['_EXTRACTEDTEXTWRAPPER']._serialized_start=3996
  _globals['_EXTRACTEDTEXTWRAPPER']._serialized_end=4145
  _globals['_EXTRACTEDVECTORSWRAPPER']._serialized_start=4148
  _globals['_EXTRACTEDVECTORSWRAPPER']._serialized_end=4324
  _globals['_USERVECTORSWRAPPER']._serialized_start=4327
  _globals['_USERVECTORSWRAPPER']._serialized_end=4580
  _globals['_USERVECTORSWRAPPER_VECTORSTODELETEENTRY']._serialized_start=4502
  _globals['_USERVECTORSWRAPPER_VECTORSTODELETEENTRY']._serialized_end=4580
  _globals['_SENTENCE']._serialized_start=4582
  _globals['_SENTENCE']._serialized_end=4633
  _globals['_PAGEINFORMATION']._serialized_start=4635
  _globals['_PAGEINFORMATION']._serialized_end=4692
  _globals['_REPRESENTATION']._serialized_start=4694
  _globals['_REPRESENTATION']._serialized_end=4754
  _globals['_PARAGRAPHRELATIONS']._serialized_start=4756
  _globals['_PARAGRAPHRELATIONS']._serialized_end=4833
  _globals['_PARAGRAPH']._serialized_start=4836
  _globals['_PARAGRAPH']._serialized_end=5337
  _globals['_PARAGRAPH_TYPEPARAGRAPH']._serialized_start=5233
  _globals['_PARAGRAPH_TYPEPARAGRAPH']._serialized_end=5337
  _globals['_POSITION']._serialized_start=5339
  _globals['_POSITION']._serialized_end=5377
  _globals['_POSITIONS']._serialized_start=5379
  _globals['_POSITIONS']._serialized_end=5445
  _globals['_FIELDMETADATA']._serialized_start=5448
  _globals['_FIELDMETADATA']._serialized_end=6136
  _globals['_FIELDMETADATA_NERENTRY']._serialized_start=6022
  _globals['_FIELDMETADATA_NERENTRY']._serialized_end=6064
  _globals['_FIELDMETADATA_POSITIONSENTRY']._serialized_start=6066
  _globals['_FIELDMETADATA_POSITIONSENTRY']._serialized_end=6136
  _globals['_QUESTION']._serialized_start=6138
  _globals['_QUESTION']._serialized_end=6204
  _globals['_ANSWERS']._serialized_start=6206
  _globals['_ANSWERS']._serialized_end=6271
  _globals['_QUESTIONANSWER']._serialized_start=6273
  _globals['_QUESTIONANSWER']._serialized_end=6365
  _globals['_QUESTIONANSWERS']._serialized_start=6367
  _globals['_QUESTIONANSWERS']._serialized_end=6436
  _globals['_FIELDQUESTIONANSWERS']._serialized_start=6439
  _globals['_FIELDQUESTIONANSWERS']._serialized_end=6719
  _globals['_FIELDQUESTIONANSWERS_SPLITQUESTIONANSWERSENTRY']._serialized_start=6632
  _globals['_FIELDQUESTIONANSWERS_SPLITQUESTIONANSWERSENTRY']._serialized_end=6719
  _globals['_FIELDQUESTIONANSWERWRAPPER']._serialized_start=6722
  _globals['_FIELDQUESTIONANSWERWRAPPER']._serialized_end=6900
  _globals['_FIELDCOMPUTEDMETADATA']._serialized_start=6903
  _globals['_FIELDCOMPUTEDMETADATA']._serialized_end=7151
  _globals['_FIELDCOMPUTEDMETADATA_SPLITMETADATAENTRY']._serialized_start=7073
  _globals['_FIELDCOMPUTEDMETADATA_SPLITMETADATAENTRY']._serialized_end=7151
  _globals['_FIELDCOMPUTEDMETADATAWRAPPER']._serialized_start=7153
  _globals['_FIELDCOMPUTEDMETADATAWRAPPER']._serialized_end=7270
  _globals['_METADATA']._serialized_start=7273
  _globals['_METADATA']._serialized_end=7557
  _globals['_METADATA_METADATAENTRY']._serialized_start=1415
  _globals['_METADATA_METADATAENTRY']._serialized_end=1462
  _globals['_METADATA_STATUS']._serialized_start=7484
  _globals['_METADATA_STATUS']._serialized_end=7557
  _globals['_FIELDTEXT']._serialized_start=7560
  _globals['_FIELDTEXT']._serialized_end=7726
  _globals['_FIELDTEXT_FORMAT']._serialized_start=7645
  _globals['_FIELDTEXT_FORMAT']._serialized_end=7726
  _globals['_BLOCK']._serialized_start=7729
  _globals['_BLOCK']._serialized_end=8013
  _globals['_BLOCK_TYPEBLOCK']._serialized_start=7898
  _globals['_BLOCK_TYPEBLOCK']._serialized_end=8013
  _globals['_CLASSIFICATION']._serialized_start=8015
  _globals['_CLASSIFICATION']._serialized_end=8106
  _globals['_USERMETADATA']._serialized_start=8108
  _globals['_USERMETADATA']._serialized_end=8210
  _globals['_FIELDCLASSIFICATIONS']._serialized_start=8212
  _globals['_FIELDCLASSIFICATIONS']._serialized_end=8321
  _globals['_COMPUTEDMETADATA']._serialized_start=8323
  _globals['_COMPUTEDMETADATA']._serialized_end=8405
  _globals['_TOKENSPLIT']._serialized_start=8407
  _globals['_TOKENSPLIT']._serialized_end=8519
  _globals['_PARAGRAPHANNOTATION']._serialized_start=8521
  _globals['_PARAGRAPHANNOTATION']._serialized_end=8607
  _globals['_QUESTIONANSWERANNOTATION']._serialized_start=8609
  _globals['_QUESTIONANSWERANNOTATION']._serialized_end=8714
  _globals['_VISUALSELECTION']._serialized_start=8716
  _globals['_VISUALSELECTION']._serialized_end=8825
  _globals['_PAGESELECTIONS']._serialized_start=8827
  _globals['_PAGESELECTIONS']._serialized_end=8901
  _globals['_USERFIELDMETADATA']._serialized_start=8904
  _globals['_USERFIELDMETADATA']._serialized_end=9163
  _globals['_FIELDLINK']._serialized_start=9166
  _globals['_FIELDLINK']._serialized_end=9603
  _globals['_FIELDLINK_HEADERSENTRY']._serialized_start=9456
  _globals['_FIELDLINK_HEADERSENTRY']._serialized_end=9502
  _globals['_FIELDLINK_COOKIESENTRY']._serialized_start=9504
  _globals['_FIELDLINK_COOKIESENTRY']._serialized_end=9550
  _globals['_FIELDLINK_LOCALSTORAGEENTRY']._serialized_start=9552
  _globals['_FIELDLINK_LOCALSTORAGEENTRY']._serialized_end=9603
  _globals['_FIELDFILE']._serialized_start=9606
  _globals['_FIELDFILE']._serialized_end=9845
  _globals['_FIELDFILE_HEADERSENTRY']._serialized_start=9456
  _globals['_FIELDFILE_HEADERSENTRY']._serialized_end=9502
  _globals['_ENTITY']._serialized_start=9847
  _globals['_ENTITY']._serialized_end=9898
  _globals['_FIELDLARGEMETADATA']._serialized_start=9901
  _globals['_FIELDLARGEMETADATA']._serialized_end=10064
  _globals['_FIELDLARGEMETADATA_TOKENSENTRY']._serialized_start=10019
  _globals['_FIELDLARGEMETADATA_TOKENSENTRY']._serialized_end=10064
  _globals['_LARGECOMPUTEDMETADATA']._serialized_start=10067
  _globals['_LARGECOMPUTEDMETADATA']._serialized_end=10325
  _globals['_LARGECOMPUTEDMETADATA_SPLITMETADATAENTRY']._serialized_start=10242
  _globals['_LARGECOMPUTEDMETADATA_SPLITMETADATAENTRY']._serialized_end=10325
  _globals['_LARGECOMPUTEDMETADATAWRAPPER']._serialized_start=10328
  _globals['_LARGECOMPUTEDMETADATAWRAPPER']._serialized_end=10497
  _globals['_PAGEPOSITIONS']._serialized_start=10499
  _globals['_PAGEPOSITIONS']._serialized_end=10542
  _globals['_PAGESTRUCTUREPAGE']._serialized_start=10544
  _globals['_PAGESTRUCTUREPAGE']._serialized_end=10594
  _globals['_PAGESTRUCTURETOKEN']._serialized_start=10596
  _globals['_PAGESTRUCTURETOKEN']._serialized_end=10697
  _globals['_PAGESTRUCTURE']._serialized_start=10699
  _globals['_PAGESTRUCTURE']._serialized_end=10805
  _globals['_FILEPAGES']._serialized_start=10808
  _globals['_FILEPAGES']._serialized_end=10947
  _globals['_ROWSPREVIEW']._serialized_start=10950
  _globals['_ROWSPREVIEW']._serialized_end=11170
  _globals['_ROWSPREVIEW_SHEET']._serialized_start=11017
  _globals['_ROWSPREVIEW_SHEET']._serialized_end=11093
  _globals['_ROWSPREVIEW_SHEET_ROW']._serialized_start=11074
  _globals['_ROWSPREVIEW_SHEET_ROW']._serialized_end=11093
  _globals['_ROWSPREVIEW_SHEETSENTRY']._serialized_start=11095
  _globals['_ROWSPREVIEW_SHEETSENTRY']._serialized_end=11170
  _globals['_FIELDID']._serialized_start=11172
  _globals['_FIELDID']._serialized_end=11238
  _globals['_ALLFIELDIDS']._serialized_start=11240
  _globals['_ALLFIELDIDS']._serialized_end=11289
# @@protoc_insertion_point(module_scope)
