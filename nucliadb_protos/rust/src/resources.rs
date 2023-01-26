#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CloudFile {
    #[prost(string, tag="1")]
    pub uri: ::prost::alloc::string::String,
    #[prost(int32, tag="2")]
    pub size: i32,
    #[prost(string, tag="3")]
    pub content_type: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub bucket_name: ::prost::alloc::string::String,
    #[prost(enumeration="cloud_file::Source", tag="5")]
    pub source: i32,
    #[prost(string, tag="6")]
    pub filename: ::prost::alloc::string::String,
    /// Temporal upload information
    #[prost(string, tag="7")]
    pub resumable_uri: ::prost::alloc::string::String,
    #[prost(int64, tag="8")]
    pub offset: i64,
    #[prost(string, tag="9")]
    pub upload_uri: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="10")]
    pub parts: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="11")]
    pub old_uri: ::prost::alloc::string::String,
    #[prost(string, tag="12")]
    pub old_bucket: ::prost::alloc::string::String,
    #[prost(string, tag="13")]
    pub md5: ::prost::alloc::string::String,
}
/// Nested message and enum types in `CloudFile`.
pub mod cloud_file {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Source {
        Flaps = 0,
        Gcs = 1,
        S3 = 2,
        Local = 3,
        External = 4,
        Empty = 5,
        Export = 6,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Basic {
    #[prost(string, tag="1")]
    pub slug: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub icon: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub summary: ::prost::alloc::string::String,
    /// reference to inner thumbnail
    #[prost(string, tag="5")]
    pub thumbnail: ::prost::alloc::string::String,
    #[prost(string, tag="6")]
    pub layout: ::prost::alloc::string::String,
    #[prost(message, optional, tag="7")]
    pub created: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag="8")]
    pub modified: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag="9")]
    pub metadata: ::core::option::Option<Metadata>,
    /// Not Basic
    #[prost(message, optional, tag="10")]
    pub usermetadata: ::core::option::Option<UserMetadata>,
    #[prost(message, repeated, tag="11")]
    pub fieldmetadata: ::prost::alloc::vec::Vec<UserFieldMetadata>,
    #[prost(message, optional, tag="15")]
    pub computedmetadata: ::core::option::Option<ComputedMetadata>,
    /// Only for read operations
    #[prost(string, tag="12")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="13")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// last processing seqid of the resource
    #[prost(int64, tag="14")]
    pub last_seqid: i64,
    /// last processing sequid (non nats) of this resource in the account queue
    #[prost(int64, tag="35")]
    pub last_account_seq: i64,
    #[prost(enumeration="basic::QueueType", tag="36")]
    pub queue: i32,
}
/// Nested message and enum types in `Basic`.
pub mod basic {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum QueueType {
        Private = 0,
        Shared = 1,
    }
}
// Block behaviors

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Origin {
    #[prost(enumeration="origin::Source", tag="1")]
    pub source: i32,
    #[prost(string, tag="2")]
    pub source_id: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub url: ::prost::alloc::string::String,
    #[prost(message, optional, tag="4")]
    pub created: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag="5")]
    pub modified: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(map="string, string", tag="6")]
    pub metadata: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(string, repeated, tag="7")]
    pub tags: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="8")]
    pub colaborators: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="9")]
    pub filename: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="10")]
    pub related: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Nested message and enum types in `Origin`.
pub mod origin {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Source {
        Web = 0,
        Desktop = 1,
        Api = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Relations {
    #[prost(message, repeated, tag="1")]
    pub relations: ::prost::alloc::vec::Vec<super::utils::Relation>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MessageContent {
    #[prost(string, tag="1")]
    pub text: ::prost::alloc::string::String,
    #[prost(enumeration="message_content::Format", tag="2")]
    pub format: i32,
    #[prost(message, repeated, tag="4")]
    pub attachments: ::prost::alloc::vec::Vec<CloudFile>,
}
/// Nested message and enum types in `MessageContent`.
pub mod message_content {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Format {
        Plain = 0,
        Html = 1,
        Markdown = 2,
        Rst = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Message {
    #[prost(message, optional, tag="1")]
    pub timestamp: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(string, tag="2")]
    pub who: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="3")]
    pub to: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag="4")]
    pub content: ::core::option::Option<MessageContent>,
    #[prost(string, tag="5")]
    pub ident: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Conversation {
    #[prost(message, repeated, tag="1")]
    pub messages: ::prost::alloc::vec::Vec<Message>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldConversation {
    #[prost(int32, tag="1")]
    pub pages: i32,
    #[prost(int32, tag="2")]
    pub size: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NestedPosition {
    #[prost(int64, tag="1")]
    pub start: i64,
    #[prost(int64, tag="2")]
    pub end: i64,
    #[prost(int64, tag="3")]
    pub page: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NestedListPosition {
    #[prost(message, repeated, tag="1")]
    pub positions: ::prost::alloc::vec::Vec<NestedPosition>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileExtractedData {
    #[prost(string, tag="1")]
    pub language: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub md5: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="3")]
    pub metadata: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(map="string, string", tag="4")]
    pub nested: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(map="string, message", tag="5")]
    pub file_generated: ::std::collections::HashMap<::prost::alloc::string::String, CloudFile>,
    #[prost(map="string, message", tag="6")]
    pub file_rows_previews: ::std::collections::HashMap<::prost::alloc::string::String, RowsPreview>,
    #[prost(message, optional, tag="7")]
    pub file_preview: ::core::option::Option<CloudFile>,
    #[prost(message, optional, tag="8")]
    pub file_pages_previews: ::core::option::Option<FilePages>,
    #[prost(message, optional, tag="9")]
    pub file_thumbnail: ::core::option::Option<CloudFile>,
    #[prost(string, tag="10")]
    pub field: ::prost::alloc::string::String,
    #[prost(string, tag="11")]
    pub icon: ::prost::alloc::string::String,
    #[prost(map="string, message", tag="12")]
    pub nested_position: ::std::collections::HashMap<::prost::alloc::string::String, NestedPosition>,
    #[prost(map="string, message", tag="13")]
    pub nested_list_position: ::std::collections::HashMap<::prost::alloc::string::String, NestedListPosition>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LinkExtractedData {
    #[prost(message, optional, tag="1")]
    pub date: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(string, tag="2")]
    pub language: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub title: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="5")]
    pub metadata: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(message, optional, tag="6")]
    pub link_thumbnail: ::core::option::Option<CloudFile>,
    #[prost(message, optional, tag="7")]
    pub link_preview: ::core::option::Option<CloudFile>,
    #[prost(string, tag="8")]
    pub field: ::prost::alloc::string::String,
    #[prost(message, optional, tag="9")]
    pub link_image: ::core::option::Option<CloudFile>,
    #[prost(string, tag="10")]
    pub description: ::prost::alloc::string::String,
    #[prost(string, tag="11")]
    pub r#type: ::prost::alloc::string::String,
    #[prost(string, tag="12")]
    pub embed: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractedTextWrapper {
    #[prost(message, optional, tag="3")]
    pub field: ::core::option::Option<FieldId>,
    #[prost(oneof="extracted_text_wrapper::FileOrData", tags="1, 2")]
    pub file_or_data: ::core::option::Option<extracted_text_wrapper::FileOrData>,
}
/// Nested message and enum types in `ExtractedTextWrapper`.
pub mod extracted_text_wrapper {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FileOrData {
        #[prost(message, tag="1")]
        Body(super::super::utils::ExtractedText),
        #[prost(message, tag="2")]
        File(super::CloudFile),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractedVectorsWrapper {
    #[prost(message, optional, tag="3")]
    pub field: ::core::option::Option<FieldId>,
    #[prost(oneof="extracted_vectors_wrapper::FileOrData", tags="1, 2")]
    pub file_or_data: ::core::option::Option<extracted_vectors_wrapper::FileOrData>,
}
/// Nested message and enum types in `ExtractedVectorsWrapper`.
pub mod extracted_vectors_wrapper {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FileOrData {
        #[prost(message, tag="1")]
        Vectors(super::super::utils::VectorObject),
        #[prost(message, tag="2")]
        File(super::CloudFile),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserVectorsWrapper {
    #[prost(message, optional, tag="1")]
    pub vectors: ::core::option::Option<super::utils::UserVectorSet>,
    /// Vectorset prefix vector id
    #[prost(map="string, message", tag="13")]
    pub vectors_to_delete: ::std::collections::HashMap<::prost::alloc::string::String, super::utils::UserVectorsList>,
    #[prost(message, optional, tag="3")]
    pub field: ::core::option::Option<FieldId>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sentence {
    #[prost(int32, tag="1")]
    pub start: i32,
    #[prost(int32, tag="2")]
    pub end: i32,
    #[prost(string, tag="3")]
    pub key: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Paragraph {
    #[prost(uint32, tag="1")]
    pub start: u32,
    #[prost(uint32, tag="2")]
    pub end: u32,
    #[prost(uint32, repeated, tag="3")]
    pub start_seconds: ::prost::alloc::vec::Vec<u32>,
    #[prost(uint32, repeated, tag="4")]
    pub end_seconds: ::prost::alloc::vec::Vec<u32>,
    #[prost(enumeration="paragraph::TypeParagraph", tag="5")]
    pub kind: i32,
    #[prost(message, repeated, tag="6")]
    pub classifications: ::prost::alloc::vec::Vec<Classification>,
    #[prost(message, repeated, tag="7")]
    pub sentences: ::prost::alloc::vec::Vec<Sentence>,
    #[prost(string, tag="8")]
    pub key: ::prost::alloc::string::String,
    /// Optional, as a computed value
    #[prost(string, tag="9")]
    pub text: ::prost::alloc::string::String,
}
/// Nested message and enum types in `Paragraph`.
pub mod paragraph {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum TypeParagraph {
        Text = 0,
        Ocr = 1,
        Inception = 2,
        Description = 3,
        Transcript = 4,
        Title = 5,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Position {
    #[prost(int64, tag="1")]
    pub start: i64,
    #[prost(int64, tag="2")]
    pub end: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Positions {
    #[prost(message, repeated, tag="1")]
    pub position: ::prost::alloc::vec::Vec<Position>,
    #[prost(string, tag="2")]
    pub entity: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldMetadata {
    #[prost(string, repeated, tag="1")]
    pub links: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, repeated, tag="2")]
    pub paragraphs: ::prost::alloc::vec::Vec<Paragraph>,
    /// Document
    #[prost(map="string, string", tag="3")]
    pub ner: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(message, repeated, tag="4")]
    pub classifications: ::prost::alloc::vec::Vec<Classification>,
    #[prost(message, optional, tag="5")]
    pub last_index: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag="6")]
    pub last_understanding: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag="7")]
    pub last_extract: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag="8")]
    pub last_summary: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag="9")]
    pub thumbnail: ::core::option::Option<CloudFile>,
    #[prost(string, tag="10")]
    pub language: ::prost::alloc::string::String,
    #[prost(string, tag="11")]
    pub summary: ::prost::alloc::string::String,
    /// Document
    #[prost(map="string, message", tag="12")]
    pub positions: ::std::collections::HashMap<::prost::alloc::string::String, Positions>,
    #[prost(message, repeated, tag="13")]
    pub relations: ::prost::alloc::vec::Vec<Relations>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldComputedMetadata {
    #[prost(message, optional, tag="1")]
    pub metadata: ::core::option::Option<FieldMetadata>,
    #[prost(map="string, message", tag="2")]
    pub split_metadata: ::std::collections::HashMap<::prost::alloc::string::String, FieldMetadata>,
    #[prost(string, repeated, tag="3")]
    pub deleted_splits: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldComputedMetadataWrapper {
    #[prost(message, optional, tag="1")]
    pub metadata: ::core::option::Option<FieldComputedMetadata>,
    #[prost(message, optional, tag="4")]
    pub field: ::core::option::Option<FieldId>,
}
// Mutable behaviors

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Metadata {
    #[prost(map="string, string", tag="1")]
    pub metadata: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(string, tag="2")]
    pub language: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="3")]
    pub languages: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(bool, tag="4")]
    pub useful: bool,
    #[prost(enumeration="metadata::Status", tag="5")]
    pub status: i32,
}
/// Nested message and enum types in `Metadata`.
pub mod metadata {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Pending = 0,
        Processed = 1,
        Error = 2,
        Blocked = 3,
        Expired = 4,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldText {
    #[prost(string, tag="1")]
    pub body: ::prost::alloc::string::String,
    #[prost(enumeration="field_text::Format", tag="2")]
    pub format: i32,
    #[prost(string, tag="3")]
    pub md5: ::prost::alloc::string::String,
}
/// Nested message and enum types in `FieldText`.
pub mod field_text {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Format {
        Plain = 0,
        Html = 1,
        Rst = 2,
        Markdown = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Block {
    #[prost(int32, tag="1")]
    pub x: i32,
    #[prost(int32, tag="2")]
    pub y: i32,
    #[prost(int32, tag="3")]
    pub cols: i32,
    #[prost(int32, tag="4")]
    pub rows: i32,
    #[prost(enumeration="block::TypeBlock", tag="5")]
    pub r#type: i32,
    #[prost(string, tag="6")]
    pub ident: ::prost::alloc::string::String,
    #[prost(string, tag="7")]
    pub payload: ::prost::alloc::string::String,
    #[prost(message, optional, tag="8")]
    pub file: ::core::option::Option<CloudFile>,
}
/// Nested message and enum types in `Block`.
pub mod block {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum TypeBlock {
        Title = 0,
        Description = 1,
        Richtext = 2,
        Text = 3,
        Attachments = 4,
        Comments = 5,
        Classifications = 6,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LayoutContent {
    #[prost(map="string, message", tag="1")]
    pub blocks: ::std::collections::HashMap<::prost::alloc::string::String, Block>,
    #[prost(string, repeated, tag="2")]
    pub deleted_blocks: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldLayout {
    #[prost(message, optional, tag="1")]
    pub body: ::core::option::Option<LayoutContent>,
    #[prost(enumeration="field_layout::Format", tag="2")]
    pub format: i32,
}
/// Nested message and enum types in `FieldLayout`.
pub mod field_layout {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Format {
        NucliAv1 = 0,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Classification {
    #[prost(string, tag="1")]
    pub labelset: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub label: ::prost::alloc::string::String,
    #[prost(bool, tag="3")]
    pub cancelled_by_user: bool,
    /// On field classification we need to set on which split is the classification
    #[prost(string, tag="4")]
    pub split: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserMetadata {
    #[prost(message, repeated, tag="1")]
    pub classifications: ::prost::alloc::vec::Vec<Classification>,
    #[prost(message, repeated, tag="3")]
    pub relations: ::prost::alloc::vec::Vec<super::utils::Relation>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldClassifications {
    #[prost(message, optional, tag="1")]
    pub field: ::core::option::Option<FieldId>,
    #[prost(message, repeated, tag="2")]
    pub classifications: ::prost::alloc::vec::Vec<Classification>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ComputedMetadata {
    #[prost(message, repeated, tag="1")]
    pub field_classifications: ::prost::alloc::vec::Vec<FieldClassifications>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TokenSplit {
    #[prost(string, tag="1")]
    pub token: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub klass: ::prost::alloc::string::String,
    #[prost(uint32, tag="3")]
    pub start: u32,
    #[prost(uint32, tag="4")]
    pub end: u32,
    #[prost(bool, tag="5")]
    pub cancelled_by_user: bool,
    #[prost(string, tag="6")]
    pub split: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParagraphAnnotation {
    #[prost(string, tag="1")]
    pub key: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="2")]
    pub classifications: ::prost::alloc::vec::Vec<Classification>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserFieldMetadata {
    #[prost(message, repeated, tag="1")]
    pub token: ::prost::alloc::vec::Vec<TokenSplit>,
    #[prost(message, repeated, tag="2")]
    pub paragraphs: ::prost::alloc::vec::Vec<ParagraphAnnotation>,
    #[prost(message, optional, tag="3")]
    pub field: ::core::option::Option<FieldId>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldLink {
    #[prost(message, optional, tag="1")]
    pub added: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(map="string, string", tag="2")]
    pub headers: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(map="string, string", tag="3")]
    pub cookies: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(string, tag="4")]
    pub uri: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub language: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="6")]
    pub localstorage: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Keyword {
    #[prost(string, tag="1")]
    pub value: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldKeywordset {
    #[prost(message, repeated, tag="1")]
    pub keywords: ::prost::alloc::vec::Vec<Keyword>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldDatetime {
    #[prost(message, optional, tag="1")]
    pub value: ::core::option::Option<::prost_types::Timestamp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldFile {
    #[prost(message, optional, tag="1")]
    pub added: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag="2")]
    pub file: ::core::option::Option<CloudFile>,
    #[prost(string, tag="3")]
    pub language: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub url: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="6")]
    pub headers: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(map="string, string", tag="7")]
    pub cookies: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Entity {
    #[prost(string, tag="1")]
    pub token: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub root: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub r#type: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldLargeMetadata {
    #[prost(message, repeated, tag="1")]
    pub entities: ::prost::alloc::vec::Vec<Entity>,
    #[prost(map="string, int32", tag="2")]
    pub tokens: ::std::collections::HashMap<::prost::alloc::string::String, i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LargeComputedMetadata {
    #[prost(message, optional, tag="1")]
    pub metadata: ::core::option::Option<FieldLargeMetadata>,
    #[prost(map="string, message", tag="2")]
    pub split_metadata: ::std::collections::HashMap<::prost::alloc::string::String, FieldLargeMetadata>,
    #[prost(string, repeated, tag="3")]
    pub deleted_splits: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LargeComputedMetadataWrapper {
    #[prost(message, optional, tag="3")]
    pub field: ::core::option::Option<FieldId>,
    #[prost(oneof="large_computed_metadata_wrapper::FileOrData", tags="1, 2")]
    pub file_or_data: ::core::option::Option<large_computed_metadata_wrapper::FileOrData>,
}
/// Nested message and enum types in `LargeComputedMetadataWrapper`.
pub mod large_computed_metadata_wrapper {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FileOrData {
        #[prost(message, tag="1")]
        Real(super::LargeComputedMetadata),
        #[prost(message, tag="2")]
        File(super::CloudFile),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PagePositions {
    #[prost(int64, tag="1")]
    pub start: i64,
    #[prost(int64, tag="2")]
    pub end: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilePages {
    #[prost(message, repeated, tag="1")]
    pub pages: ::prost::alloc::vec::Vec<CloudFile>,
    #[prost(message, repeated, tag="2")]
    pub positions: ::prost::alloc::vec::Vec<PagePositions>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RowsPreview {
    #[prost(map="string, message", tag="1")]
    pub sheets: ::std::collections::HashMap<::prost::alloc::string::String, rows_preview::Sheet>,
}
/// Nested message and enum types in `RowsPreview`.
pub mod rows_preview {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Sheet {
        #[prost(message, repeated, tag="1")]
        pub rows: ::prost::alloc::vec::Vec<sheet::Row>,
    }
    /// Nested message and enum types in `Sheet`.
    pub mod sheet {
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct Row {
            #[prost(string, repeated, tag="1")]
            pub cell: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        }
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldId {
    #[prost(enumeration="FieldType", tag="1")]
    pub field_type: i32,
    #[prost(string, tag="2")]
    pub field: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum FieldType {
    File = 0,
    Link = 1,
    Datetime = 2,
    Keywordset = 3,
    Text = 4,
    Layout = 5,
    /// Base title/summary fields
    Generic = 6,
    Conversation = 7,
}
