#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TextInformation {
    #[prost(string, tag="1")]
    pub text: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="2")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexMetadata {
    /// Tantivy doc & para
    #[prost(message, optional, tag="1")]
    pub modified: ::core::option::Option<::prost_types::Timestamp>,
    /// Tantivy doc & para
    #[prost(message, optional, tag="2")]
    pub created: ::core::option::Option<::prost_types::Timestamp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardId {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardIds {
    #[prost(message, repeated, tag="1")]
    pub ids: ::prost::alloc::vec::Vec<ShardId>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardCreated {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(enumeration="shard_created::DocumentService", tag="2")]
    pub document_service: i32,
    #[prost(enumeration="shard_created::ParagraphService", tag="3")]
    pub paragraph_service: i32,
    #[prost(enumeration="shard_created::VectorService", tag="4")]
    pub vector_service: i32,
    #[prost(enumeration="shard_created::RelationService", tag="5")]
    pub relation_service: i32,
}
/// Nested message and enum types in `ShardCreated`.
pub mod shard_created {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum DocumentService {
        DocumentV0 = 0,
        DocumentV1 = 1,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ParagraphService {
        ParagraphV0 = 0,
        ParagraphV1 = 1,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum VectorService {
        VectorV0 = 0,
        VectorV1 = 1,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum RelationService {
        RelationV0 = 0,
        RelationV1 = 1,
        RelationV2 = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardCleaned {
    #[prost(enumeration="shard_created::DocumentService", tag="2")]
    pub document_service: i32,
    #[prost(enumeration="shard_created::ParagraphService", tag="3")]
    pub paragraph_service: i32,
    #[prost(enumeration="shard_created::VectorService", tag="4")]
    pub vector_service: i32,
    #[prost(enumeration="shard_created::RelationService", tag="5")]
    pub relation_service: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceId {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub uuid: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Shard {
    #[prost(message, optional, tag="5")]
    pub metadata: ::core::option::Option<ShardMetadata>,
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    #[prost(uint64, tag="2")]
    pub fields: u64,
    #[prost(uint64, tag="3")]
    pub paragraphs: u64,
    #[prost(uint64, tag="4")]
    pub sentences: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyResponse {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyQuery {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Position {
    #[prost(uint64, tag="1")]
    pub index: u64,
    #[prost(uint64, tag="2")]
    pub start: u64,
    #[prost(uint64, tag="3")]
    pub end: u64,
    /// For pdfs/documents only
    #[prost(uint64, tag="4")]
    pub page_number: u64,
    /// For multimedia only
    #[prost(uint32, repeated, tag="5")]
    pub start_seconds: ::prost::alloc::vec::Vec<u32>,
    #[prost(uint32, repeated, tag="6")]
    pub end_seconds: ::prost::alloc::vec::Vec<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SentenceMetadata {
    #[prost(message, optional, tag="1")]
    pub position: ::core::option::Option<Position>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSentence {
    #[prost(float, repeated, tag="1")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
    #[prost(message, optional, tag="9")]
    pub metadata: ::core::option::Option<SentenceMetadata>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParagraphMetadata {
    #[prost(message, optional, tag="1")]
    pub position: ::core::option::Option<Position>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexParagraph {
    /// Start end position in field text
    #[prost(int32, tag="1")]
    pub start: i32,
    /// Start end position in field text
    #[prost(int32, tag="2")]
    pub end: i32,
    /// Paragraph specific labels
    #[prost(string, repeated, tag="3")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// key is full id for vectors
    #[prost(map="string, message", tag="4")]
    pub sentences: ::std::collections::HashMap<::prost::alloc::string::String, VectorSentence>,
    #[prost(string, tag="5")]
    pub field: ::prost::alloc::string::String,
    /// split were it belongs
    #[prost(string, tag="6")]
    pub split: ::prost::alloc::string::String,
    #[prost(uint64, tag="7")]
    pub index: u64,
    #[prost(bool, tag="8")]
    pub repeated_in_field: bool,
    #[prost(message, optional, tag="9")]
    pub metadata: ::core::option::Option<ParagraphMetadata>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSetId {
    #[prost(message, optional, tag="1")]
    pub shard: ::core::option::Option<ShardId>,
    #[prost(string, tag="2")]
    pub vectorset: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSetList {
    #[prost(message, optional, tag="1")]
    pub shard: ::core::option::Option<ShardId>,
    #[prost(string, repeated, tag="2")]
    pub vectorset: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexParagraphs {
    /// id of the paragraph f"{self.rid}/{field_key}/{paragraph.start}-{paragraph.end}"
    #[prost(map="string, message", tag="1")]
    pub paragraphs: ::std::collections::HashMap<::prost::alloc::string::String, IndexParagraph>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Resource {
    #[prost(message, optional, tag="1")]
    pub resource: ::core::option::Option<ResourceId>,
    #[prost(message, optional, tag="2")]
    pub metadata: ::core::option::Option<IndexMetadata>,
    /// Doc index
    ///
    /// Tantivy doc filled by field allways full
    #[prost(map="string, message", tag="3")]
    pub texts: ::std::collections::HashMap<::prost::alloc::string::String, TextInformation>,
    // Key is RID/FIELDID

    /// Document labels always serialized full
    #[prost(string, repeated, tag="4")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Tantivy doc
    #[prost(enumeration="resource::ResourceStatus", tag="5")]
    pub status: i32,
    /// Paragraph
    ///
    /// Paragraphs by field
    #[prost(map="string, message", tag="6")]
    pub paragraphs: ::std::collections::HashMap<::prost::alloc::string::String, IndexParagraphs>,
    #[prost(string, repeated, tag="7")]
    pub paragraphs_to_delete: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="8")]
    pub sentences_to_delete: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Relations
    #[prost(message, repeated, tag="9")]
    pub relations: ::prost::alloc::vec::Vec<super::utils::Relation>,
    #[prost(string, tag="11")]
    pub shard_id: ::prost::alloc::string::String,
    /// vectorset is the key
    #[prost(map="string, message", tag="12")]
    pub vectors: ::std::collections::HashMap<::prost::alloc::string::String, super::utils::UserVectors>,
    /// Vectorset prefix vector id
    #[prost(map="string, message", tag="13")]
    pub vectors_to_delete: ::std::collections::HashMap<::prost::alloc::string::String, super::utils::UserVectorsList>,
}
/// Nested message and enum types in `Resource`.
pub mod resource {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ResourceStatus {
        Processed = 0,
        Empty = 1,
        Error = 2,
        Delete = 3,
        Pending = 4,
        Blocked = 5,
        Expired = 6,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardMetadata {
    #[prost(string, tag="1")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(enumeration="super::utils::ReleaseChannel", tag="2")]
    pub release_channel: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeMetadata {
    #[deprecated]
    #[prost(float, tag="1")]
    pub load_score: f32,
    #[prost(uint64, tag="2")]
    pub shard_count: u64,
    #[prost(map="string, message", tag="3")]
    pub shards: ::std::collections::HashMap<::prost::alloc::string::String, node_metadata::ShardMetadata>,
    #[prost(string, tag="4")]
    pub node_id: ::prost::alloc::string::String,
    #[prost(string, optional, tag="5")]
    pub primary_node_id: ::core::option::Option<::prost::alloc::string::String>,
}
/// Nested message and enum types in `NodeMetadata`.
pub mod node_metadata {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ShardMetadata {
        #[prost(string, tag="1")]
        pub kbid: ::prost::alloc::string::String,
        #[deprecated]
        #[prost(float, tag="2")]
        pub load_score: f32,
    }
}
