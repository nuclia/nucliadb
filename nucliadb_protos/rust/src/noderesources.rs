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
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ParagraphService {
        ParagraphV0 = 0,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum VectorService {
        VectorV0 = 0,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum RelationService {
        RelationV0 = 0,
    }
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
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    #[prost(uint64, tag="2")]
    pub resources: u64,
    #[prost(uint64, tag="3")]
    pub paragraphs: u64,
    #[prost(uint64, tag="4")]
    pub sentences: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardList {
    #[prost(message, repeated, tag="1")]
    pub shards: ::prost::alloc::vec::Vec<Shard>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyResponse {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyQuery {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSentence {
    #[prost(float, repeated, tag="1")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
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

    /// Document labels allways serialized full
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
    #[prost(message, repeated, tag="10")]
    pub relations_to_delete: ::prost::alloc::vec::Vec<super::utils::Relation>,
    #[prost(string, tag="11")]
    pub shard_id: ::prost::alloc::string::String,
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
    }
}
