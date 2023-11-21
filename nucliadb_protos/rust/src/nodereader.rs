#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Filter {
    #[prost(string, repeated, tag="1")]
    pub field_labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="2")]
    pub paragraph_labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamFilter {
    #[prost(enumeration="stream_filter::Conjunction", tag="1")]
    pub conjunction: i32,
    #[prost(string, repeated, tag="2")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Nested message and enum types in `StreamFilter`.
pub mod stream_filter {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Conjunction {
        And = 0,
        Or = 1,
        Not = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Faceted {
    #[prost(string, repeated, tag="1")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OrderBy {
    #[deprecated]
    #[prost(string, tag="1")]
    pub field: ::prost::alloc::string::String,
    #[prost(enumeration="order_by::OrderType", tag="2")]
    pub r#type: i32,
    #[prost(enumeration="order_by::OrderField", tag="3")]
    pub sort_by: i32,
}
/// Nested message and enum types in `OrderBy`.
pub mod order_by {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum OrderType {
        Desc = 0,
        Asc = 1,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum OrderField {
        Created = 0,
        Modified = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamps {
    #[prost(message, optional, tag="1")]
    pub from_modified: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag="2")]
    pub to_modified: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag="3")]
    pub from_created: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag="4")]
    pub to_created: ::core::option::Option<::prost_types::Timestamp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FacetResult {
    #[prost(string, tag="1")]
    pub tag: ::prost::alloc::string::String,
    #[prost(int32, tag="2")]
    pub total: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FacetResults {
    #[prost(message, repeated, tag="1")]
    pub facetresults: ::prost::alloc::vec::Vec<FacetResult>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocumentSearchRequest {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub body: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="3")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag="4")]
    pub filter: ::core::option::Option<Filter>,
    #[prost(message, optional, tag="5")]
    pub order: ::core::option::Option<OrderBy>,
    #[prost(message, optional, tag="6")]
    pub faceted: ::core::option::Option<Faceted>,
    #[prost(int32, tag="7")]
    pub page_number: i32,
    #[prost(int32, tag="8")]
    pub result_per_page: i32,
    #[prost(message, optional, tag="9")]
    pub timestamps: ::core::option::Option<Timestamps>,
    #[deprecated]
    #[prost(bool, tag="10")]
    pub reload: bool,
    #[prost(bool, tag="15")]
    pub only_faceted: bool,
    #[prost(enumeration="super::noderesources::resource::ResourceStatus", optional, tag="16")]
    pub with_status: ::core::option::Option<i32>,
    #[prost(string, optional, tag="17")]
    pub advanced_query: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParagraphSearchRequest {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="3")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// query this text in all the paragraphs
    #[prost(string, tag="4")]
    pub body: ::prost::alloc::string::String,
    #[prost(message, optional, tag="5")]
    pub filter: ::core::option::Option<Filter>,
    #[prost(message, optional, tag="7")]
    pub order: ::core::option::Option<OrderBy>,
    /// Faceted{ labels: Vec<String>}
    #[prost(message, optional, tag="8")]
    pub faceted: ::core::option::Option<Faceted>,
    #[prost(int32, tag="10")]
    pub page_number: i32,
    #[prost(int32, tag="11")]
    pub result_per_page: i32,
    #[prost(message, optional, tag="12")]
    pub timestamps: ::core::option::Option<Timestamps>,
    #[deprecated]
    #[prost(bool, tag="13")]
    pub reload: bool,
    #[prost(bool, tag="14")]
    pub with_duplicates: bool,
    #[prost(bool, tag="15")]
    pub only_faceted: bool,
    #[prost(string, optional, tag="16")]
    pub advanced_query: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="17")]
    pub key_filters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResultScore {
    #[prost(float, tag="1")]
    pub bm25: f32,
    /// In the case of two equal bm25 scores, booster
    /// decides
    #[prost(float, tag="2")]
    pub booster: f32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocumentResult {
    #[prost(string, tag="1")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub score: ::core::option::Option<ResultScore>,
    #[prost(string, tag="4")]
    pub field: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="5")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocumentSearchResponse {
    #[prost(int32, tag="1")]
    pub total: i32,
    #[prost(message, repeated, tag="2")]
    pub results: ::prost::alloc::vec::Vec<DocumentResult>,
    #[prost(map="string, message", tag="3")]
    pub facets: ::std::collections::HashMap<::prost::alloc::string::String, FacetResults>,
    #[prost(int32, tag="4")]
    pub page_number: i32,
    #[prost(int32, tag="5")]
    pub result_per_page: i32,
    /// The text that lead to this results
    #[prost(string, tag="6")]
    pub query: ::prost::alloc::string::String,
    /// Is there a next page
    #[prost(bool, tag="7")]
    pub next_page: bool,
    #[prost(bool, tag="8")]
    pub bm25: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParagraphResult {
    #[prost(string, tag="1")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub field: ::prost::alloc::string::String,
    #[prost(uint64, tag="4")]
    pub start: u64,
    #[prost(uint64, tag="5")]
    pub end: u64,
    #[prost(string, tag="6")]
    pub paragraph: ::prost::alloc::string::String,
    #[prost(string, tag="7")]
    pub split: ::prost::alloc::string::String,
    #[prost(uint64, tag="8")]
    pub index: u64,
    #[prost(message, optional, tag="9")]
    pub score: ::core::option::Option<ResultScore>,
    #[prost(string, repeated, tag="10")]
    pub matches: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Metadata that can't be searched with but is returned on search results
    #[prost(message, optional, tag="11")]
    pub metadata: ::core::option::Option<super::noderesources::ParagraphMetadata>,
    #[prost(string, repeated, tag="12")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParagraphSearchResponse {
    #[prost(int32, tag="10")]
    pub fuzzy_distance: i32,
    #[prost(int32, tag="1")]
    pub total: i32,
    ///
    #[prost(message, repeated, tag="2")]
    pub results: ::prost::alloc::vec::Vec<ParagraphResult>,
    /// For each field what facets are.
    #[prost(map="string, message", tag="3")]
    pub facets: ::std::collections::HashMap<::prost::alloc::string::String, FacetResults>,
    /// What page is the answer.
    #[prost(int32, tag="4")]
    pub page_number: i32,
    /// How many results are in this page.
    #[prost(int32, tag="5")]
    pub result_per_page: i32,
    /// The text that lead to this results
    #[prost(string, tag="6")]
    pub query: ::prost::alloc::string::String,
    /// Is there a next page
    #[prost(bool, tag="7")]
    pub next_page: bool,
    #[prost(bool, tag="8")]
    pub bm25: bool,
    #[prost(string, repeated, tag="9")]
    pub ematches: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSearchRequest {
    ///Shard ID
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    /// Embedded vector search.
    #[prost(float, repeated, tag="2")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
    /// labels to filter
    #[prost(string, repeated, tag="3")]
    pub field_labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// paragraph labels to filter
    #[prost(string, repeated, tag="18")]
    pub paragraph_labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// What page is the answer.
    #[prost(int32, tag="4")]
    pub page_number: i32,
    /// How many results are in this page.
    #[prost(int32, tag="5")]
    pub result_per_page: i32,
    #[deprecated]
    #[prost(bool, tag="13")]
    pub reload: bool,
    #[prost(bool, tag="14")]
    pub with_duplicates: bool,
    /// ID for the vector set.
    /// Empty for searching on the original index
    #[prost(string, tag="15")]
    pub vector_set: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="16")]
    pub key_filters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(float, tag="17")]
    pub min_score: f32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocumentVectorIdentifier {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocumentScored {
    #[prost(message, optional, tag="1")]
    pub doc_id: ::core::option::Option<DocumentVectorIdentifier>,
    #[prost(float, tag="2")]
    pub score: f32,
    #[prost(message, optional, tag="3")]
    pub metadata: ::core::option::Option<super::noderesources::SentenceMetadata>,
    #[prost(string, repeated, tag="4")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSearchResponse {
    /// List of docs closer to the asked one.
    #[prost(message, repeated, tag="1")]
    pub documents: ::prost::alloc::vec::Vec<DocumentScored>,
    /// What page is the answer.
    #[prost(int32, tag="4")]
    pub page_number: i32,
    /// How many results are in this page.
    #[prost(int32, tag="5")]
    pub result_per_page: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationNodeFilter {
    #[prost(enumeration="super::utils::relation_node::NodeType", tag="1")]
    pub node_type: i32,
    #[prost(string, optional, tag="2")]
    pub node_subtype: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationEdgeFilter {
    /// Will filter the search to edges of type ntype.
    #[prost(enumeration="super::utils::relation::RelationType", tag="1")]
    pub relation_type: i32,
    #[prost(string, optional, tag="2")]
    pub relation_subtype: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="3")]
    pub relation_value: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationPrefixSearchRequest {
    #[prost(string, tag="1")]
    pub prefix: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="2")]
    pub node_filters: ::prost::alloc::vec::Vec<RelationNodeFilter>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationPrefixSearchResponse {
    #[prost(message, repeated, tag="1")]
    pub nodes: ::prost::alloc::vec::Vec<super::utils::RelationNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntitiesSubgraphRequest {
    /// List of vertices where search will trigger
    #[prost(message, repeated, tag="1")]
    pub entry_points: ::prost::alloc::vec::Vec<super::utils::RelationNode>,
    #[prost(int32, optional, tag="3")]
    pub depth: ::core::option::Option<i32>,
    #[prost(message, repeated, tag="4")]
    pub deleted_entities: ::prost::alloc::vec::Vec<entities_subgraph_request::DeletedEntities>,
    #[prost(string, repeated, tag="5")]
    pub deleted_groups: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Nested message and enum types in `EntitiesSubgraphRequest`.
pub mod entities_subgraph_request {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DeletedEntities {
        #[prost(string, tag="1")]
        pub node_subtype: ::prost::alloc::string::String,
        #[prost(string, repeated, tag="2")]
        pub node_values: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntitiesSubgraphResponse {
    #[prost(message, repeated, tag="1")]
    pub relations: ::prost::alloc::vec::Vec<super::utils::Relation>,
}
/// Query relation index to obtain different information about the
/// knowledge graph. It can be queried using the following strategies:
///
/// - prefix search over vertex (node) names
/// - graph search:
///   - given some entry vertices, get the filtered subgraph around them
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationSearchRequest {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    #[deprecated]
    #[prost(bool, tag="5")]
    pub reload: bool,
    #[prost(message, optional, tag="11")]
    pub prefix: ::core::option::Option<RelationPrefixSearchRequest>,
    #[prost(message, optional, tag="12")]
    pub subgraph: ::core::option::Option<EntitiesSubgraphRequest>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationSearchResponse {
    #[prost(message, optional, tag="11")]
    pub prefix: ::core::option::Option<RelationPrefixSearchResponse>,
    #[prost(message, optional, tag="12")]
    pub subgraph: ::core::option::Option<EntitiesSubgraphResponse>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchRequest {
    #[prost(string, tag="1")]
    pub shard: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="2")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// query this text in all the paragraphs
    #[prost(string, tag="3")]
    pub body: ::prost::alloc::string::String,
    #[prost(message, optional, tag="4")]
    pub filter: ::core::option::Option<Filter>,
    #[prost(message, optional, tag="5")]
    pub order: ::core::option::Option<OrderBy>,
    #[prost(message, optional, tag="6")]
    pub faceted: ::core::option::Option<Faceted>,
    #[prost(int32, tag="7")]
    pub page_number: i32,
    #[prost(int32, tag="8")]
    pub result_per_page: i32,
    #[prost(message, optional, tag="9")]
    pub timestamps: ::core::option::Option<Timestamps>,
    /// Embedded vector search.
    #[prost(float, repeated, tag="10")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
    #[prost(string, tag="15")]
    pub vectorset: ::prost::alloc::string::String,
    #[deprecated]
    #[prost(bool, tag="11")]
    pub reload: bool,
    #[prost(bool, tag="12")]
    pub paragraph: bool,
    #[prost(bool, tag="13")]
    pub document: bool,
    #[prost(bool, tag="14")]
    pub with_duplicates: bool,
    #[prost(bool, tag="16")]
    pub only_faceted: bool,
    #[prost(string, optional, tag="18")]
    pub advanced_query: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(enumeration="super::noderesources::resource::ResourceStatus", optional, tag="17")]
    pub with_status: ::core::option::Option<i32>,
    /// if provided, search metadata for this nodes (nodes at distance
    /// one) and get the shortest path between nodes
    #[deprecated]
    #[prost(message, optional, tag="19")]
    pub relations: ::core::option::Option<RelationSearchRequest>,
    #[prost(message, optional, tag="20")]
    pub relation_prefix: ::core::option::Option<RelationPrefixSearchRequest>,
    #[prost(message, optional, tag="21")]
    pub relation_subgraph: ::core::option::Option<EntitiesSubgraphRequest>,
    #[prost(string, repeated, tag="22")]
    pub key_filters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(float, tag="23")]
    pub min_score: f32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuggestRequest {
    #[prost(string, tag="1")]
    pub shard: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub body: ::prost::alloc::string::String,
    #[prost(enumeration="SuggestFeatures", repeated, tag="6")]
    pub features: ::prost::alloc::vec::Vec<i32>,
    #[prost(message, optional, tag="3")]
    pub filter: ::core::option::Option<Filter>,
    #[prost(message, optional, tag="4")]
    pub timestamps: ::core::option::Option<Timestamps>,
    #[prost(string, repeated, tag="5")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelatedEntities {
    #[prost(string, repeated, tag="1")]
    pub entities: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(uint32, tag="2")]
    pub total: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuggestResponse {
    #[prost(int32, tag="1")]
    pub total: i32,
    #[prost(message, repeated, tag="2")]
    pub results: ::prost::alloc::vec::Vec<ParagraphResult>,
    /// The text that lead to this results
    #[prost(string, tag="3")]
    pub query: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="4")]
    pub ematches: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Entities related with the query
    #[prost(message, optional, tag="5")]
    pub entities: ::core::option::Option<RelatedEntities>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchResponse {
    #[prost(message, optional, tag="1")]
    pub document: ::core::option::Option<DocumentSearchResponse>,
    #[prost(message, optional, tag="2")]
    pub paragraph: ::core::option::Option<ParagraphSearchResponse>,
    #[prost(message, optional, tag="3")]
    pub vector: ::core::option::Option<VectorSearchResponse>,
    #[prost(message, optional, tag="4")]
    pub relation: ::core::option::Option<RelationSearchResponse>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IdCollection {
    #[prost(string, repeated, tag="1")]
    pub ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationEdge {
    #[prost(enumeration="super::utils::relation::RelationType", tag="1")]
    pub edge_type: i32,
    #[prost(string, tag="2")]
    pub property: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EdgeList {
    #[prost(message, repeated, tag="1")]
    pub list: ::prost::alloc::vec::Vec<RelationEdge>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationTypeListMember {
    #[prost(enumeration="super::utils::relation_node::NodeType", tag="1")]
    pub with_type: i32,
    #[prost(string, tag="2")]
    pub with_subtype: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TypeList {
    #[prost(message, repeated, tag="1")]
    pub list: ::prost::alloc::vec::Vec<RelationTypeListMember>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShardRequest {
    #[prost(message, optional, tag="1")]
    pub shard_id: ::core::option::Option<super::noderesources::ShardId>,
    #[prost(string, tag="2")]
    pub vectorset: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParagraphItem {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="2")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DocumentItem {
    #[prost(string, tag="1")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub field: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="3")]
    pub labels: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamRequest {
    #[deprecated]
    #[prost(message, optional, tag="1")]
    pub filter_deprecated: ::core::option::Option<Filter>,
    #[deprecated]
    #[prost(bool, tag="2")]
    pub reload: bool,
    #[prost(message, optional, tag="3")]
    pub shard_id: ::core::option::Option<super::noderesources::ShardId>,
    #[prost(message, optional, tag="4")]
    pub filter: ::core::option::Option<StreamFilter>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShardFilesRequest {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardFileList {
    #[prost(message, repeated, tag="2")]
    pub files: ::prost::alloc::vec::Vec<ShardFile>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardFile {
    #[prost(string, tag="1")]
    pub relative_path: ::prost::alloc::string::String,
    #[prost(uint64, tag="2")]
    pub size: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DownloadShardFileRequest {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub relative_path: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardFileChunk {
    #[prost(bytes="vec", tag="1")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(int32, tag="2")]
    pub index: i32,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SuggestFeatures {
    Entities = 0,
    Paragraphs = 1,
}
/// Generated client implementations.
pub mod node_reader_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct NodeReaderClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl NodeReaderClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> NodeReaderClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> NodeReaderClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            NodeReaderClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn get_shard(
            &mut self,
            request: impl tonic::IntoRequest<super::GetShardRequest>,
        ) -> Result<tonic::Response<super::super::noderesources::Shard>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/GetShard",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn document_search(
            &mut self,
            request: impl tonic::IntoRequest<super::DocumentSearchRequest>,
        ) -> Result<tonic::Response<super::DocumentSearchResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/DocumentSearch",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn paragraph_search(
            &mut self,
            request: impl tonic::IntoRequest<super::ParagraphSearchRequest>,
        ) -> Result<tonic::Response<super::ParagraphSearchResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/ParagraphSearch",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn vector_search(
            &mut self,
            request: impl tonic::IntoRequest<super::VectorSearchRequest>,
        ) -> Result<tonic::Response<super::VectorSearchResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/VectorSearch",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn relation_search(
            &mut self,
            request: impl tonic::IntoRequest<super::RelationSearchRequest>,
        ) -> Result<tonic::Response<super::RelationSearchResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/RelationSearch",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn document_ids(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::IdCollection>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/DocumentIds",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn paragraph_ids(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::IdCollection>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/ParagraphIds",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn vector_ids(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::IdCollection>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/VectorIds",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn relation_ids(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::IdCollection>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/RelationIds",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn relation_edges(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::EdgeList>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/RelationEdges",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn relation_types(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::TypeList>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/RelationTypes",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn search(
            &mut self,
            request: impl tonic::IntoRequest<super::SearchRequest>,
        ) -> Result<tonic::Response<super::SearchResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/Search",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn suggest(
            &mut self,
            request: impl tonic::IntoRequest<super::SuggestRequest>,
        ) -> Result<tonic::Response<super::SuggestResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/Suggest",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Streams
        pub async fn paragraphs(
            &mut self,
            request: impl tonic::IntoRequest<super::StreamRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::ParagraphItem>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/Paragraphs",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
        pub async fn documents(
            &mut self,
            request: impl tonic::IntoRequest<super::StreamRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::DocumentItem>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/Documents",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
        /// Shard Download
        pub async fn get_shard_files(
            &mut self,
            request: impl tonic::IntoRequest<super::GetShardFilesRequest>,
        ) -> Result<tonic::Response<super::ShardFileList>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/GetShardFiles",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn download_shard_file(
            &mut self,
            request: impl tonic::IntoRequest<super::DownloadShardFileRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::ShardFileChunk>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/nodereader.NodeReader/DownloadShardFile",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod node_reader_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with NodeReaderServer.
    #[async_trait]
    pub trait NodeReader: Send + Sync + 'static {
        async fn get_shard(
            &self,
            request: tonic::Request<super::GetShardRequest>,
        ) -> Result<tonic::Response<super::super::noderesources::Shard>, tonic::Status>;
        async fn document_search(
            &self,
            request: tonic::Request<super::DocumentSearchRequest>,
        ) -> Result<tonic::Response<super::DocumentSearchResponse>, tonic::Status>;
        async fn paragraph_search(
            &self,
            request: tonic::Request<super::ParagraphSearchRequest>,
        ) -> Result<tonic::Response<super::ParagraphSearchResponse>, tonic::Status>;
        async fn vector_search(
            &self,
            request: tonic::Request<super::VectorSearchRequest>,
        ) -> Result<tonic::Response<super::VectorSearchResponse>, tonic::Status>;
        async fn relation_search(
            &self,
            request: tonic::Request<super::RelationSearchRequest>,
        ) -> Result<tonic::Response<super::RelationSearchResponse>, tonic::Status>;
        async fn document_ids(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::IdCollection>, tonic::Status>;
        async fn paragraph_ids(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::IdCollection>, tonic::Status>;
        async fn vector_ids(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::IdCollection>, tonic::Status>;
        async fn relation_ids(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::IdCollection>, tonic::Status>;
        async fn relation_edges(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::EdgeList>, tonic::Status>;
        async fn relation_types(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::TypeList>, tonic::Status>;
        async fn search(
            &self,
            request: tonic::Request<super::SearchRequest>,
        ) -> Result<tonic::Response<super::SearchResponse>, tonic::Status>;
        async fn suggest(
            &self,
            request: tonic::Request<super::SuggestRequest>,
        ) -> Result<tonic::Response<super::SuggestResponse>, tonic::Status>;
        ///Server streaming response type for the Paragraphs method.
        type ParagraphsStream: futures_core::Stream<
                Item = Result<super::ParagraphItem, tonic::Status>,
            >
            + Send
            + 'static;
        /// Streams
        async fn paragraphs(
            &self,
            request: tonic::Request<super::StreamRequest>,
        ) -> Result<tonic::Response<Self::ParagraphsStream>, tonic::Status>;
        ///Server streaming response type for the Documents method.
        type DocumentsStream: futures_core::Stream<
                Item = Result<super::DocumentItem, tonic::Status>,
            >
            + Send
            + 'static;
        async fn documents(
            &self,
            request: tonic::Request<super::StreamRequest>,
        ) -> Result<tonic::Response<Self::DocumentsStream>, tonic::Status>;
        /// Shard Download
        async fn get_shard_files(
            &self,
            request: tonic::Request<super::GetShardFilesRequest>,
        ) -> Result<tonic::Response<super::ShardFileList>, tonic::Status>;
        ///Server streaming response type for the DownloadShardFile method.
        type DownloadShardFileStream: futures_core::Stream<
                Item = Result<super::ShardFileChunk, tonic::Status>,
            >
            + Send
            + 'static;
        async fn download_shard_file(
            &self,
            request: tonic::Request<super::DownloadShardFileRequest>,
        ) -> Result<tonic::Response<Self::DownloadShardFileStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct NodeReaderServer<T: NodeReader> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: NodeReader> NodeReaderServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for NodeReaderServer<T>
    where
        T: NodeReader,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/nodereader.NodeReader/GetShard" => {
                    #[allow(non_camel_case_types)]
                    struct GetShardSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::GetShardRequest>
                    for GetShardSvc<T> {
                        type Response = super::super::noderesources::Shard;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetShardRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_shard(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetShardSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/DocumentSearch" => {
                    #[allow(non_camel_case_types)]
                    struct DocumentSearchSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::DocumentSearchRequest>
                    for DocumentSearchSvc<T> {
                        type Response = super::DocumentSearchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DocumentSearchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).document_search(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DocumentSearchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/ParagraphSearch" => {
                    #[allow(non_camel_case_types)]
                    struct ParagraphSearchSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::ParagraphSearchRequest>
                    for ParagraphSearchSvc<T> {
                        type Response = super::ParagraphSearchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ParagraphSearchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).paragraph_search(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ParagraphSearchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/VectorSearch" => {
                    #[allow(non_camel_case_types)]
                    struct VectorSearchSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::VectorSearchRequest>
                    for VectorSearchSvc<T> {
                        type Response = super::VectorSearchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::VectorSearchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).vector_search(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = VectorSearchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/RelationSearch" => {
                    #[allow(non_camel_case_types)]
                    struct RelationSearchSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::RelationSearchRequest>
                    for RelationSearchSvc<T> {
                        type Response = super::RelationSearchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RelationSearchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).relation_search(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RelationSearchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/DocumentIds" => {
                    #[allow(non_camel_case_types)]
                    struct DocumentIdsSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for DocumentIdsSvc<T> {
                        type Response = super::IdCollection;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::noderesources::ShardId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).document_ids(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DocumentIdsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/ParagraphIds" => {
                    #[allow(non_camel_case_types)]
                    struct ParagraphIdsSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for ParagraphIdsSvc<T> {
                        type Response = super::IdCollection;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::noderesources::ShardId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).paragraph_ids(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ParagraphIdsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/VectorIds" => {
                    #[allow(non_camel_case_types)]
                    struct VectorIdsSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for VectorIdsSvc<T> {
                        type Response = super::IdCollection;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::noderesources::ShardId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).vector_ids(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = VectorIdsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/RelationIds" => {
                    #[allow(non_camel_case_types)]
                    struct RelationIdsSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for RelationIdsSvc<T> {
                        type Response = super::IdCollection;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::noderesources::ShardId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).relation_ids(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RelationIdsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/RelationEdges" => {
                    #[allow(non_camel_case_types)]
                    struct RelationEdgesSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for RelationEdgesSvc<T> {
                        type Response = super::EdgeList;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::noderesources::ShardId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).relation_edges(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RelationEdgesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/RelationTypes" => {
                    #[allow(non_camel_case_types)]
                    struct RelationTypesSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for RelationTypesSvc<T> {
                        type Response = super::TypeList;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::noderesources::ShardId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).relation_types(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RelationTypesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/Search" => {
                    #[allow(non_camel_case_types)]
                    struct SearchSvc<T: NodeReader>(pub Arc<T>);
                    impl<T: NodeReader> tonic::server::UnaryService<super::SearchRequest>
                    for SearchSvc<T> {
                        type Response = super::SearchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SearchRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).search(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SearchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/Suggest" => {
                    #[allow(non_camel_case_types)]
                    struct SuggestSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::SuggestRequest>
                    for SuggestSvc<T> {
                        type Response = super::SuggestResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SuggestRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).suggest(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SuggestSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/Paragraphs" => {
                    #[allow(non_camel_case_types)]
                    struct ParagraphsSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::ServerStreamingService<super::StreamRequest>
                    for ParagraphsSvc<T> {
                        type Response = super::ParagraphItem;
                        type ResponseStream = T::ParagraphsStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StreamRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).paragraphs(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ParagraphsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/Documents" => {
                    #[allow(non_camel_case_types)]
                    struct DocumentsSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::ServerStreamingService<super::StreamRequest>
                    for DocumentsSvc<T> {
                        type Response = super::DocumentItem;
                        type ResponseStream = T::DocumentsStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StreamRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).documents(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DocumentsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/GetShardFiles" => {
                    #[allow(non_camel_case_types)]
                    struct GetShardFilesSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<super::GetShardFilesRequest>
                    for GetShardFilesSvc<T> {
                        type Response = super::ShardFileList;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetShardFilesRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).get_shard_files(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetShardFilesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/nodereader.NodeReader/DownloadShardFile" => {
                    #[allow(non_camel_case_types)]
                    struct DownloadShardFileSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::ServerStreamingService<
                        super::DownloadShardFileRequest,
                    > for DownloadShardFileSvc<T> {
                        type Response = super::ShardFileChunk;
                        type ResponseStream = T::DownloadShardFileStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DownloadShardFileRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).download_shard_file(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DownloadShardFileSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: NodeReader> Clone for NodeReaderServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: NodeReader> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: NodeReader> tonic::transport::NamedService for NodeReaderServer<T> {
        const NAME: &'static str = "nodereader.NodeReader";
    }
}
