#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Filter {
    #[prost(string, repeated, tag="1")]
    pub tags: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Faceted {
    #[prost(string, repeated, tag="1")]
    pub tags: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OrderBy {
    #[prost(string, tag="1")]
    pub field: ::prost::alloc::string::String,
    #[prost(enumeration="order_by::OrderType", tag="2")]
    pub r#type: i32,
}
/// Nested message and enum types in `OrderBy`.
pub mod order_by {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum OrderType {
        Desc = 0,
        Asc = 1,
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
    #[prost(bool, tag="10")]
    pub reload: bool,
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
    /// Faceted{ tags: Vec<String>}
    #[prost(message, optional, tag="8")]
    pub faceted: ::core::option::Option<Faceted>,
    #[prost(int32, tag="10")]
    pub page_number: i32,
    #[prost(int32, tag="11")]
    pub result_per_page: i32,
    #[prost(message, optional, tag="12")]
    pub timestamps: ::core::option::Option<Timestamps>,
    #[prost(bool, tag="13")]
    pub reload: bool,
    #[prost(bool, tag="14")]
    pub with_duplicates: bool,
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
    /// tags to filter
    #[prost(string, repeated, tag="3")]
    pub tags: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// What page is the answer.
    #[prost(int32, tag="4")]
    pub page_number: i32,
    /// How many results are in this page.
    #[prost(int32, tag="5")]
    pub result_per_page: i32,
    #[prost(bool, tag="14")]
    pub with_duplicates: bool,
    #[prost(bool, tag="13")]
    pub reload: bool,
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
/// Relation filters are used to make the 
/// search domain smaller. By providing filters the 
/// search may  be faster.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationFilter {
    /// Will filter the search to nodes of type ntype.
    #[prost(enumeration="super::utils::relation_node::NodeType", tag="1")]
    pub ntype: i32,
    /// Additionally the search can be even more specific by 
    /// providing a subtype. The empty string is a wilcard that 
    /// indicates to not filter by subtype. 
    #[prost(string, tag="2")]
    pub subtype: ::prost::alloc::string::String,
}
/// A request for the relation index.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationSearchRequest {
    ///Shard ID
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    /// A search will start from each of the entry points.
    /// Zero entry points are provided will trigger an iteration
    /// through all of the nodes.
    #[prost(message, repeated, tag="2")]
    pub entry_points: ::prost::alloc::vec::Vec<super::utils::RelationNode>,
    /// If needed, the search can be guided through 
    #[prost(message, repeated, tag="3")]
    pub type_filters: ::prost::alloc::vec::Vec<RelationFilter>,
    /// The user can impose a limit in the number of jumps
    /// the seach may perfom.
    #[prost(int32, tag="4")]
    pub depth: i32,
    /// Nodes can be filtered by prefix.
    #[prost(string, tag="5")]
    pub prefix: ::prost::alloc::string::String,
    #[prost(bool, tag="13")]
    pub reload: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationSearchResponse {
    #[prost(message, repeated, tag="1")]
    pub neighbours: ::prost::alloc::vec::Vec<super::utils::RelationNode>,
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
    /// Faceted{ tags: Vec<String>}
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
    #[prost(bool, tag="11")]
    pub reload: bool,
    #[prost(bool, tag="12")]
    pub paragraph: bool,
    #[prost(bool, tag="13")]
    pub document: bool,
    #[prost(bool, tag="14")]
    pub with_duplicates: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuggestRequest {
    #[prost(string, tag="1")]
    pub shard: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub body: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub filter: ::core::option::Option<Filter>,
    #[prost(message, optional, tag="4")]
    pub timestamps: ::core::option::Option<Timestamps>,
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
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
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
        pub async fn get_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::EmptyQuery>,
        ) -> Result<
            tonic::Response<super::super::noderesources::ShardList>,
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
                "/nodereader.NodeReader/GetShards",
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
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::super::noderesources::Shard>, tonic::Status>;
        async fn get_shards(
            &self,
            request: tonic::Request<super::super::noderesources::EmptyQuery>,
        ) -> Result<
            tonic::Response<super::super::noderesources::ShardList>,
            tonic::Status,
        >;
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
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for GetShardSvc<T> {
                        type Response = super::super::noderesources::Shard;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::noderesources::ShardId>,
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
                "/nodereader.NodeReader/GetShards" => {
                    #[allow(non_camel_case_types)]
                    struct GetShardsSvc<T: NodeReader>(pub Arc<T>);
                    impl<
                        T: NodeReader,
                    > tonic::server::UnaryService<
                        super::super::noderesources::EmptyQuery,
                    > for GetShardsSvc<T> {
                        type Response = super::super::noderesources::ShardList;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::noderesources::EmptyQuery,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_shards(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetShardsSvc(inner);
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
