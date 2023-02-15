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
pub struct ParagraphPosition {
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
pub struct ParagraphMetadata {
    #[prost(message, optional, tag="1")]
    pub position: ::core::option::Option<ParagraphPosition>,
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
    #[prost(message, repeated, tag="10")]
    pub relations_to_delete: ::prost::alloc::vec::Vec<super::utils::Relation>,
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
    }
}
/// Generated client implementations.
pub mod shutdown_handler_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct ShutdownHandlerClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ShutdownHandlerClient<tonic::transport::Channel> {
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
    impl<T> ShutdownHandlerClient<T>
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
        ) -> ShutdownHandlerClient<InterceptedService<T, F>>
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
            ShutdownHandlerClient::new(InterceptedService::new(inner, interceptor))
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
        pub async fn shutdown(
            &mut self,
            request: impl tonic::IntoRequest<super::EmptyQuery>,
        ) -> Result<tonic::Response<super::EmptyResponse>, tonic::Status> {
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
                "/noderesources.ShutdownHandler/Shutdown",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod shutdown_handler_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with ShutdownHandlerServer.
    #[async_trait]
    pub trait ShutdownHandler: Send + Sync + 'static {
        async fn shutdown(
            &self,
            request: tonic::Request<super::EmptyQuery>,
        ) -> Result<tonic::Response<super::EmptyResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ShutdownHandlerServer<T: ShutdownHandler> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ShutdownHandler> ShutdownHandlerServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ShutdownHandlerServer<T>
    where
        T: ShutdownHandler,
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
                "/noderesources.ShutdownHandler/Shutdown" => {
                    #[allow(non_camel_case_types)]
                    struct ShutdownSvc<T: ShutdownHandler>(pub Arc<T>);
                    impl<
                        T: ShutdownHandler,
                    > tonic::server::UnaryService<super::EmptyQuery> for ShutdownSvc<T> {
                        type Response = super::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::EmptyQuery>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).shutdown(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ShutdownSvc(inner);
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
    impl<T: ShutdownHandler> Clone for ShutdownHandlerServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: ShutdownHandler> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ShutdownHandler> tonic::transport::NamedService
    for ShutdownHandlerServer<T> {
        const NAME: &'static str = "noderesources.ShutdownHandler";
    }
}
