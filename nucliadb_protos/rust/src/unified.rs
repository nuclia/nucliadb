#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardReplicationPosition {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    #[prost(uint64, tag="2")]
    pub position: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SecondaryReplicateRequest {
    #[prost(string, tag="1")]
    pub secondary_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="2")]
    pub positions: ::prost::alloc::vec::Vec<ShardReplicationPosition>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrimaryReplicateCommit {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    #[prost(uint64, tag="2")]
    pub position: u64,
    #[prost(string, repeated, tag="3")]
    pub segments: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrimaryReplicateResponse {
    #[prost(message, repeated, tag="1")]
    pub commits: ::prost::alloc::vec::Vec<PrimaryReplicateCommit>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DownloadSegmentRequest {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub segment_id: ::prost::alloc::string::String,
    #[prost(uint64, tag="3")]
    pub chunk_size: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DownloadSegmentResponse {
    #[prost(bytes="vec", tag="1")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, tag="2")]
    pub chunk: u32,
    #[prost(uint64, tag="3")]
    pub read_position: u64,
    #[prost(uint64, tag="4")]
    pub total_size: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateShardRequest {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateShardResponse {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardRequest {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardResponse {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchRequest {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub query: ::prost::alloc::string::String,
    #[prost(float, repeated, tag="3")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
    #[prost(uint64, tag="4")]
    pub limit: u64,
    #[prost(uint64, tag="5")]
    pub offset: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResultItem {
    #[prost(string, tag="1")]
    pub resource_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub field_id: ::prost::alloc::string::String,
    #[prost(float, tag="3")]
    pub score: f32,
    #[prost(string, tag="4")]
    pub score_type: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchResponse {
    #[prost(message, repeated, tag="1")]
    pub items: ::prost::alloc::vec::Vec<ResultItem>,
    #[prost(uint64, tag="2")]
    pub total: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexRequest {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub resource_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexResponse {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteRequest {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub resource_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteResponse {
}
/// Generated client implementations.
pub mod node_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct NodeServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl NodeServiceClient<tonic::transport::Channel> {
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
    impl<T> NodeServiceClient<T>
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
        ) -> NodeServiceClient<InterceptedService<T, F>>
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
            NodeServiceClient::new(InterceptedService::new(inner, interceptor))
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
        /// Shard replication RPCs
        pub async fn replicate(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::SecondaryReplicateRequest,
            >,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::PrimaryReplicateResponse>>,
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
                "/unified.NodeService/Replicate",
            );
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
        pub async fn download_segment(
            &mut self,
            request: impl tonic::IntoRequest<super::DownloadSegmentRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::DownloadSegmentResponse>>,
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
                "/unified.NodeService/DownloadSegment",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
        /// Shard management RPCs
        pub async fn create_shard(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateShardRequest>,
        ) -> Result<tonic::Response<super::CreateShardResponse>, tonic::Status> {
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
                "/unified.NodeService/CreateShard",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_shard(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteShardRequest>,
        ) -> Result<tonic::Response<super::DeleteShardResponse>, tonic::Status> {
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
                "/unified.NodeService/DeleteShard",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Indexing RPCs
        pub async fn index(
            &mut self,
            request: impl tonic::IntoRequest<super::IndexRequest>,
        ) -> Result<tonic::Response<super::IndexResponse>, tonic::Status> {
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
                "/unified.NodeService/Index",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteRequest>,
        ) -> Result<tonic::Response<super::DeleteResponse>, tonic::Status> {
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
                "/unified.NodeService/Delete",
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
                "/unified.NodeService/Search",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod node_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with NodeServiceServer.
    #[async_trait]
    pub trait NodeService: Send + Sync + 'static {
        ///Server streaming response type for the Replicate method.
        type ReplicateStream: futures_core::Stream<
                Item = Result<super::PrimaryReplicateResponse, tonic::Status>,
            >
            + Send
            + 'static;
        /// Shard replication RPCs
        async fn replicate(
            &self,
            request: tonic::Request<tonic::Streaming<super::SecondaryReplicateRequest>>,
        ) -> Result<tonic::Response<Self::ReplicateStream>, tonic::Status>;
        ///Server streaming response type for the DownloadSegment method.
        type DownloadSegmentStream: futures_core::Stream<
                Item = Result<super::DownloadSegmentResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn download_segment(
            &self,
            request: tonic::Request<super::DownloadSegmentRequest>,
        ) -> Result<tonic::Response<Self::DownloadSegmentStream>, tonic::Status>;
        /// Shard management RPCs
        async fn create_shard(
            &self,
            request: tonic::Request<super::CreateShardRequest>,
        ) -> Result<tonic::Response<super::CreateShardResponse>, tonic::Status>;
        async fn delete_shard(
            &self,
            request: tonic::Request<super::DeleteShardRequest>,
        ) -> Result<tonic::Response<super::DeleteShardResponse>, tonic::Status>;
        /// Indexing RPCs
        async fn index(
            &self,
            request: tonic::Request<super::IndexRequest>,
        ) -> Result<tonic::Response<super::IndexResponse>, tonic::Status>;
        async fn delete(
            &self,
            request: tonic::Request<super::DeleteRequest>,
        ) -> Result<tonic::Response<super::DeleteResponse>, tonic::Status>;
        async fn search(
            &self,
            request: tonic::Request<super::SearchRequest>,
        ) -> Result<tonic::Response<super::SearchResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct NodeServiceServer<T: NodeService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: NodeService> NodeServiceServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for NodeServiceServer<T>
    where
        T: NodeService,
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
                "/unified.NodeService/Replicate" => {
                    #[allow(non_camel_case_types)]
                    struct ReplicateSvc<T: NodeService>(pub Arc<T>);
                    impl<
                        T: NodeService,
                    > tonic::server::StreamingService<super::SecondaryReplicateRequest>
                    for ReplicateSvc<T> {
                        type Response = super::PrimaryReplicateResponse;
                        type ResponseStream = T::ReplicateStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::SecondaryReplicateRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).replicate(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ReplicateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/unified.NodeService/DownloadSegment" => {
                    #[allow(non_camel_case_types)]
                    struct DownloadSegmentSvc<T: NodeService>(pub Arc<T>);
                    impl<
                        T: NodeService,
                    > tonic::server::ServerStreamingService<
                        super::DownloadSegmentRequest,
                    > for DownloadSegmentSvc<T> {
                        type Response = super::DownloadSegmentResponse;
                        type ResponseStream = T::DownloadSegmentStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DownloadSegmentRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).download_segment(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DownloadSegmentSvc(inner);
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
                "/unified.NodeService/CreateShard" => {
                    #[allow(non_camel_case_types)]
                    struct CreateShardSvc<T: NodeService>(pub Arc<T>);
                    impl<
                        T: NodeService,
                    > tonic::server::UnaryService<super::CreateShardRequest>
                    for CreateShardSvc<T> {
                        type Response = super::CreateShardResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateShardRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).create_shard(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateShardSvc(inner);
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
                "/unified.NodeService/DeleteShard" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteShardSvc<T: NodeService>(pub Arc<T>);
                    impl<
                        T: NodeService,
                    > tonic::server::UnaryService<super::DeleteShardRequest>
                    for DeleteShardSvc<T> {
                        type Response = super::DeleteShardResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteShardRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).delete_shard(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteShardSvc(inner);
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
                "/unified.NodeService/Index" => {
                    #[allow(non_camel_case_types)]
                    struct IndexSvc<T: NodeService>(pub Arc<T>);
                    impl<T: NodeService> tonic::server::UnaryService<super::IndexRequest>
                    for IndexSvc<T> {
                        type Response = super::IndexResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::IndexRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).index(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = IndexSvc(inner);
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
                "/unified.NodeService/Delete" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteSvc<T: NodeService>(pub Arc<T>);
                    impl<
                        T: NodeService,
                    > tonic::server::UnaryService<super::DeleteRequest>
                    for DeleteSvc<T> {
                        type Response = super::DeleteResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).delete(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteSvc(inner);
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
                "/unified.NodeService/Search" => {
                    #[allow(non_camel_case_types)]
                    struct SearchSvc<T: NodeService>(pub Arc<T>);
                    impl<
                        T: NodeService,
                    > tonic::server::UnaryService<super::SearchRequest>
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
    impl<T: NodeService> Clone for NodeServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: NodeService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: NodeService> tonic::transport::NamedService for NodeServiceServer<T> {
        const NAME: &'static str = "unified.NodeService";
    }
}
