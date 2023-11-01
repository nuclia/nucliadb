#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrimaryShardReplicationState {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    /// ID to identify the generation of the shard to know
    /// if there is a new version of the shard available to download
    #[prost(string, tag="2")]
    pub generation_id: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub similarity: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SecondaryShardReplicationState {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    /// ID to identify the generation of the shard to know
    /// if there is a new version of the shard available to download
    #[prost(string, tag="2")]
    pub generation_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SecondaryCheckReplicationStateRequest {
    #[prost(string, tag="1")]
    pub secondary_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="2")]
    pub shard_states: ::prost::alloc::vec::Vec<SecondaryShardReplicationState>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrimaryCheckReplicationStateResponse {
    #[prost(message, repeated, tag="1")]
    pub shard_states: ::prost::alloc::vec::Vec<PrimaryShardReplicationState>,
    #[prost(string, repeated, tag="2")]
    pub shards_to_remove: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="3")]
    pub primary_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SegmentIds {
    #[prost(string, repeated, tag="1")]
    pub items: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateShardRequest {
    #[prost(string, tag="1")]
    pub shard_id: ::prost::alloc::string::String,
    /// list of existing segment ids so we replicate same shards again
    #[prost(map="string, message", tag="2")]
    pub existing_segment_ids: ::std::collections::HashMap<::prost::alloc::string::String, SegmentIds>,
    #[prost(uint64, tag="3")]
    pub chunk_size: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateShardResponse {
    #[prost(string, tag="1")]
    pub generation_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub filepath: ::prost::alloc::string::String,
    #[prost(bytes="vec", tag="3")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, tag="4")]
    pub chunk: u32,
    #[prost(uint64, tag="5")]
    pub read_position: u64,
    #[prost(uint64, tag="6")]
    pub total_size: u64,
}
/// Generated client implementations.
pub mod replication_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct ReplicationServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ReplicationServiceClient<tonic::transport::Channel> {
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
    impl<T> ReplicationServiceClient<T>
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
        ) -> ReplicationServiceClient<InterceptedService<T, F>>
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
            ReplicationServiceClient::new(InterceptedService::new(inner, interceptor))
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
        pub async fn check_replication_state(
            &mut self,
            request: impl tonic::IntoRequest<
                super::SecondaryCheckReplicationStateRequest,
            >,
        ) -> Result<
            tonic::Response<super::PrimaryCheckReplicationStateResponse>,
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
                "/replication.ReplicationService/CheckReplicationState",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn replicate_shard(
            &mut self,
            request: impl tonic::IntoRequest<super::ReplicateShardRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::ReplicateShardResponse>>,
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
                "/replication.ReplicationService/ReplicateShard",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
        pub async fn get_metadata(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::EmptyQuery>,
        ) -> Result<
            tonic::Response<super::super::noderesources::NodeMetadata>,
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
                "/replication.ReplicationService/GetMetadata",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod replication_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with ReplicationServiceServer.
    #[async_trait]
    pub trait ReplicationService: Send + Sync + 'static {
        /// Shard replication RPCs
        async fn check_replication_state(
            &self,
            request: tonic::Request<super::SecondaryCheckReplicationStateRequest>,
        ) -> Result<
            tonic::Response<super::PrimaryCheckReplicationStateResponse>,
            tonic::Status,
        >;
        ///Server streaming response type for the ReplicateShard method.
        type ReplicateShardStream: futures_core::Stream<
                Item = Result<super::ReplicateShardResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn replicate_shard(
            &self,
            request: tonic::Request<super::ReplicateShardRequest>,
        ) -> Result<tonic::Response<Self::ReplicateShardStream>, tonic::Status>;
        async fn get_metadata(
            &self,
            request: tonic::Request<super::super::noderesources::EmptyQuery>,
        ) -> Result<
            tonic::Response<super::super::noderesources::NodeMetadata>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct ReplicationServiceServer<T: ReplicationService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ReplicationService> ReplicationServiceServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ReplicationServiceServer<T>
    where
        T: ReplicationService,
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
                "/replication.ReplicationService/CheckReplicationState" => {
                    #[allow(non_camel_case_types)]
                    struct CheckReplicationStateSvc<T: ReplicationService>(pub Arc<T>);
                    impl<
                        T: ReplicationService,
                    > tonic::server::UnaryService<
                        super::SecondaryCheckReplicationStateRequest,
                    > for CheckReplicationStateSvc<T> {
                        type Response = super::PrimaryCheckReplicationStateResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::SecondaryCheckReplicationStateRequest,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).check_replication_state(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CheckReplicationStateSvc(inner);
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
                "/replication.ReplicationService/ReplicateShard" => {
                    #[allow(non_camel_case_types)]
                    struct ReplicateShardSvc<T: ReplicationService>(pub Arc<T>);
                    impl<
                        T: ReplicationService,
                    > tonic::server::ServerStreamingService<super::ReplicateShardRequest>
                    for ReplicateShardSvc<T> {
                        type Response = super::ReplicateShardResponse;
                        type ResponseStream = T::ReplicateShardStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReplicateShardRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).replicate_shard(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ReplicateShardSvc(inner);
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
                "/replication.ReplicationService/GetMetadata" => {
                    #[allow(non_camel_case_types)]
                    struct GetMetadataSvc<T: ReplicationService>(pub Arc<T>);
                    impl<
                        T: ReplicationService,
                    > tonic::server::UnaryService<
                        super::super::noderesources::EmptyQuery,
                    > for GetMetadataSvc<T> {
                        type Response = super::super::noderesources::NodeMetadata;
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
                            let fut = async move {
                                (*inner).get_metadata(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetMetadataSvc(inner);
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
    impl<T: ReplicationService> Clone for ReplicationServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: ReplicationService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ReplicationService> tonic::transport::NamedService
    for ReplicationServiceServer<T> {
        const NAME: &'static str = "replication.ReplicationService";
    }
}
