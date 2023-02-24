#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpStatus {
    #[prost(enumeration="op_status::Status", tag="1")]
    pub status: i32,
    #[prost(string, tag="2")]
    pub detail: ::prost::alloc::string::String,
    #[prost(uint64, tag="3")]
    pub count: u64,
    #[prost(uint64, tag="5")]
    pub count_paragraphs: u64,
    #[prost(uint64, tag="6")]
    pub count_sentences: u64,
    #[prost(string, tag="4")]
    pub shard_id: ::prost::alloc::string::String,
}
/// Nested message and enum types in `OpStatus`.
pub mod op_status {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Warning = 1,
        Error = 2,
    }
}
// Implemented at nucliadb_object_storage

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexMessage {
    #[prost(string, tag="1")]
    pub node: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub shard: ::prost::alloc::string::String,
    #[prost(uint64, tag="3")]
    pub txid: u64,
    #[prost(string, tag="4")]
    pub resource: ::prost::alloc::string::String,
    #[prost(enumeration="index_message::TypeMessage", tag="5")]
    pub typemessage: i32,
    #[prost(string, tag="6")]
    pub reindex_id: ::prost::alloc::string::String,
}
/// Nested message and enum types in `IndexMessage`.
pub mod index_message {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum TypeMessage {
        Creation = 0,
        Deletion = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetGraph {
    #[prost(message, optional, tag="1")]
    pub shard_id: ::core::option::Option<super::noderesources::ShardId>,
    #[prost(message, optional, tag="2")]
    pub graph: ::core::option::Option<super::utils::JoinGraph>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteGraphNodes {
    #[prost(message, optional, tag="2")]
    pub shard_id: ::core::option::Option<super::noderesources::ShardId>,
    #[prost(message, repeated, tag="1")]
    pub nodes: ::prost::alloc::vec::Vec<super::utils::RelationNode>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MoveShardRequest {
    #[prost(message, optional, tag="1")]
    pub shard_id: ::core::option::Option<super::noderesources::ShardId>,
    #[prost(string, tag="2")]
    pub address: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcceptShardRequest {
    #[prost(message, optional, tag="1")]
    pub shard_id: ::core::option::Option<super::noderesources::ShardId>,
    #[prost(uint32, tag="2")]
    pub port: u32,
    #[prost(bool, tag="3")]
    pub override_shard: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NewShardRequest {
    #[prost(enumeration="new_shard_request::VectorSimilarity", tag="1")]
    pub similarity: i32,
    #[prost(string, tag="2")]
    pub kbid: ::prost::alloc::string::String,
}
/// Nested message and enum types in `NewShardRequest`.
pub mod new_shard_request {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum VectorSimilarity {
        Cosine = 0,
        Dot = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Counter {
    #[prost(uint64, tag="1")]
    pub resources: u64,
    #[prost(uint64, tag="2")]
    pub paragraphs: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShadowShardResponse {
    #[prost(bool, tag="1")]
    pub success: bool,
    #[prost(message, optional, tag="2")]
    pub shard: ::core::option::Option<super::noderesources::ShardId>,
}
/// Generated client implementations.
pub mod node_writer_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct NodeWriterClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl NodeWriterClient<tonic::transport::Channel> {
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
    impl<T> NodeWriterClient<T>
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
        ) -> NodeWriterClient<InterceptedService<T, F>>
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
            NodeWriterClient::new(InterceptedService::new(inner, interceptor))
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
        ) -> Result<
            tonic::Response<super::super::noderesources::ShardId>,
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
                "/nodewriter.NodeWriter/GetShard",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn new_shard(
            &mut self,
            request: impl tonic::IntoRequest<super::NewShardRequest>,
        ) -> Result<
            tonic::Response<super::super::noderesources::ShardCreated>,
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
                "/nodewriter.NodeWriter/NewShard",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn clean_and_upgrade_shard(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
        ) -> Result<
            tonic::Response<super::super::noderesources::ShardCleaned>,
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
                "/nodewriter.NodeWriter/CleanAndUpgradeShard",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_shard(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
        ) -> Result<
            tonic::Response<super::super::noderesources::ShardId>,
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
                "/nodewriter.NodeWriter/DeleteShard",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn list_shards(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::EmptyQuery>,
        ) -> Result<
            tonic::Response<super::super::noderesources::ShardIds>,
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
                "/nodewriter.NodeWriter/ListShards",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn gc(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
        ) -> Result<
            tonic::Response<super::super::noderesources::EmptyResponse>,
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
            let path = http::uri::PathAndQuery::from_static("/nodewriter.NodeWriter/GC");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn set_resource(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::Resource>,
        ) -> Result<tonic::Response<super::OpStatus>, tonic::Status> {
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
                "/nodewriter.NodeWriter/SetResource",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_relation_nodes(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteGraphNodes>,
        ) -> Result<tonic::Response<super::OpStatus>, tonic::Status> {
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
                "/nodewriter.NodeWriter/DeleteRelationNodes",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn join_graph(
            &mut self,
            request: impl tonic::IntoRequest<super::SetGraph>,
        ) -> Result<tonic::Response<super::OpStatus>, tonic::Status> {
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
                "/nodewriter.NodeWriter/JoinGraph",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn remove_resource(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ResourceId>,
        ) -> Result<tonic::Response<super::OpStatus>, tonic::Status> {
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
                "/nodewriter.NodeWriter/RemoveResource",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn add_vector_set(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::VectorSetId>,
        ) -> Result<tonic::Response<super::OpStatus>, tonic::Status> {
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
                "/nodewriter.NodeWriter/AddVectorSet",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn remove_vector_set(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::VectorSetId>,
        ) -> Result<tonic::Response<super::OpStatus>, tonic::Status> {
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
                "/nodewriter.NodeWriter/RemoveVectorSet",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn list_vector_sets(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
        ) -> Result<
            tonic::Response<super::super::noderesources::VectorSetList>,
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
                "/nodewriter.NodeWriter/ListVectorSets",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn move_shard(
            &mut self,
            request: impl tonic::IntoRequest<super::MoveShardRequest>,
        ) -> Result<
            tonic::Response<super::super::noderesources::EmptyResponse>,
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
                "/nodewriter.NodeWriter/MoveShard",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn accept_shard(
            &mut self,
            request: impl tonic::IntoRequest<super::AcceptShardRequest>,
        ) -> Result<
            tonic::Response<super::super::noderesources::EmptyResponse>,
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
                "/nodewriter.NodeWriter/AcceptShard",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated client implementations.
pub mod node_sidecar_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct NodeSidecarClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl NodeSidecarClient<tonic::transport::Channel> {
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
    impl<T> NodeSidecarClient<T>
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
        ) -> NodeSidecarClient<InterceptedService<T, F>>
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
            NodeSidecarClient::new(InterceptedService::new(inner, interceptor))
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
        pub async fn get_count(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::Counter>, tonic::Status> {
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
                "/nodewriter.NodeSidecar/GetCount",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn create_shadow_shard(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::EmptyQuery>,
        ) -> Result<tonic::Response<super::ShadowShardResponse>, tonic::Status> {
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
                "/nodewriter.NodeSidecar/CreateShadowShard",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_shadow_shard(
            &mut self,
            request: impl tonic::IntoRequest<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::ShadowShardResponse>, tonic::Status> {
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
                "/nodewriter.NodeSidecar/DeleteShadowShard",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod node_writer_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with NodeWriterServer.
    #[async_trait]
    pub trait NodeWriter: Send + Sync + 'static {
        async fn get_shard(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<
            tonic::Response<super::super::noderesources::ShardId>,
            tonic::Status,
        >;
        async fn new_shard(
            &self,
            request: tonic::Request<super::NewShardRequest>,
        ) -> Result<
            tonic::Response<super::super::noderesources::ShardCreated>,
            tonic::Status,
        >;
        async fn clean_and_upgrade_shard(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<
            tonic::Response<super::super::noderesources::ShardCleaned>,
            tonic::Status,
        >;
        async fn delete_shard(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<
            tonic::Response<super::super::noderesources::ShardId>,
            tonic::Status,
        >;
        async fn list_shards(
            &self,
            request: tonic::Request<super::super::noderesources::EmptyQuery>,
        ) -> Result<
            tonic::Response<super::super::noderesources::ShardIds>,
            tonic::Status,
        >;
        async fn gc(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<
            tonic::Response<super::super::noderesources::EmptyResponse>,
            tonic::Status,
        >;
        async fn set_resource(
            &self,
            request: tonic::Request<super::super::noderesources::Resource>,
        ) -> Result<tonic::Response<super::OpStatus>, tonic::Status>;
        async fn delete_relation_nodes(
            &self,
            request: tonic::Request<super::DeleteGraphNodes>,
        ) -> Result<tonic::Response<super::OpStatus>, tonic::Status>;
        async fn join_graph(
            &self,
            request: tonic::Request<super::SetGraph>,
        ) -> Result<tonic::Response<super::OpStatus>, tonic::Status>;
        async fn remove_resource(
            &self,
            request: tonic::Request<super::super::noderesources::ResourceId>,
        ) -> Result<tonic::Response<super::OpStatus>, tonic::Status>;
        async fn add_vector_set(
            &self,
            request: tonic::Request<super::super::noderesources::VectorSetId>,
        ) -> Result<tonic::Response<super::OpStatus>, tonic::Status>;
        async fn remove_vector_set(
            &self,
            request: tonic::Request<super::super::noderesources::VectorSetId>,
        ) -> Result<tonic::Response<super::OpStatus>, tonic::Status>;
        async fn list_vector_sets(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<
            tonic::Response<super::super::noderesources::VectorSetList>,
            tonic::Status,
        >;
        async fn move_shard(
            &self,
            request: tonic::Request<super::MoveShardRequest>,
        ) -> Result<
            tonic::Response<super::super::noderesources::EmptyResponse>,
            tonic::Status,
        >;
        async fn accept_shard(
            &self,
            request: tonic::Request<super::AcceptShardRequest>,
        ) -> Result<
            tonic::Response<super::super::noderesources::EmptyResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct NodeWriterServer<T: NodeWriter> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: NodeWriter> NodeWriterServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for NodeWriterServer<T>
    where
        T: NodeWriter,
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
                "/nodewriter.NodeWriter/GetShard" => {
                    #[allow(non_camel_case_types)]
                    struct GetShardSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for GetShardSvc<T> {
                        type Response = super::super::noderesources::ShardId;
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
                "/nodewriter.NodeWriter/NewShard" => {
                    #[allow(non_camel_case_types)]
                    struct NewShardSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<super::NewShardRequest>
                    for NewShardSvc<T> {
                        type Response = super::super::noderesources::ShardCreated;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::NewShardRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).new_shard(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = NewShardSvc(inner);
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
                "/nodewriter.NodeWriter/CleanAndUpgradeShard" => {
                    #[allow(non_camel_case_types)]
                    struct CleanAndUpgradeShardSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for CleanAndUpgradeShardSvc<T> {
                        type Response = super::super::noderesources::ShardCleaned;
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
                                (*inner).clean_and_upgrade_shard(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CleanAndUpgradeShardSvc(inner);
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
                "/nodewriter.NodeWriter/DeleteShard" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteShardSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for DeleteShardSvc<T> {
                        type Response = super::super::noderesources::ShardId;
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
                "/nodewriter.NodeWriter/ListShards" => {
                    #[allow(non_camel_case_types)]
                    struct ListShardsSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<
                        super::super::noderesources::EmptyQuery,
                    > for ListShardsSvc<T> {
                        type Response = super::super::noderesources::ShardIds;
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
                            let fut = async move { (*inner).list_shards(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListShardsSvc(inner);
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
                "/nodewriter.NodeWriter/GC" => {
                    #[allow(non_camel_case_types)]
                    struct GCSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for GCSvc<T> {
                        type Response = super::super::noderesources::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::noderesources::ShardId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).gc(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GCSvc(inner);
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
                "/nodewriter.NodeWriter/SetResource" => {
                    #[allow(non_camel_case_types)]
                    struct SetResourceSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<super::super::noderesources::Resource>
                    for SetResourceSvc<T> {
                        type Response = super::OpStatus;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::noderesources::Resource,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).set_resource(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SetResourceSvc(inner);
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
                "/nodewriter.NodeWriter/DeleteRelationNodes" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteRelationNodesSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<super::DeleteGraphNodes>
                    for DeleteRelationNodesSvc<T> {
                        type Response = super::OpStatus;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteGraphNodes>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).delete_relation_nodes(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteRelationNodesSvc(inner);
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
                "/nodewriter.NodeWriter/JoinGraph" => {
                    #[allow(non_camel_case_types)]
                    struct JoinGraphSvc<T: NodeWriter>(pub Arc<T>);
                    impl<T: NodeWriter> tonic::server::UnaryService<super::SetGraph>
                    for JoinGraphSvc<T> {
                        type Response = super::OpStatus;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SetGraph>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).join_graph(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = JoinGraphSvc(inner);
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
                "/nodewriter.NodeWriter/RemoveResource" => {
                    #[allow(non_camel_case_types)]
                    struct RemoveResourceSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<
                        super::super::noderesources::ResourceId,
                    > for RemoveResourceSvc<T> {
                        type Response = super::OpStatus;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::noderesources::ResourceId,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).remove_resource(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RemoveResourceSvc(inner);
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
                "/nodewriter.NodeWriter/AddVectorSet" => {
                    #[allow(non_camel_case_types)]
                    struct AddVectorSetSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<
                        super::super::noderesources::VectorSetId,
                    > for AddVectorSetSvc<T> {
                        type Response = super::OpStatus;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::noderesources::VectorSetId,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).add_vector_set(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AddVectorSetSvc(inner);
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
                "/nodewriter.NodeWriter/RemoveVectorSet" => {
                    #[allow(non_camel_case_types)]
                    struct RemoveVectorSetSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<
                        super::super::noderesources::VectorSetId,
                    > for RemoveVectorSetSvc<T> {
                        type Response = super::OpStatus;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::noderesources::VectorSetId,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).remove_vector_set(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RemoveVectorSetSvc(inner);
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
                "/nodewriter.NodeWriter/ListVectorSets" => {
                    #[allow(non_camel_case_types)]
                    struct ListVectorSetsSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for ListVectorSetsSvc<T> {
                        type Response = super::super::noderesources::VectorSetList;
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
                                (*inner).list_vector_sets(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListVectorSetsSvc(inner);
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
                "/nodewriter.NodeWriter/MoveShard" => {
                    #[allow(non_camel_case_types)]
                    struct MoveShardSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<super::MoveShardRequest>
                    for MoveShardSvc<T> {
                        type Response = super::super::noderesources::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MoveShardRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).move_shard(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = MoveShardSvc(inner);
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
                "/nodewriter.NodeWriter/AcceptShard" => {
                    #[allow(non_camel_case_types)]
                    struct AcceptShardSvc<T: NodeWriter>(pub Arc<T>);
                    impl<
                        T: NodeWriter,
                    > tonic::server::UnaryService<super::AcceptShardRequest>
                    for AcceptShardSvc<T> {
                        type Response = super::super::noderesources::EmptyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AcceptShardRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).accept_shard(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AcceptShardSvc(inner);
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
    impl<T: NodeWriter> Clone for NodeWriterServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: NodeWriter> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: NodeWriter> tonic::transport::NamedService for NodeWriterServer<T> {
        const NAME: &'static str = "nodewriter.NodeWriter";
    }
}
/// Generated server implementations.
pub mod node_sidecar_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with NodeSidecarServer.
    #[async_trait]
    pub trait NodeSidecar: Send + Sync + 'static {
        async fn get_count(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::Counter>, tonic::Status>;
        async fn create_shadow_shard(
            &self,
            request: tonic::Request<super::super::noderesources::EmptyQuery>,
        ) -> Result<tonic::Response<super::ShadowShardResponse>, tonic::Status>;
        async fn delete_shadow_shard(
            &self,
            request: tonic::Request<super::super::noderesources::ShardId>,
        ) -> Result<tonic::Response<super::ShadowShardResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct NodeSidecarServer<T: NodeSidecar> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: NodeSidecar> NodeSidecarServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for NodeSidecarServer<T>
    where
        T: NodeSidecar,
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
                "/nodewriter.NodeSidecar/GetCount" => {
                    #[allow(non_camel_case_types)]
                    struct GetCountSvc<T: NodeSidecar>(pub Arc<T>);
                    impl<
                        T: NodeSidecar,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for GetCountSvc<T> {
                        type Response = super::Counter;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::noderesources::ShardId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_count(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetCountSvc(inner);
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
                "/nodewriter.NodeSidecar/CreateShadowShard" => {
                    #[allow(non_camel_case_types)]
                    struct CreateShadowShardSvc<T: NodeSidecar>(pub Arc<T>);
                    impl<
                        T: NodeSidecar,
                    > tonic::server::UnaryService<
                        super::super::noderesources::EmptyQuery,
                    > for CreateShadowShardSvc<T> {
                        type Response = super::ShadowShardResponse;
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
                                (*inner).create_shadow_shard(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateShadowShardSvc(inner);
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
                "/nodewriter.NodeSidecar/DeleteShadowShard" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteShadowShardSvc<T: NodeSidecar>(pub Arc<T>);
                    impl<
                        T: NodeSidecar,
                    > tonic::server::UnaryService<super::super::noderesources::ShardId>
                    for DeleteShadowShardSvc<T> {
                        type Response = super::ShadowShardResponse;
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
                                (*inner).delete_shadow_shard(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteShadowShardSvc(inner);
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
    impl<T: NodeSidecar> Clone for NodeSidecarServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: NodeSidecar> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: NodeSidecar> tonic::transport::NamedService for NodeSidecarServer<T> {
        const NAME: &'static str = "nodewriter.NodeSidecar";
    }
}
