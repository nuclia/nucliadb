// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::collections::{HashMap, HashSet};
use std::{pin::Pin, sync::Arc};

use futures::Stream;
use nidx_protos::nidx::nidx_searcher_client::NidxSearcherClient;
use nidx_protos::nidx::nidx_searcher_server::{NidxSearcher, NidxSearcherServer};
use nidx_protos::*;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tonic::Code;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Request, Response, Result, Status, service::Routes};
use tracing::field::Empty;
use uuid::Uuid;

use crate::errors::{NidxError, NidxResult};
use crate::searcher::shard_merge::{Limit, OrderBy};
use crate::searcher::shard_search::PartialResponse;
use crate::searcher::shard_selector::SearcherNode;

use super::shard_selector::ShardSelector;
use super::streams;
use super::{index_cache::IndexCache, shard_merge, shard_search, shard_suggest, shard_text};
use tracing::*;

/// When this header is set, we only try to serve the request from the local node
const HEADER_LOCAL_ONLY: &str = "nidx-local-only";

#[derive(Clone)]
struct TelemetryInterceptor;
#[cfg(feature = "telemetry")]
impl Interceptor for TelemetryInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> std::result::Result<tonic::Request<()>, Status> {
        crate::telemetry::middleware::add_telemetry_headers(request)
    }
}
#[cfg(not(feature = "telemetry"))]
impl Interceptor for TelemetryInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> std::result::Result<tonic::Request<()>, Status> {
        Ok(request)
    }
}

type SearcherClient = NidxSearcherClient<InterceptedService<Channel, TelemetryInterceptor>>;

pub struct SearchServer {
    index_cache: Arc<IndexCache>,
    shard_selector: ShardSelector,
    clients: Arc<RwLock<HashMap<String, SearcherClient>>>,
}

impl SearchServer {
    pub fn new(index_cache: Arc<IndexCache>, shard_selector: ShardSelector) -> Self {
        SearchServer {
            index_cache,
            shard_selector,
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn into_router(self) -> axum::Router {
        Routes::new(NidxSearcherServer::new(self)).into_axum_router()
    }

    async fn get_client(&self, hostname: &String) -> NidxResult<SearcherClient> {
        if let Some(client) = self.clients.read().await.get(hostname) {
            return Ok(client.clone());
        }

        let transport = tonic::transport::Endpoint::new(format!("http://{hostname}"))?
            .connect()
            .await?;
        let client = NidxSearcherClient::with_interceptor(transport, TelemetryInterceptor);
        self.clients.write().await.insert(hostname.clone(), client.clone());
        Ok(client)
    }
}

/// Macro to run a request on a shard, local or remote
macro_rules! shard_request {
    {
        $name:literal,
        $self:ident,
        $request:ident,
        $shard_id:ident,
        LOCAL => $local:expr,
        REMOTE => $remote_method:ident
        $(, STREAM => $stream_type:ty)?
    } => {{
        let mut errors = Vec::new();
        let local_only = $request.metadata().contains_key(HEADER_LOCAL_ONLY);
        let nodes = if local_only {
            vec![SearcherNode::This]
        } else {
            $self.shard_selector.nodes_for_shard(&$shard_id)
        };
        for node in &nodes {
            debug!(?node, ?$shard_id, concat!("Attempting ", $name));
            let result = match node {
                SearcherNode::This => {
                    $local
                        $(.map(|r| -> $stream_type { Box::pin(futures::stream::iter(r.map(Result::Ok))) }))?
                        .map(Response::new)
                }
                SearcherNode::Remote(hostname) => {
                    match $self.get_client(hostname).await {
                        Ok(mut client) => {
                            let mut request = Request::new($request.get_ref().clone());
                            request.metadata_mut().insert(HEADER_LOCAL_ONLY, "1".parse().unwrap());
                            client.$remote_method(request)
                                .await
                                .map_err(|e| match e.code() {
                                    tonic::Code::NotFound => NidxError::NotFound,
                                    _ => NidxError::Unknown(anyhow::Error::from(e)),
                                })
                                $(.map(|response| -> Response<$stream_type> { Response::new(Box::pin(response.into_inner())) }))?
                        },
                        Err(e) => Err(e)
                    }
                }
            };
            match result {
                Ok(response) => return Ok(response),
                Err(e) => {
                    warn!(?node, ?$shard_id, concat!("Error in ", $name, ", trying with next node: {:?}"), e);
                    errors.push(e);
                }
            }
        }

        error!(?errors, ?nodes, ?$shard_id, concat!("Error in ", $name, ", exhausted all availabled nodes"));
        if let Some(reported_error) = errors.pop() {
            return Err(reported_error.into());
        } else {
            return Err(Status::internal("Unknown search error"));
        }
    }};
}

#[tonic::async_trait]
impl NidxSearcher for SearchServer {
    async fn search(&self, request: Request<SearchRequest>) -> Result<Response<SearchResponse>> {
        let coordinator = !request.metadata().contains_key(HEADER_LOCAL_ONLY);
        let request = request.into_inner();

        let mut shards = vec![];
        for shard_id in &request.shard_ids {
            let shard_id = Uuid::parse_str(shard_id).map_err(NidxError::from)?;
            shards.push(shard_id);
        }

        let response = if coordinator {
            // We are the requested searcher to perform a distributed query
            let order_by = OrderBy::from(&request);
            let limit = Limit(request.result_per_page as usize);
            let partitions = self.distributed_query::<SearchOp>(request, shards).await?;
            SearchOp::merge(partitions, order_by, limit)
        } else {
            // This is a hopped node, i.e., another searcher has delegated this part of the query to
            // us. We must query our shards and return either a full or partial response or an error.
            // We won't hop to any other node
            SearchOp::local_query(Arc::clone(&self.index_cache), request, shards)
                .await?
                .data
        };

        Ok(Response::new(response))
    }

    async fn suggest(&self, request: Request<SuggestRequest>) -> Result<Response<SuggestResponse>> {
        let message = request.get_ref();
        let shard_id = Uuid::parse_str(&message.shard).map_err(NidxError::from)?;
        shard_request! {
            "suggest",
            self,
            request,
            shard_id,
            LOCAL => shard_suggest::suggest(Arc::clone(&self.index_cache), message.clone()).await,
            REMOTE => suggest
        }
    }

    async fn graph_search(&self, request: Request<GraphSearchRequest>) -> Result<Response<GraphSearchResponse>> {
        let message = request.get_ref();
        let shard_id = Uuid::parse_str(&message.shard).map_err(NidxError::from)?;
        shard_request! {
            "graph_search",
            self,
            request,
            shard_id,
            LOCAL => shard_search::graph_search(Arc::clone(&self.index_cache), message.clone()).await,
            REMOTE => graph_search
        }
    }

    async fn extracted_texts(
        &self,
        request: Request<ExtractedTextsRequest>,
    ) -> Result<Response<ExtractedTextsResponse>> {
        let message = request.get_ref();
        let shard_id = Uuid::parse_str(&message.shard_id).map_err(NidxError::from)?;
        shard_request! {
            "extracted_texts",
            self,
            request,
            shard_id,
            LOCAL => shard_text::extracted_texts(Arc::clone(&self.index_cache), message.clone()).await,
            REMOTE => extracted_texts
        }
    }

    type ParagraphsStream = Pin<Box<dyn Stream<Item = Result<ParagraphItem, Status>> + Send>>;
    async fn paragraphs(&self, request: Request<StreamRequest>) -> Result<Response<Self::ParagraphsStream>> {
        let message = request.get_ref();
        let Some(shard) = &message.shard_id else {
            return Err(NidxError::invalid("Uuid is required").into());
        };
        let shard_id = Uuid::parse_str(&shard.id).map_err(NidxError::from)?;
        shard_request! {
            "paragraphs",
            self,
            request,
            shard_id,
            LOCAL => streams::paragraph_iterator(Arc::clone(&self.index_cache), message.clone()).await,
            REMOTE => paragraphs,
            STREAM => Self::ParagraphsStream
        }
    }

    type DocumentsStream = Pin<Box<dyn Stream<Item = Result<DocumentItem, Status>> + Send>>;
    async fn documents(&self, request: Request<StreamRequest>) -> Result<Response<Self::DocumentsStream>> {
        let message = request.get_ref();
        let Some(shard) = &message.shard_id else {
            return Err(NidxError::invalid("Uuid is required").into());
        };
        let shard_id = Uuid::parse_str(&shard.id).map_err(NidxError::from)?;
        shard_request! {
            "documents",
            self,
            request,
            shard_id,
            LOCAL => streams::document_iterator(Arc::clone(&self.index_cache), message.clone()).await,
            REMOTE => documents,
            STREAM => Self::DocumentsStream
        }
    }
}

impl SearchServer {
    /// Perform a distributed query over a partitioned dataset across a cluster of nodes.
    ///
    /// The whole dataset is partitioned and distributed (with replication) across nidx-searcher
    /// nodes. To answer a query, we must query all partitions (shards) and merge the results into a
    /// single respose.
    ///
    /// This function is responsible for partial failure handling and retry. As a node can be
    /// requested to query several partitions and one or more can fail, it will then return a
    /// partial result and is our responsibility to retry on other nodes.
    ///
    async fn distributed_query<Op>(&self, request: Op::Request, shards: Vec<Uuid>) -> NidxResult<Vec<Op::Response>>
    where
        Op: SearcherOp,
    {
        let mut partitioner = QueryPartitioner::new(&self.shard_selector, &shards)?;

        let mut responses = vec![];
        let mut pending: HashSet<Uuid> = HashSet::from_iter(shards.clone());
        while !pending.is_empty() {
            for PartialResponse { data: response, shards } in self
                .scatter_gather::<Op>(&mut partitioner, request.clone(), &pending)
                .await?
            {
                // The response includes a list of successful shards, that may be a subset of the
                // requested ones. This is the way we communicate partial failures.
                for shard_id in shards {
                    pending.remove(&shard_id);
                }
                responses.push(response);
            }
        }

        Ok(responses)
    }

    /// Perform a scatter-gather cycle using the preferred node for each shard.
    /// This operation will group shards by node and scatter the requests across
    /// nodes. Then, we'll wait for all of them and gather the results
    ///
    /// NOTE that cycles are less time-efficient than immediately requesting
    /// failed nodes, as tail latency can increase due to slow/faulty nodes
    #[instrument(skip_all, fields(partitions = shards.len(), nodes = Empty))]
    async fn scatter_gather<Op>(
        &self,
        partitioner: &mut QueryPartitioner,
        request: Op::Request,
        shards: &HashSet<Uuid>,
    ) -> NidxResult<Vec<PartialResponse<Op::Response>>>
    where
        Op: SearcherOp,
    {
        let mut groups = HashMap::new();
        for shard_id in shards.iter() {
            let Some(node) = partitioner.next_node_for_shard(shard_id) else {
                // A shard doesn't have more nodes to query but we haven't finished yet. We
                // won't be able to complete the request in all partitions, so we abort
                error!(?shard_id, "Error in search, exhausted all available nodes for shard");
                // Propagate a NotFound error. Note that we can only arrive here due to NotFound
                // errors as they are the only ones we retry. If at some point we handle more,
                // this will be an incorrect assumption
                return Err(NidxError::NotFound);
            };
            groups
                .entry(node)
                .and_modify(|shards: &mut Vec<Uuid>| shards.push(*shard_id))
                .or_insert(vec![*shard_id]);
        }
        Span::current().record("nodes", groups.len());

        // Distribute requests across nodes
        let mut tasks = JoinSet::new();
        for (node, shard_ids) in groups {
            tasks.spawn(
                self.node_query::<Op>(node, shard_ids, request.clone())
                    .await?
                    .instrument(Span::current()),
            );
        }

        // Aggregate results
        let mut responses = vec![];
        while let Some(join) = tasks.join_next().await {
            match join {
                Ok(Ok(response)) => {
                    responses.push(response);
                }
                Ok(Err(NidxError::NotFound)) => {
                    // shard not found in searcher, probably due to a
                    // topology change the shard is not yet where it should
                }
                Ok(Err(NidxError::GrpcError(status))) if status.code() == Code::NotFound => {
                    // same as above
                }
                Ok(Err(NidxError::GrpcError(status))) if status.code() == Code::Unavailable => {
                    // searcher peer temporarily unavailable, will retry with another node
                }
                Ok(Err(search_error)) => {
                    return Err(search_error);
                }
                Err(join_error) => {
                    // Either a panic or a cancellation happened while searching
                    return Err(NidxError::from(join_error));
                }
            }
        }

        Ok(responses)
    }

    async fn node_query<Op>(
        &self,
        node: SearcherNode,
        shards: Vec<Uuid>,
        request: Op::Request,
    ) -> NidxResult<Pin<Box<dyn Future<Output = NidxResult<PartialResponse<Op::Response>>> + Send + 'static>>>
    where
        Op: SearcherOp,
    {
        match node {
            SearcherNode::This => {
                let index_cache = Arc::clone(&self.index_cache);
                Ok(Box::pin(
                    async move { Op::local_query(index_cache, request, shards).await },
                ))
            }
            SearcherNode::Remote(ref hostname) => {
                match self.get_client(hostname).await {
                    Ok(client) => Ok(Box::pin(async move { Op::remote_query(client, request, shards).await })),
                    Err(e) => {
                        // A client for this node is not available, although we've seen it
                        // in the k8s cluster. We skip it and will retry those shards on
                        // another node to complete the request
                        warn!(
                            ?node,
                            ?shards,
                            "{}",
                            format!("Error getting a client for node '{hostname}': {e}")
                        );
                        Err(e)
                    }
                }
            }
        }
    }
}

trait SearcherOp: Clone + Send {
    type Request: Clone + Send + 'static;
    type Response: Send + 'static;

    fn local_query(
        index_cache: Arc<IndexCache>,
        request: Self::Request,
        shards: Vec<Uuid>,
    ) -> impl std::future::Future<Output = NidxResult<PartialResponse<Self::Response>>> + Send;

    fn remote_query(
        client: SearcherClient,
        request: Self::Request,
        shards: Vec<Uuid>,
    ) -> impl std::future::Future<Output = NidxResult<PartialResponse<Self::Response>>> + Send;

    fn merge(partitions: Vec<Self::Response>, order_by: OrderBy, limit: Limit) -> Self::Response;
}

#[derive(Clone)]
struct SearchOp;

impl SearcherOp for SearchOp {
    type Request = SearchRequest;
    type Response = SearchResponse;

    async fn local_query(
        index_cache: Arc<IndexCache>,
        request: Self::Request,
        shards: Vec<Uuid>,
    ) -> NidxResult<PartialResponse<Self::Response>> {
        shard_search::search(shards, index_cache, request).await
    }

    async fn remote_query(
        mut client: SearcherClient,
        request: Self::Request,
        shards: Vec<Uuid>,
    ) -> NidxResult<PartialResponse<Self::Response>> {
        // Send the query to a different node specifying only a subset of shards
        let mut search_request = request.clone();
        search_request.shard_ids = shards.iter().map(|x| x.to_string()).collect();

        // We do include a marker header to indicate we already hopped in
        // the cluster. This avoids an infinite loop across nodes
        let mut request = Request::new(search_request);
        request.metadata_mut().insert(HEADER_LOCAL_ONLY, "1".parse().unwrap());

        let response = client.search(request).await.map_err(NidxError::GrpcError)?.into_inner();

        let result = if response.shard_ids.len() == shards.len() {
            PartialResponse { data: response, shards }
        } else {
            let successful_shards = response
                .shard_ids
                .iter()
                .map(|s| Uuid::parse_str(s).expect("This is an internal response, we always send valid UUIDs"))
                .collect();
            PartialResponse {
                data: response,
                shards: successful_shards,
            }
        };
        Ok(result)
    }

    fn merge(mut partitions: Vec<Self::Response>, order_by: OrderBy, limit: Limit) -> Self::Response {
        if partitions.len() == 1 {
            partitions.pop().unwrap()
        } else {
            shard_merge::merge(partitions, order_by, limit)
        }
    }
}

struct QueryPartitioner {
    shard_nodes: HashMap<Uuid, Vec<SearcherNode>>,
}

impl QueryPartitioner {
    fn new(shard_selector: &ShardSelector, shards: &[Uuid]) -> NidxResult<Self> {
        let mut shard_nodes = HashMap::new();
        for shard_id in shards {
            let nodes = shard_selector.nodes_for_shard(shard_id);
            if nodes.is_empty() {
                // We haven't found any node with this shard. As we'll never be able to return a
                // full result, we fail fast
                return Err(NidxError::NotFound);
            }
            shard_nodes.insert(*shard_id, nodes);
        }
        Ok(Self { shard_nodes })
    }

    /// Return the next preferred node to query for a shard.
    ///
    /// *Panics* if a called with a shard that wasn't in the original set
    fn next_node_for_shard(&mut self, shard: &Uuid) -> Option<SearcherNode> {
        let nodes = self.shard_nodes.get_mut(shard).unwrap();
        if !nodes.is_empty() { Some(nodes.remove(0)) } else { None }
    }
}
