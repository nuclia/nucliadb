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

use std::collections::HashMap;
use std::{pin::Pin, sync::Arc};

use futures::Stream;
use nidx_protos::nidx::nidx_searcher_client::NidxSearcherClient;
use nidx_protos::nidx::nidx_searcher_server::{NidxSearcher, NidxSearcherServer};
use nidx_protos::*;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Request, Response, Result, Status, service::Routes};

use crate::NidxMetadata;
use crate::errors::{NidxError, NidxResult};
use crate::searcher::shard_merge::{Limit, OrderBy};
use crate::searcher::shard_search::{PartialResult, SearchResult};
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
    meta: NidxMetadata,
    index_cache: Arc<IndexCache>,
    shard_selector: ShardSelector,
    clients: Arc<RwLock<HashMap<String, SearcherClient>>>,
}

impl SearchServer {
    pub fn new(meta: NidxMetadata, index_cache: Arc<IndexCache>, shard_selector: ShardSelector) -> Self {
        SearchServer {
            meta,
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
        let local_only = request.metadata().contains_key(HEADER_LOCAL_ONLY);
        let request = request.into_inner();

        let mut shards = vec![];
        for shard_id in &request.shard_ids {
            let shard_id = uuid::Uuid::parse_str(shard_id).map_err(NidxError::from)?;
            shards.push(shard_id);
        }

        let response = if local_only {
            // This is a hopped node, i.e., another searcher has delegated this part of the query to
            // us. We must query our shards and return either a full or partial response or an error.
            // We won't hop to any other node
            shard_search::search(shards, Arc::clone(&self.index_cache), request)
                .await?
                .into_value()
        } else {
            // A query that may need to be distributed across nodes in order to search in all partitions.

            // Before doing any work, validate shards exist
            let existing_shards = crate::metadata::Shard::exist_many(&self.meta.pool, &shards)
                .await
                .map_err(NidxError::from)?;
            for shard_id in &shards {
                if !existing_shards.contains(shard_id) {
                    return Err(NidxError::NotFound.into());
                }
            }
            self.distributed_query(request, shards).await?
        };
        Ok(Response::new(response))
    }

    async fn suggest(&self, request: Request<SuggestRequest>) -> Result<Response<SuggestResponse>> {
        let message = request.get_ref();
        let shard_id = uuid::Uuid::parse_str(&message.shard).map_err(NidxError::from)?;
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
        let shard_id = uuid::Uuid::parse_str(&message.shard).map_err(NidxError::from)?;
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
        let shard_id = uuid::Uuid::parse_str(&message.shard_id).map_err(NidxError::from)?;
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
        let shard_id = uuid::Uuid::parse_str(&shard.id).map_err(NidxError::from)?;
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
        let shard_id = uuid::Uuid::parse_str(&shard.id).map_err(NidxError::from)?;
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
    async fn distributed_query(&self, request: SearchRequest, shards: Vec<uuid::Uuid>) -> NidxResult<SearchResponse> {
        // Locate which nodes contain each shard and get a list by preference
        let mut partitioner = QueryPartitioner::new(&self.shard_selector, shards)?;

        // Now, we'll do scatter-gather cycles using the most preferred node per shard. Grouping
        // cycles is less time-efficient, as we wait for the slowest request, but we minimize the
        // amount of requests through the cluster.
        let mut responses = vec![];
        while let Some(shard_groups) = partitioner.next_groups_by_node()? {
            let mut tasks = JoinSet::new();
            for (node, shard_ids) in shard_groups {
                self.spawn_node_search(&mut tasks, node, shard_ids, request.clone())
                    .await;
            }

            while let Some(join) = tasks.join_next().await {
                match join {
                    Ok(Ok(result)) => {
                        let response = result.into_value();
                        // The response includes a list of successful shards, that may be a subset of the
                        // requested ones. This is the way we communicate partial failures.
                        for shard_id in &response.shard_ids {
                            let shard_id = uuid::Uuid::parse_str(shard_id)
                                .expect("we always populates queried shards with valid UUIDs");
                            // We now remove successful shards from the set of shards we want to query
                            partitioner.succeeded(&shard_id);
                        }
                        responses.push(response);
                    }
                    Ok(Err(search_error)) => {
                        warn!("An error occurred while searching: {search_error:?}");
                    }
                    Err(join_error) => {
                        // Either a panic or a cancellation happened while searching
                        error!("A shard query failed in tokio: {:?}", join_error.to_string());
                        return Err(join_error.into());
                    }
                }
            }
        }

        let merged = if responses.len() == 1 {
            responses.pop().unwrap()
        } else {
            shard_merge::merge(
                responses,
                OrderBy::from(&request),
                Limit(request.result_per_page as usize),
            )
        };
        Ok(merged)
    }

    async fn spawn_node_search(
        &self,
        tasks: &mut JoinSet<NidxResult<SearchResult<SearchResponse>>>,
        node: SearcherNode,
        shard_ids: Vec<uuid::Uuid>,
        request: SearchRequest,
    ) {
        match node {
            SearcherNode::This => {
                tasks.spawn(shard_search::search(shard_ids, Arc::clone(&self.index_cache), request));
            }
            SearcherNode::Remote(ref hostname) => {
                match self.get_client(hostname).await {
                    Ok(client) => {
                        tasks.spawn(Self::remote_query(client, request, shard_ids));
                    }
                    Err(e) => {
                        // A client for this node is not available, although we've seen it
                        // in the k8s cluster. We skip it and will retry those shards on
                        // another node to complete the request
                        warn!(
                            ?node,
                            ?shard_ids,
                            "{}",
                            format!("Error getting a client for node '{hostname}': {e}")
                        );
                    }
                }
            }
        }
    }

    async fn remote_query(
        mut client: SearcherClient,
        request: SearchRequest,
        shards: Vec<uuid::Uuid>,
    ) -> NidxResult<SearchResult<SearchResponse>> {
        // Send the query to a different node specifying only a subset of shards
        let mut search_request = request.clone();
        search_request.shard_ids = shards.iter().map(|x| x.to_string()).collect();

        // We do include a marker header to indicate we already hopped in
        // the cluster. This avoids an infinite loop across nodes
        let mut request = Request::new(search_request);
        request.metadata_mut().insert(HEADER_LOCAL_ONLY, "1".parse().unwrap());

        let response = client.search(request).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();
                let result = if response.shard_ids.len() == shards.len() {
                    SearchResult::Complete(response)
                } else {
                    SearchResult::Partial(PartialResult { part: response })
                };
                Ok(result)
            }
            Err(status) => {
                let error = match status.code() {
                    tonic::Code::NotFound => NidxError::NotFound,
                    _ => NidxError::Unknown(anyhow::Error::from(status)),
                };
                Err(error)
            }
        }
    }
}

struct QueryPartitioner {
    shard_nodes: HashMap<uuid::Uuid, Vec<SearcherNode>>,
}

impl QueryPartitioner {
    fn new(shard_selector: &ShardSelector, shards: Vec<uuid::Uuid>) -> NidxResult<Self> {
        let mut shard_nodes = HashMap::new();
        for shard_id in &shards {
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

    fn next_groups_by_node(&mut self) -> Result<Option<HashMap<SearcherNode, Vec<uuid::Uuid>>>, NidxError> {
        if self.shard_nodes.is_empty() {
            return Ok(None);
        }

        let mut shard_groups = HashMap::new();
        for (shard_id, nodes) in self.shard_nodes.iter_mut() {
            if nodes.is_empty() {
                // A shard doesn't have more nodes to query but we haven't finished yet. We
                // won't be able to complete the request in all partitions, so we abort
                return Err(NidxError::QueryError(format!(
                    "Error in search, exhausted all available nodes for shard {shard_id}"
                )));
            }

            let node = nodes.remove(0);
            shard_groups
                .entry(node)
                .and_modify(|shards: &mut Vec<uuid::Uuid>| shards.push(*shard_id))
                .or_insert(vec![*shard_id]);
        }
        Ok(Some(shard_groups))
    }

    fn succeeded(&mut self, shard_id: &uuid::Uuid) {
        self.shard_nodes.remove(shard_id);
    }
}
