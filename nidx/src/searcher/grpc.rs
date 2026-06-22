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
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Request, Response, Result, Status, service::Routes};

use crate::errors::{NidxError, NidxResult};
use crate::searcher::shard_merge::{Limit, OrderBy};
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
        let message = request.get_ref();

        let mut errors = Vec::new();
        let mut responses = Vec::with_capacity(message.shard_ids.len());

        let local_only = request.metadata().contains_key(HEADER_LOCAL_ONLY);
        for shard_id in &message.shard_ids {
            let shard_id = uuid::Uuid::parse_str(shard_id).map_err(NidxError::from)?;

            let nodes = if local_only {
                vec![SearcherNode::This]
            } else {
                self.shard_selector.nodes_for_shard(&shard_id)
            };

            for node in &nodes {
                debug!(?node, ?shard_id, "Attempting search");
                let result = match node {
                    SearcherNode::This => {
                        shard_search::search(shard_id, Arc::clone(&self.index_cache), message.clone())
                            .await
                            .map(Response::new)
                    }
                    SearcherNode::Remote(hostname) => match self.get_client(hostname).await {
                        Ok(mut client) => {
                            let mut request = Request::new(message.clone());
                            request.metadata_mut().insert(HEADER_LOCAL_ONLY, "1".parse().unwrap());
                            client.search(request).await.map_err(|e| match e.code() {
                                tonic::Code::NotFound => NidxError::NotFound,
                                _ => NidxError::Unknown(anyhow::Error::from(e)),
                            })
                        }
                        Err(e) => Err(e),
                    },
                };
                match result {
                    Ok(response) => {
                        responses.push(response);
                        break;
                    }
                    Err(e) => {
                        warn!(
                            ?node,
                            ?shard_id,
                            concat!("Error in search, trying with next node: {:?}"),
                            e
                        );
                        errors.push(e);
                    }
                }
            }

            if !errors.is_empty() {
                error!(
                    ?errors,
                    ?nodes,
                    ?shard_id,
                    "Error in search, exhausted all availabled nodes"
                );
                if let Some(reported_error) = errors.pop() {
                    return Err(reported_error.into());
                } else {
                    return Err(Status::internal("Unknown search error"));
                }
            }
        }

        let merged = shard_merge::merge(
            responses.into_iter().map(|r| r.into_inner()).collect(),
            OrderBy::from(message),
            Limit(message.result_per_page as usize),
        );
        Ok(Response::new(merged))
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
