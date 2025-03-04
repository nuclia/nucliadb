// Copyright (C) 2021 Bosutech XXI S.L.
//
// nucliadb is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at info@nuclia.com.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
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
use crate::searcher::shard_selector::SearcherNode;

use super::shard_selector::ShardSelector;
use super::streams;
use super::{index_cache::IndexCache, shard_search, shard_suggest};
use tracing::*;

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
        let nodes = $self.shard_selector.nodes_for_shard(&$shard_id);
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
                            client.$remote_method(Request::new($request.clone()))
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
        let request = request.into_inner();
        let shard_id = uuid::Uuid::parse_str(&request.shard).map_err(NidxError::from)?;
        shard_request! {
            "search",
            self,
            request,
            shard_id,
            LOCAL => shard_search::search(Arc::clone(&self.index_cache), request.clone()).await,
            REMOTE => search
        }
    }

    async fn suggest(&self, request: Request<SuggestRequest>) -> Result<Response<SuggestResponse>> {
        let request = request.into_inner();
        let shard_id = uuid::Uuid::parse_str(&request.shard).map_err(NidxError::from)?;
        shard_request! {
            "suggest",
            self,
            request,
            shard_id,
            LOCAL => shard_suggest::suggest(Arc::clone(&self.index_cache), request.clone()).await,
            REMOTE => suggest
        }
    }

    async fn graph_search(&self, request: Request<GraphSearchRequest>) -> Result<Response<GraphSearchResponse>> {
        let request = request.into_inner();
        let shard_id = uuid::Uuid::parse_str(&request.shard).map_err(NidxError::from)?;
        shard_request! {
            "graph_search",
            self,
            request,
            shard_id,
            LOCAL => shard_search::graph_search(Arc::clone(&self.index_cache), request.clone()).await,
            REMOTE => graph_search
        }
    }

    type ParagraphsStream = Pin<Box<dyn Stream<Item = Result<ParagraphItem, Status>> + Send>>;
    async fn paragraphs(&self, request: Request<StreamRequest>) -> Result<Response<Self::ParagraphsStream>> {
        let request = request.into_inner();
        let Some(shard) = &request.shard_id.as_ref() else {
            return Err(NidxError::invalid("Uuid is required").into());
        };
        let shard_id = uuid::Uuid::parse_str(&shard.id).map_err(NidxError::from)?;
        shard_request! {
            "paragraphs",
            self,
            request,
            shard_id,
            LOCAL => streams::paragraph_iterator(Arc::clone(&self.index_cache), request.clone()).await,
            REMOTE => paragraphs,
            STREAM => Self::ParagraphsStream
        }
    }

    type DocumentsStream = Pin<Box<dyn Stream<Item = Result<DocumentItem, Status>> + Send>>;
    async fn documents(&self, request: Request<StreamRequest>) -> Result<Response<Self::DocumentsStream>> {
        let request = request.into_inner();
        let Some(shard) = &request.shard_id.as_ref() else {
            return Err(NidxError::invalid("Uuid is required").into());
        };
        let shard_id = uuid::Uuid::parse_str(&shard.id).map_err(NidxError::from)?;
        shard_request! {
            "documents",
            self,
            request,
            shard_id,
            LOCAL => streams::document_iterator(Arc::clone(&self.index_cache), request.clone()).await,
            REMOTE => documents,
            STREAM => Self::DocumentsStream
        }
    }
}
