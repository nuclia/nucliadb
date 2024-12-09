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

use std::{pin::Pin, sync::Arc};

use futures::Stream;
use nidx_protos::nidx::nidx_searcher_server::{NidxSearcher, NidxSearcherServer};
use nidx_protos::*;
use tonic::{service::Routes, Request, Response, Result, Status};

use super::streams::{document_iterator, paragraph_iterator};
use super::{index_cache::IndexCache, shard_search, shard_suggest};
use tracing::*;

pub struct SearchServer {
    index_cache: Arc<IndexCache>,
}

impl SearchServer {
    pub fn new(index_cache: Arc<IndexCache>) -> Self {
        SearchServer {
            index_cache,
        }
    }

    pub fn into_service(self) -> Routes {
        Routes::new(NidxSearcherServer::new(self))
    }
}

#[tonic::async_trait]
impl NidxSearcher for SearchServer {
    async fn search(&self, request: Request<SearchRequest>) -> Result<Response<SearchResponse>> {
        let response = shard_search::search(Arc::clone(&self.index_cache), request.into_inner()).await;
        match response {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(?e, "Error in search");
                Err(Status::internal(e.to_string()))
            }
        }
    }

    async fn suggest(&self, request: Request<SuggestRequest>) -> Result<Response<SuggestResponse>> {
        let response = shard_suggest::suggest(Arc::clone(&self.index_cache), request.into_inner()).await;
        match response {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(?e, "Error in suggest");
                Err(Status::internal(e.to_string()))
            }
        }
    }

    type ParagraphsStream = Pin<Box<dyn Stream<Item = Result<ParagraphItem, Status>> + Send>>;

    async fn paragraphs(&self, request: Request<StreamRequest>) -> Result<Response<Self::ParagraphsStream>> {
        let response = paragraph_iterator(Arc::clone(&self.index_cache), request.into_inner()).await;
        match response {
            Ok(response) => Ok(Response::new(Box::pin(futures::stream::iter(response.map(Result::Ok))))),
            Err(e) => {
                error!(?e, "Error in paragraphs stream");
                Err(Status::internal(e.to_string()))
            }
        }
    }
    type DocumentsStream = Pin<Box<dyn Stream<Item = Result<DocumentItem, Status>> + Send>>;

    async fn documents(&self, request: Request<StreamRequest>) -> Result<Response<Self::DocumentsStream>> {
        let response = document_iterator(Arc::clone(&self.index_cache), request.into_inner()).await;
        match response {
            Ok(response) => Ok(Response::new(Box::pin(futures::stream::iter(response.map(Result::Ok))))),
            Err(e) => {
                error!(?e, "Error in paragraphs stream");
                Err(Status::internal(e.to_string()))
            }
        }
    }
}
