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

use crate::{grpc_server::RemappedGrpcService, NidxMetadata};

use super::{index_cache::IndexCache, shard_search, shard_suggest};
use tracing::*;

pub struct SearchServer {
    meta: NidxMetadata,
    index_cache: Arc<IndexCache>,
}

impl SearchServer {
    pub fn new(meta: NidxMetadata, index_cache: Arc<IndexCache>) -> Self {
        SearchServer {
            meta,
            index_cache,
        }
    }

    pub fn into_service(self) -> RemappedGrpcService {
        RemappedGrpcService {
            routes: Routes::new(NidxSearcherServer::new(self)),
            package: "nidx.NidxSearcher".to_string(),
        }
    }
}

#[tonic::async_trait]
impl NidxSearcher for SearchServer {
    async fn vector_search(&self, _request: Request<VectorSearchRequest>) -> Result<Response<VectorSearchResponse>> {
        todo!()
    }

    async fn document_ids(&self, _request: Request<noderesources::ShardId>) -> Result<Response<IdCollection>> {
        todo!()
    }

    async fn paragraph_ids(&self, _request: Request<noderesources::ShardId>) -> Result<Response<IdCollection>> {
        todo!()
    }

    async fn vector_ids(&self, _request: Request<noderesources::VectorSetId>) -> Result<Response<IdCollection>> {
        todo!()
    }

    async fn relation_ids(&self, _request: Request<noderesources::ShardId>) -> Result<Response<IdCollection>> {
        todo!()
    }

    async fn relation_edges(&self, _request: Request<noderesources::ShardId>) -> Result<Response<EdgeList>> {
        todo!()
    }

    async fn search(&self, request: Request<SearchRequest>) -> Result<Response<SearchResponse>> {
        let response = shard_search::search(&self.meta, Arc::clone(&self.index_cache), request.into_inner()).await;
        match response {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(?e, "Error in search");
                Err(Status::internal(e.to_string()))
            }
        }
    }

    async fn suggest(&self, request: Request<SuggestRequest>) -> Result<Response<SuggestResponse>> {
        let response = shard_suggest::suggest(&self.meta, Arc::clone(&self.index_cache), request.into_inner()).await;
        match response {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                error!(?e, "Error in suggest");
                Err(Status::internal(e.to_string()))
            }
        }
    }

    type ParagraphsStream = Pin<Box<dyn Stream<Item = Result<ParagraphItem, Status>> + Send>>;

    async fn paragraphs(&self, _request: Request<StreamRequest>) -> Result<Response<Self::ParagraphsStream>> {
        todo!()
    }
    type DocumentsStream = Pin<Box<dyn Stream<Item = Result<DocumentItem, Status>> + Send>>;

    async fn documents(&self, _request: Request<StreamRequest>) -> Result<Response<Self::DocumentsStream>> {
        todo!()
    }
}
