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

use std::sync::Arc;

use nidx_protos::{SearchRequest, SearchResponse, VectorSearchRequest};
use nidx_vector::data_point_provider::reader::VectorsContext;

use crate::{metadata::Index, NidxMetadata};

use super::index_cache::{IndexCache, IndexSearcher};

pub async fn search(
    meta: &NidxMetadata,
    index_cache: Arc<IndexCache>,
    search_request: SearchRequest,
) -> anyhow::Result<SearchResponse> {
    // TODO: Bring all query parsing code from nucliadb_vectors and use all indexes

    let index = Index::find(
        &meta.pool,
        uuid::Uuid::parse_str(&search_request.shard)?,
        crate::metadata::IndexKind::Vector,
        &search_request.vectorset,
    )
    .await?;
    let index_seacher = index_cache.get(&index.id).await?;
    let IndexSearcher::Vector(vector_searcher) = index_seacher.as_ref();

    let results = vector_searcher.search(
        &VectorSearchRequest {
            id: search_request.shard.clone(),
            vector_set: search_request.vectorset.clone(),
            vector: search_request.vector.clone(),
            page_number: search_request.page_number,
            result_per_page: search_request.result_per_page,
            with_duplicates: search_request.with_duplicates,
            key_filters: search_request.key_filters.clone(),
            min_score: search_request.min_score_semantic,
            field_filters: search_request.fields.clone(),
            ..Default::default()
        },
        &VectorsContext::default(),
    )?;

    Ok(SearchResponse {
        document: None,
        paragraph: None,
        vector: Some(results),
        relation: None,
    })
}
