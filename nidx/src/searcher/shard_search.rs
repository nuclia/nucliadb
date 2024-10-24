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
use nidx_vector::{config::VectorConfig, data_point_provider::reader::VectorsContext, VectorSearcher};

use crate::{metadata::Index, NidxMetadata};

use super::metadata::SearchMetadata;

pub async fn search(
    meta: &NidxMetadata,
    index_metadata: Arc<SearchMetadata>,
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

    // TODO: Cache for index readers
    let read_metadata = index_metadata.read().await;
    let index_meta = read_metadata.get(&index.id).unwrap();
    let operations = index_meta
        .iter()
        .map(|s| (s.seq, s.segments.iter().map(|(_, s)| s).cloned().collect(), s.deleted_keys.clone()))
        .collect();
    let vector_reader = VectorSearcher::open(VectorConfig::default(), operations)?;
    drop(read_metadata);

    let results = vector_reader.search(
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
