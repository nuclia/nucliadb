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

use nidx_paragraph::ParagraphSearcher;
use nidx_protos::{DocumentItem, ParagraphItem, StreamRequest};
use nidx_text::TextSearcher;

use crate::errors::{NidxError, NidxResult};

use super::index_cache::IndexCache;

pub async fn paragraph_iterator(
    index_cache: Arc<IndexCache>,
    request: StreamRequest,
) -> NidxResult<impl Iterator<Item = ParagraphItem>> {
    let shard_id = uuid::Uuid::parse_str(&request.shard_id.as_ref().unwrap().id)?;

    let Some(indexes) = index_cache.get_shard_indexes(&shard_id).await else {
        return Err(NidxError::NotFound);
    };

    let Some(paragraph_index) = indexes.paragraph_index() else {
        return Err(NidxError::NotFound);
    };
    let paragraph_searcher_arc = index_cache.get(&paragraph_index).await?;

    tokio::task::spawn_blocking(move || {
        let paragraph_searcher: &ParagraphSearcher = paragraph_searcher_arc.as_ref().into();
        paragraph_searcher.iterator(&request)
    })
    .await?
    .map_err(NidxError::from)
}

pub async fn document_iterator(
    index_cache: Arc<IndexCache>,
    request: StreamRequest,
) -> NidxResult<impl Iterator<Item = DocumentItem>> {
    let shard_id = uuid::Uuid::parse_str(&request.shard_id.as_ref().unwrap().id)?;

    let Some(indexes) = index_cache.get_shard_indexes(&shard_id).await else {
        return Err(NidxError::NotFound);
    };

    let Some(text_index) = indexes.text_index() else {
        return Err(NidxError::NotFound);
    };
    let text_searcher_arc = index_cache.get(&text_index).await?;

    tokio::task::spawn_blocking(move || {
        let text_searcher: &TextSearcher = text_searcher_arc.as_ref().into();
        text_searcher.iterator(&request)
    })
    .await?
    .map_err(NidxError::from)
}
