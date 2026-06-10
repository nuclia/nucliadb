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
