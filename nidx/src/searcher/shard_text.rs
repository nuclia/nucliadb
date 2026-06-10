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
use std::time::Instant;

use nidx_protos::{ExtractedTextsRequest, ExtractedTextsResponse};
use nidx_text::{FieldUid, ParagraphUid, TextSearcher};
use tracing::Span;

use crate::errors::{NidxError, NidxResult};
use crate::searcher::index_cache::IndexCache;

pub async fn extracted_texts(
    index_cache: Arc<IndexCache>,
    request: ExtractedTextsRequest,
) -> NidxResult<ExtractedTextsResponse> {
    let start = Instant::now();

    if request.field_ids.is_empty() && request.paragraph_ids.is_empty() {
        // nothing requested, early return
        return Ok(ExtractedTextsResponse::default());
    }

    let shard_id = uuid::Uuid::parse_str(&request.shard_id)?;
    let Some(indexes) = index_cache.get_shard_indexes(&shard_id).await else {
        return Err(NidxError::NotFound);
    };

    let Some(text_index_id) = indexes.text_index() else {
        return Err(NidxError::NotFound);
    };
    let index = index_cache.get(&text_index_id).await?;

    let span = Span::current();
    let extracted_texts = tokio::task::spawn_blocking(move || {
        span.in_scope(|| {
            let searcher: &TextSearcher = index.as_ref().into();
            blocking_extracted_texts(searcher, request)
        })
    })
    .await??;

    tracing::debug!("Extracted texts took {:?}", start.elapsed());
    Ok(extracted_texts)
}

fn blocking_extracted_texts(
    searcher: &TextSearcher,
    request: ExtractedTextsRequest,
) -> NidxResult<ExtractedTextsResponse> {
    let mut extracted_texts = ExtractedTextsResponse::default();

    if !request.field_ids.is_empty() {
        let mut field_ids = vec![];
        for id in request.field_ids {
            field_ids.push(FieldUid {
                rid: id.rid,
                field_type: id.field_type,
                field_name: id.field_name,
                split: id.split,
            });
        }
        let fields_text = searcher.get_fields_text(field_ids)?;
        for (k, v) in fields_text {
            extracted_texts.fields.insert(k.to_string(), v.unwrap_or_default());
        }
    }

    if !request.paragraph_ids.is_empty() {
        let mut paragraph_ids = vec![];
        for id in request.paragraph_ids {
            paragraph_ids.push(ParagraphUid {
                rid: id.rid,
                field_type: id.field_type,
                field_name: id.field_name,
                split: id.split,
                paragraph_start: id.paragraph_start,
                paragraph_end: id.paragraph_end,
            });
        }
        let paragraphs_text = searcher.get_paragraphs_text(paragraph_ids)?;
        for (k, v) in paragraphs_text {
            extracted_texts.paragraphs.insert(k.to_string(), v);
        }
    }

    Ok(extracted_texts)
}
