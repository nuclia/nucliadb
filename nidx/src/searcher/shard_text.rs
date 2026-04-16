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
            extracted_texts.paragraphs.insert(k.to_string(), v.unwrap_or_default());
        }
    }

    Ok(extracted_texts)
}
