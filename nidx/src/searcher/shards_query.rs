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

use tokio::task::JoinSet;
use tracing::{Instrument, Span};
use uuid::Uuid;

use crate::errors::{NidxError, NidxResult};

use super::index_cache::IndexCache;

/// Query multiple shards in parallel
///
pub async fn shards_query<Q, R, AsyncR>(
    index_cache: Arc<IndexCache>,
    shards: Vec<Uuid>,
    query: Q,
    op: impl Fn(Uuid, Arc<IndexCache>, Q) -> AsyncR + Send,
) -> NidxResult<Vec<R>>
where
    Q: Clone,
    R: Send + 'static,
    AsyncR: std::future::Future<Output = NidxResult<R>> + Send + 'static,
{
    let responses = if shards.len() == 1 {
        let shard_id = shards[0];
        let response = op(shard_id, Arc::clone(&index_cache), query).await?;
        vec![response]
    } else {
        let mut tasks = JoinSet::new();
        for shard_id in shards {
            tasks.spawn(op(shard_id, Arc::clone(&index_cache), query.clone()).instrument(Span::current()));
        }

        let mut responses = vec![];
        while let Some(join) = tasks.join_next().await {
            match join {
                Ok(Ok(response)) => responses.push(response),
                Ok(Err(NidxError::NotFound)) => {
                    // shard not found in searcher, probably due to a
                    // topology change the shard is not yet where it should
                }
                Ok(Err(search_error)) => {
                    return Err(search_error);
                }
                Err(join_error) => {
                    // Either a panic or a cancellation happened while searching
                    return Err(NidxError::from(join_error));
                }
            }
        }
        if responses.is_empty() {
            return Err(NidxError::NotFound);
        }
        responses
    };
    Ok(responses)
}
