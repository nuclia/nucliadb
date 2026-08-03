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

use std::collections::HashMap;

use crate::common::services::NidxFixture;
use nidx_protos::paragraph_result;
use nidx_protos::{NewShardRequest, SearchAfter, SearchRequest, VectorIndexConfig};
use nidx_tests::little_prince;
use sqlx::PgPool;
use tonic::Request;

#[sqlx::test]
async fn test_search_after_multiple_shards(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;

    let mut shard_ids = Vec::new();
    for _ in 0..3 {
        let response = fixture
            .api_client
            .new_shard(Request::new(NewShardRequest {
                kbid: "aabbccddeeff11223344556677889900".to_string(),
                vectorsets_configs: HashMap::from([(
                    "english".to_string(),
                    VectorIndexConfig {
                        vector_dimension: Some(3),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }))
            .await?;
        let shard_id = response.get_ref().id.clone();
        // Same text in every shard, so we have duplicate scores
        fixture
            .index_resource(&shard_id, little_prince(shard_id.clone(), None))
            .await?;
        shard_ids.push(shard_id);
    }

    fixture.wait_sync().await;

    // One large query to establish the expected result set and ordering.
    let full_response = fixture
        .searcher_client
        .search(Request::new(SearchRequest {
            shard_ids: shard_ids.clone(),
            body: "prince".to_string(),
            paragraph: true,
            result_per_page: 1000,
            ..Default::default()
        }))
        .await?
        .into_inner();

    let expected: Vec<String> = full_response
        .paragraph
        .as_ref()
        .unwrap()
        .results
        .iter()
        .map(|r| r.paragraph.clone())
        .collect();

    let total = expected.len();
    assert!(total >= 6);

    // Page through results one at a time.
    let mut collected: Vec<String> = Vec::new();
    let mut cursor: Option<SearchAfter> = None;

    let mut last_score = None;
    loop {
        let response = fixture
            .searcher_client
            .search(Request::new(SearchRequest {
                shard_ids: shard_ids.clone(),
                body: "prince".to_string(),
                paragraph: true,
                result_per_page: 1,
                search_after: cursor,
                ..Default::default()
            }))
            .await?
            .into_inner();

        let results = &response.paragraph.as_ref().unwrap().results;
        if results.is_empty() {
            break;
        }

        for r in results {
            collected.push(r.paragraph.clone());
        }

        // Advance the cursor to the last result on this page.
        let last = results.last().unwrap();
        let Some(paragraph_result::SortValue::Score(score)) = &last.sort_value else {
            anyhow::bail!("Expected Score sort value on paragraph result");
        };
        cursor = Some(SearchAfter {
            score: score.bm25,
            shard_id: last.shard_id.clone(),
            docaddr: score.docaddr,
        });

        if let Some(last_score) = last_score {
            assert!(score.bm25 <= last_score);
        }
        last_score = Some(score.bm25);
    }

    assert_eq!(
        collected.len(),
        total,
        "search_after pagination collected {} results but expected {total}",
        collected.len()
    );

    // Sorted sets must match exactly: no duplicates, no gaps.
    let mut collected_sorted = collected.clone();
    collected_sorted.sort();
    let mut expected_sorted = expected.clone();
    expected_sorted.sort();
    assert_eq!(
        collected_sorted, expected_sorted,
        "Paginated results must exactly match the full result set"
    );

    Ok(())
}
