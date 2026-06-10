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

mod common;
use common::TestOpener;
use nidx_protos::TextInformation;
use nidx_tests::minimal_resource;
use nidx_text::{DocumentSearchRequest, TextConfig, TextIndexer, TextSearcher};
use tempfile::tempdir;

#[test]
fn test_texts_deletions() -> anyhow::Result<()> {
    // Create a resource with a couple of fields
    let mut resource = minimal_resource("shard".to_string());
    resource.texts.insert(
        "a/title".to_string(),
        TextInformation {
            text: "The little prince".to_string(),
            ..Default::default()
        },
    );
    resource.texts.insert(
        "a/summary".to_string(),
        TextInformation {
            text: "A story about a little prince".to_string(),
            ..Default::default()
        },
    );
    let rid = resource.resource.clone().unwrap().uuid.clone();
    let segment_dir = tempdir()?;
    let meta1 = TextIndexer
        .index_resource(segment_dir.path(), TextConfig::default(), &resource)?
        .unwrap();

    // Search to validate that both fields are indexed
    let searcher = TextSearcher::open(
        TextConfig::default(),
        TestOpener::new(vec![(meta1.clone(), 1i64.into())], vec![]),
    )?;
    let result = searcher.search(&DocumentSearchRequest {
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(result.results.len(), 2);

    // Search with resource deletion
    let searcher = TextSearcher::open(
        TextConfig::default(),
        TestOpener::new(vec![(meta1.clone(), 1i64.into())], vec![(rid.clone(), 2i64.into())]),
    )?;
    let result = searcher.search(&DocumentSearchRequest {
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(result.results.len(), 0);

    // Search with field deletion
    let searcher = TextSearcher::open(
        TextConfig::default(),
        TestOpener::new(
            vec![(meta1.clone(), 1i64.into())],
            vec![(format!("{rid}/a/title"), 2i64.into())],
        ),
    )?;
    let result = searcher.search(&DocumentSearchRequest {
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(result.results.len(), 1);

    Ok(())
}
