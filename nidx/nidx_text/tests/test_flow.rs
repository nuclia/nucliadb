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
use nidx_tests::little_prince;
use nidx_text::{DocumentSearchRequest, TextConfig, TextIndexer, TextSearcher};
use tempfile::tempdir;

#[test]
fn test_index_merge_search() -> anyhow::Result<()> {
    let resource = little_prince("shard", None);
    let segment_dir = tempdir()?;
    let meta1 = TextIndexer
        .index_resource(segment_dir.path(), TextConfig::default(), &resource)?
        .unwrap();
    let records = meta1.records;

    let segment_dir2 = tempdir()?;
    let meta2 = TextIndexer
        .index_resource(segment_dir2.path(), TextConfig::default(), &resource)?
        .unwrap();

    let merge_dir = tempdir()?;
    let merged_meta = TextIndexer.merge(
        merge_dir.path(),
        TextConfig::default(),
        TestOpener::new(
            vec![(meta1.clone(), 1i64.into()), (meta2.clone(), 2i64.into())],
            vec![(resource.resource.unwrap().uuid, 2i64.into())],
        ),
    )?;

    // Only the second resource is retained
    assert_eq!(merged_meta.records, records);

    // Search on first resource
    let searcher = TextSearcher::open(
        TextConfig::default(),
        TestOpener::new(vec![(meta1.clone(), 1i64.into())], vec![]),
    )?;
    let result = searcher.search(&DocumentSearchRequest {
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(result.results.len(), 2);

    // Search on both resources
    let searcher = TextSearcher::open(
        TextConfig::default(),
        TestOpener::new(vec![(meta1, 1i64.into()), (meta2, 2i64.into())], vec![]),
    )?;
    let result = searcher.search(&DocumentSearchRequest {
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(result.results.len(), 4);

    // Search on merged resources
    let searcher = TextSearcher::open(
        TextConfig::default(),
        TestOpener::new(vec![(merged_meta, 1i64.into())], vec![]),
    )?;
    let result = searcher.search(&DocumentSearchRequest {
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(result.results.len(), 2);

    Ok(())
}
