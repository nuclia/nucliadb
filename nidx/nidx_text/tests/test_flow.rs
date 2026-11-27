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
use nidx_text::{DocumentSearchRequest, FieldUid, TextConfig, TextIndexer, TextSearcher};
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

#[test]
fn test_skip_tokenization_preserves_stored_text_and_field_lookup() -> anyhow::Result<()> {
    let mut resource = little_prince("shard", None);
    resource.texts.get_mut("a/title").unwrap().skip_tokenization = true;
    let field = FieldUid {
        rid: resource.resource.as_ref().unwrap().uuid.clone(),
        field_type: "a".into(),
        field_name: "title".into(),
        split: None,
    };
    let segment_dir = tempdir()?;
    let segment = TextIndexer
        .index_resource(segment_dir.path(), TextConfig::default(), &resource)?
        .unwrap();
    let merge_dir = tempdir()?;
    let merged = TextIndexer.merge(
        merge_dir.path(),
        TextConfig::default(),
        TestOpener::new(vec![(segment.clone(), 1i64.into())], vec![]),
    )?;
    for meta in [segment, merged] {
        let searcher = TextSearcher::open(
            TextConfig::default(),
            TestOpener::new(vec![(meta, 1i64.into())], vec![]),
        )?;
        let fields = searcher.reader.get_fields_text(vec![field.clone()])?;
        assert_eq!(fields[&field].as_deref(), Some("The little prince"));
        for (body, expected) in [("little", 0), ("friendship", 1), ("", 2)] {
            let result = searcher.search(&DocumentSearchRequest {
                body: body.into(),
                result_per_page: 20,
                ..Default::default()
            })?;
            assert_eq!(result.results.len(), expected, "query: {body:?}");
        }
    }
    Ok(())
}

#[test]
fn test_batch_skips_texts_without_losing_other_resources() -> anyhow::Result<()> {
    let mut skipped = little_prince("shard", None);
    skipped.skip_texts = true;
    let kept = little_prince("shard", None);
    let rid = kept.resource.as_ref().unwrap().uuid.clone();
    let dir = tempdir()?;
    let segment = TextIndexer
        .index_resources(dir.path(), TextConfig::default(), &[skipped, kept])?
        .unwrap();
    assert_eq!(segment.records, 2);
    let searcher = TextSearcher::open(
        TextConfig::default(),
        TestOpener::new(vec![(segment, 1i64.into())], vec![]),
    )?;
    let response = searcher.search(&DocumentSearchRequest {
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(response.results.len(), 2);
    assert!(response.results.iter().all(|result| result.uuid == rid));
    Ok(())
}
