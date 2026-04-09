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
