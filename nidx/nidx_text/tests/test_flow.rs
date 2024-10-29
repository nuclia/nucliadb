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

use nidx_protos::DocumentSearchRequest;
use nidx_tests::little_prince;
use nidx_text::{TextIndexer, TextSearcher};
use tempfile::tempdir;

#[test]
fn test_index_merge_search() -> anyhow::Result<()> {
    let resource = little_prince("shard");
    let segment_dir = tempdir()?;
    let meta1 = TextIndexer.index_resource(segment_dir.path(), &resource)?;
    let records = meta1.records;

    let segment_dir2 = tempdir()?;
    let meta2 = TextIndexer.index_resource(segment_dir2.path(), &resource)?;

    let merge_dir = tempdir()?;
    let merged_meta = TextIndexer.merge(
        merge_dir.path(),
        vec![(meta1.clone(), 1i64.into()), (meta2.clone(), 2i64.into())],
        &[(2i64.into(), &vec![resource.resource.unwrap().uuid])],
    )?;

    // Only the second resource is retained
    assert_eq!(merged_meta.records, records);

    // Search on first resource
    let searcher = TextSearcher::open(vec![(1i64.into(), vec![meta1.clone()], vec![])])?;
    let result = searcher.search(&DocumentSearchRequest {
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(result.results.len(), 2);

    // Search on both resources
    let searcher = TextSearcher::open(vec![(1i64.into(), vec![meta1, meta2], vec![])])?;
    let result = searcher.search(&DocumentSearchRequest {
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(result.results.len(), 4);

    // Search on merged resources
    let searcher = TextSearcher::open(vec![(1i64.into(), vec![merged_meta], vec![])])?;
    let result = searcher.search(&DocumentSearchRequest {
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(result.results.len(), 2);

    Ok(())
}
