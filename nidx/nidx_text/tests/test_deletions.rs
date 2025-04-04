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
