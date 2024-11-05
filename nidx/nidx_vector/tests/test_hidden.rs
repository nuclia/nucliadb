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

use common::{resource, TestOpener};
use nidx_protos::VectorSearchRequest;
use nidx_types::query_language::BooleanExpression;
use nidx_vector::{config::VectorConfig, VectorIndexer, VectorSearcher, VectorsContext};
use std::collections::HashSet;
use tempfile::tempdir;

#[test]
fn test_hidden_search() -> anyhow::Result<()> {
    let config = VectorConfig::default();

    // Create two resources, one hidden and one not
    let labels = vec!["/q/h".to_string()];
    let hidden_resource = resource(labels);
    let hidden_dir = tempdir()?;
    let hidden_segment =
        VectorIndexer.index_resource(hidden_dir.path(), &config, &hidden_resource, "default", true)?.unwrap();

    let visible_resource = resource(vec![]);
    let visible_dir = tempdir()?;
    let visible_segment =
        VectorIndexer.index_resource(visible_dir.path(), &config, &visible_resource, "default", true)?.unwrap();

    // Find all resources
    let reader = VectorSearcher::open(
        config,
        TestOpener::new(vec![(hidden_segment, 1i64.into()), (visible_segment, 2i64.into())], vec![]),
    )?;
    let request = VectorSearchRequest {
        vector: vec![0.5, 0.5, 0.5, 0.5],
        min_score: -1.0,
        result_per_page: 10,
        ..Default::default()
    };
    let all = reader.search(
        &request,
        &VectorsContext {
            filtering_formula: None,
            segment_filtering_formula: None,
        },
    )?;
    assert_eq!(
        HashSet::from_iter(all.documents.into_iter().map(|d| d.doc_id.unwrap().id)),
        HashSet::from([
            format!("{}/a/title/0-5", hidden_resource.resource.unwrap().uuid),
            format!("{}/a/title/0-5", visible_resource.resource.as_ref().unwrap().uuid)
        ])
    );

    // Find only the visible resource
    let visible = reader.search(
        &request,
        &VectorsContext {
            filtering_formula: None,
            segment_filtering_formula: Some(BooleanExpression::Not(Box::new(BooleanExpression::Literal(
                "/q/h".to_string(),
            )))),
        },
    )?;
    assert_eq!(visible.documents.len(), 1);
    assert_eq!(
        visible.documents[0].clone().doc_id.unwrap().id,
        format!("{}/a/title/0-5", visible_resource.resource.as_ref().unwrap().uuid)
    );

    Ok(())
}
