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

use std::collections::{HashMap, HashSet};

use nidx_protos::{IndexParagraph, IndexParagraphs, Resource, ResourceId, VectorSearchRequest, VectorSentence};
use nidx_vector::config::VectorConfig;
use nidx_vector::data_point_provider::reader::{Reader, VectorsContext};
use nidx_vector::data_point_provider::DTrie;
use nidx_vector::indexer::{index_resource, ResourceWrapper};
use nidx_vector::query_language::BooleanExpression;
use tempfile::tempdir;
use uuid::Uuid;

fn resource(labels: Vec<String>) -> Resource {
    let id = Uuid::new_v4().to_string();
    Resource {
        resource: Some(ResourceId {
            shard_id: String::new(),
            uuid: id.clone(),
        }),
        labels,
        paragraphs: HashMap::from([(
            format!("{id}/a/title"),
            IndexParagraphs {
                paragraphs: HashMap::from([(
                    format!("{id}/a/title/0-5"),
                    IndexParagraph {
                        start: 0,
                        end: 5,
                        sentences: HashMap::from([(
                            format!("{id}/a/title/0-5"),
                            VectorSentence {
                                vector: vec![0.5, 0.5, 0.5, rand::random()],
                                metadata: None,
                            },
                        )]),
                        ..Default::default()
                    },
                )]),
            },
        )]),
        ..Default::default()
    }
}

#[test]
fn test_hidden_search() -> anyhow::Result<()> {
    let config = VectorConfig::default();

    // Create two resources, one hidden and one not
    let labels = vec!["/q/h".to_string()];
    let hidden_resource = resource(labels);
    let hidden_dir = tempdir()?;
    let hidden_segment = index_resource(ResourceWrapper::from(&hidden_resource), hidden_dir.path(), &config)?;

    let visible_resource = resource(vec![]);
    let visible_dir = tempdir()?;
    let visible_segment = index_resource(ResourceWrapper::from(&visible_resource), visible_dir.path(), &config)?;

    // Find all resources
    let reader = Reader::open(
        vec![(hidden_segment.unwrap(), 0i64.into()), (visible_segment.unwrap(), 0i64.into())],
        config,
        DTrie::new(),
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
