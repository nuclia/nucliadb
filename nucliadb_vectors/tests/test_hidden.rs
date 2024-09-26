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

use nucliadb_core::protos::{
    IndexParagraph, IndexParagraphs, Resource, ResourceId, VectorSearchRequest, VectorSentence,
};
use nucliadb_core::query_language::BooleanExpression;
use nucliadb_core::vectors::{MergeParameters, ResourceWrapper, VectorReader, VectorWriter, VectorsContext};
use nucliadb_core::NodeResult;
use nucliadb_vectors::config::VectorConfig;
use nucliadb_vectors::service::{VectorReaderService, VectorWriterService};
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
fn test_hidden_search() -> NodeResult<()> {
    let workdir = tempdir()?;
    let index_path = workdir.path().join("vectors");
    let mut writer = VectorWriterService::create(&index_path, String::new(), VectorConfig::default())?;

    // Create two resources, one hidden and one not
    let labels = vec!["/q/h".to_string()];
    let hidden_resource = resource(labels);
    writer.set_resource(ResourceWrapper::from(&hidden_resource))?;

    let visible_resource = resource(vec![]);
    writer.set_resource(ResourceWrapper::from(&visible_resource))?;

    // Find all resources
    let reader = VectorReaderService::open(&index_path)?;
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

    // Won't merge results
    let merge_plan = writer.prepare_merge(MergeParameters {
        max_nodes_in_merge: 1000,
        segments_before_merge: 1,
        maximum_deleted_entries: 0,
    })?;
    assert!(merge_plan.is_none());

    Ok(())
}
