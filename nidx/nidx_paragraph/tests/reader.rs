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

use common::test_reader;
use nidx_paragraph::ParagraphSearchRequest;
use nidx_protos::order_by::OrderField;
use nidx_protos::prost_types::Timestamp;
use nidx_protos::resource::ResourceStatus;
use nidx_protos::{
    Faceted, IndexMetadata, IndexParagraph, IndexParagraphs, OrderBy, Resource, ResourceId, StreamRequest,
    TextInformation,
};
use nidx_types::prefilter::PrefilterResult;
use nidx_types::query_language::{self, BooleanExpression};
use std::collections::HashMap;
use std::time::SystemTime;

const DOC1_TI: &str = "This is the first document";
const DOC1_P1: &str = "This is the text of the second paragraph.";
const DOC1_P2: &str = "This should be enough to test the tantivy.";
const DOC1_P3: &str = "But I wanted to make it three anyway.";

fn create_resource(shard_id: String, timestamp: Timestamp) -> Resource {
    const UUID: &str = "f56c58ac-b4f9-4d61-a077-ffccaadd0001";
    let resource_id = ResourceId {
        shard_id: shard_id.to_string(),
        uuid: UUID.to_string(),
    };

    let metadata = IndexMetadata {
        created: Some(timestamp),
        modified: Some(timestamp),
    };

    let ti_title = TextInformation {
        text: DOC1_TI.to_string(),
        labels: vec!["/e/mylabel".to_string()],
    };

    let ti_body = TextInformation {
        text: DOC1_P1.to_string() + DOC1_P2 + DOC1_P3,
        labels: vec!["/f/body".to_string(), "/l/mylabel2".to_string()],
    };

    let mut texts = HashMap::new();
    texts.insert("title".to_string(), ti_title);
    texts.insert("body".to_string(), ti_body);

    let p1 = IndexParagraph {
        start: 0,
        end: DOC1_P1.len() as i32,
        sentences: HashMap::new(),
        vectorsets_sentences: HashMap::new(),
        field: "body".to_string(),
        labels: vec!["/e/myentity".to_string()],
        index: 0,
        split: "".to_string(),
        repeated_in_field: false,
        metadata: None,
    };
    let p1_uuid = format!("{}/{}/{}-{}", UUID, "body", 0, DOC1_P1.len());

    let p2 = IndexParagraph {
        start: DOC1_P1.len() as i32,
        end: (DOC1_P1.len() + DOC1_P2.len()) as i32,
        sentences: HashMap::new(),
        vectorsets_sentences: HashMap::new(),
        field: "body".to_string(),
        labels: vec!["/tantivy".to_string(), "/test".to_string(), "/label1".to_string()],
        index: 1,
        split: "".to_string(),
        repeated_in_field: false,
        metadata: None,
    };
    let p2_uuid = format!(
        "{}/{}/{}-{}",
        UUID,
        "body",
        DOC1_P1.len(),
        DOC1_P1.len() + DOC1_P2.len()
    );

    let p3 = IndexParagraph {
        start: (DOC1_P1.len() + DOC1_P2.len()) as i32,
        end: (DOC1_P1.len() + DOC1_P2.len() + DOC1_P3.len()) as i32,
        sentences: HashMap::new(),
        vectorsets_sentences: HashMap::new(),
        field: "body".to_string(),
        labels: vec!["/three".to_string(), "/label2".to_string()],
        index: 2,
        split: "".to_string(),
        repeated_in_field: false,
        metadata: None,
    };
    let p3_uuid = format!(
        "{}/{}/{}-{}",
        UUID,
        "body",
        DOC1_P1.len() + DOC1_P2.len(),
        DOC1_P1.len() + DOC1_P2.len() + DOC1_P3.len()
    );

    let body_paragraphs = IndexParagraphs {
        paragraphs: [(p1_uuid, p1), (p2_uuid, p2), (p3_uuid, p3)].into_iter().collect(),
    };

    let p4 = IndexParagraph {
        start: 0,
        end: DOC1_TI.len() as i32,
        sentences: HashMap::new(),
        vectorsets_sentences: HashMap::new(),
        field: "title".to_string(),
        labels: vec!["/c/ool".to_string()],
        index: 3,
        split: "".to_string(),
        repeated_in_field: false,
        metadata: None,
    };
    let p4_uuid = format!("{}/{}/{}-{}", UUID, "body", 0, DOC1_TI.len());

    let title_paragraphs = IndexParagraphs {
        paragraphs: [(p4_uuid, p4)].into_iter().collect(),
    };

    let paragraphs = [
        ("body".to_string(), body_paragraphs),
        ("title".to_string(), title_paragraphs),
    ]
    .into_iter()
    .collect();

    Resource {
        resource: Some(resource_id),
        metadata: Some(metadata),
        texts,
        status: ResourceStatus::Processed as i32,
        labels: vec!["/l/mylabel_resource".to_string()],
        paragraphs,
        paragraphs_to_delete: vec![],
        relations: vec![],
        vectors: HashMap::default(),
        vectors_to_delete: HashMap::default(),
        shard_id,
        ..Default::default()
    }
}

#[test]
fn test_total_number_of_results() -> anyhow::Result<()> {
    const UUID: &str = "f56c58ac-b4f9-4d61-a077-ffccaadd0001";

    let seconds = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|t| t.as_secs() as i64)
        .unwrap();
    let timestamp = Timestamp { seconds, nanos: 0 };

    let resource1 = create_resource("shard1".to_string(), timestamp);
    let paragraph_reader_service = test_reader(&resource1);

    // Search on all paragraphs faceted
    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: UUID.to_string(),
        body: "".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 4);
    assert_eq!(result.results.len(), 4);

    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: UUID.to_string(),
        body: "".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 0,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert!(result.results.is_empty());
    assert_eq!(result.total, 4);

    Ok(())
}

#[test]
fn test_filtered_search() -> anyhow::Result<()> {
    const UUID: &str = "f56c58ac-b4f9-4d61-a077-ffccaadd0001";

    let seconds = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|t| t.as_secs() as i64)
        .unwrap();
    let timestamp = Timestamp { seconds, nanos: 0 };

    let resource1 = create_resource("shard1".to_string(), timestamp);
    let paragraph_reader_service = test_reader(&resource1);

    // Only one paragraph  matches
    let mut search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: UUID.to_string(),
        body: "".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    search.filtering_formula = Some(BooleanExpression::Literal("/tantivy".to_string()));
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 1);

    // Two matches due to OR
    let expression = query_language::BooleanOperation {
        operator: query_language::Operator::Or,
        operands: vec![
            BooleanExpression::Literal("/tantivy".to_string()),
            BooleanExpression::Literal("/label2".to_string()),
        ],
    };
    search.filtering_formula = Some(BooleanExpression::Operation(expression));
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 2);

    // No matches due to AND
    let expression = query_language::BooleanOperation {
        operator: query_language::Operator::And,
        operands: vec![
            BooleanExpression::Literal("/tantivy".to_string()),
            BooleanExpression::Literal("/label2".to_string()),
        ],
    };
    search.filtering_formula = Some(BooleanExpression::Operation(expression));
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 0);

    Ok(())
}

#[test]
fn test_new_paragraph() -> anyhow::Result<()> {
    const UUID: &str = "f56c58ac-b4f9-4d61-a077-ffccaadd0001";

    let seconds = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|t| t.as_secs() as i64)
        .unwrap();
    let timestamp = Timestamp { seconds, nanos: 0 };

    let resource1 = create_resource("shard1".to_string(), timestamp);
    let paragraph_reader_service = test_reader(&resource1);
    let faceted = Faceted {
        labels: vec!["".to_string(), "/l".to_string(), "/e".to_string(), "/c".to_string()],
    };

    let order = OrderBy {
        sort_by: OrderField::Created as i32,
        r#type: 0,
        ..Default::default()
    };

    // Search on all paragraphs faceted
    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: UUID.to_string(),
        body: "".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 4);

    // Search on all paragraphs without fields
    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: UUID.to_string(),
        body: "".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 4);

    // Search and filter by min_score
    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: UUID.to_string(),
        body: "should enough".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        min_score: 30.0,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.results.len(), 0);

    // Search on all paragraphs in resource with typo
    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: UUID.to_string(),
        body: "shoupd enaugh".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 1);

    // Search on all paragraphs in resource with typo
    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: UUID.to_string(),
        body: "\"should\" enaugh".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 1);

    // Search typo on all paragraph
    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: "".to_string(),
        body: "shoupd enaugh".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 1);

    // Search with invalid and unbalanced grammar
    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: "".to_string(),
        body: "shoupd + enaugh\"".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.query, "\"shoupd + enaugh\"");
    assert_eq!(result.total, 0);

    // Search with invalid grammar
    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: "".to_string(),
        body: "shoupd + enaugh".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.query, "\"shoupd + enaugh\"");
    assert_eq!(result.total, 0);

    // Empty search
    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: "".to_string(),
        body: "".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 4);

    // Search filter all paragraphs
    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: "".to_string(),
        body: "this is the".to_string(),
        faceted: Some(faceted.clone()),
        order: Some(order),
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 3);

    // Search typo on all paragraph
    let search = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: "".to_string(),
        body: "\"shoupd\"".to_string(),
        faceted: None,
        order: None,
        page_number: 0,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 0);

    let request = StreamRequest {
        shard_id: None,
        ..Default::default()
    };
    let iter = paragraph_reader_service.iterator(&request).unwrap();
    let count = iter.count();
    assert_eq!(count, 4);
    Ok(())
}
