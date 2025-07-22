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

fn create_resource(shard_id: String) -> (String, Resource) {
    let rid = "f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string();

    let resource_id = ResourceId {
        shard_id: shard_id.clone(),
        uuid: rid.clone(),
    };

    let seconds = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|t| t.as_secs() as i64)
        .unwrap();
    let timestamp = Timestamp { seconds, nanos: 0 };

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
        field: "body".to_string(),
        labels: vec!["/e/myentity".to_string()],
        index: 0,
        ..Default::default()
    };
    let p1_uuid = format!("{}/{}/{}-{}", rid, "body", 0, DOC1_P1.len());

    let p2 = IndexParagraph {
        start: DOC1_P1.len() as i32,
        end: (DOC1_P1.len() + DOC1_P2.len()) as i32,
        field: "body".to_string(),
        labels: vec!["/tantivy".to_string(), "/test".to_string(), "/label1".to_string()],
        index: 1,
        ..Default::default()
    };
    let p2_uuid = format!("{}/{}/{}-{}", rid, "body", DOC1_P1.len(), DOC1_P1.len() + DOC1_P2.len());

    let p3 = IndexParagraph {
        start: (DOC1_P1.len() + DOC1_P2.len()) as i32,
        end: (DOC1_P1.len() + DOC1_P2.len() + DOC1_P3.len()) as i32,
        field: "body".to_string(),
        labels: vec!["/three".to_string(), "/label2".to_string()],
        index: 2,
        ..Default::default()
    };
    let p3_uuid = format!(
        "{}/{}/{}-{}",
        rid,
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
        field: "title".to_string(),
        labels: vec!["/c/ool".to_string()],
        index: 3,
        ..Default::default()
    };
    let p4_uuid = format!("{}/{}/{}-{}", rid, "body", 0, DOC1_TI.len());

    let title_paragraphs = IndexParagraphs {
        paragraphs: [(p4_uuid, p4)].into_iter().collect(),
    };

    let paragraphs = [
        ("body".to_string(), body_paragraphs),
        ("title".to_string(), title_paragraphs),
    ]
    .into_iter()
    .collect();

    let resource = Resource {
        resource: Some(resource_id),
        metadata: Some(metadata),
        texts,
        status: ResourceStatus::Processed as i32,
        labels: vec!["/l/mylabel_resource".to_string()],
        paragraphs,
        paragraphs_to_delete: vec![],
        vectors: HashMap::default(),
        vectors_to_delete: HashMap::default(),
        shard_id,
        ..Default::default()
    };

    (rid, resource)
}

#[test]
fn test_total_number_of_results() -> anyhow::Result<()> {
    let shard_id = "shard1".to_string();
    let (rid, resource) = create_resource(shard_id.clone());
    let paragraph_reader_service = test_reader(&resource);

    let mut request = ParagraphSearchRequest {
        id: shard_id,
        uuid: rid,
        ..Default::default()
    };

    request.result_per_page = 20;
    let result = paragraph_reader_service
        .search(&request, &PrefilterResult::All)
        .unwrap();
    assert_eq!(result.total, 4);
    assert_eq!(result.results.len(), 4);

    request.result_per_page = 0;
    let result = paragraph_reader_service
        .search(&request, &PrefilterResult::All)
        .unwrap();
    assert!(result.results.is_empty());
    assert_eq!(result.total, 4);

    Ok(())
}

#[test]
fn test_filtering_formula() -> anyhow::Result<()> {
    let shard_id = "shard1".to_string();
    let (rid, resource) = create_resource(shard_id.clone());
    let paragraph_reader_service = test_reader(&resource);

    // Only one paragraph matches
    let mut request = ParagraphSearchRequest {
        id: shard_id,
        uuid: rid,
        body: "".to_string(),
        faceted: None,
        order: None,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    request.filtering_formula = Some(BooleanExpression::Literal("/tantivy".to_string()));
    let result = paragraph_reader_service
        .search(&request, &PrefilterResult::All)
        .unwrap();
    assert_eq!(result.total, 1);

    // Two matches due to OR
    let expression = query_language::BooleanOperation {
        operator: query_language::Operator::Or,
        operands: vec![
            BooleanExpression::Literal("/tantivy".to_string()),
            BooleanExpression::Literal("/label2".to_string()),
        ],
    };
    request.filtering_formula = Some(BooleanExpression::Operation(expression));
    let result = paragraph_reader_service
        .search(&request, &PrefilterResult::All)
        .unwrap();
    assert_eq!(result.total, 2);

    // No matches due to AND
    let expression = query_language::BooleanOperation {
        operator: query_language::Operator::And,
        operands: vec![
            BooleanExpression::Literal("/tantivy".to_string()),
            BooleanExpression::Literal("/label2".to_string()),
        ],
    };
    request.filtering_formula = Some(BooleanExpression::Operation(expression));
    let result = paragraph_reader_service
        .search(&request, &PrefilterResult::All)
        .unwrap();
    assert_eq!(result.total, 0);

    Ok(())
}

#[test]
fn test_keyword_and_fuzzy_queries() -> anyhow::Result<()> {
    let shard_id = "shard1".to_string();
    let (rid, resource) = create_resource(shard_id.clone());
    let paragraph_reader_service = test_reader(&resource);

    let paragraph_search = |query: &str| {
        let request = ParagraphSearchRequest {
            id: shard_id.clone(),
            uuid: rid.clone(),
            body: query.to_string(),
            faceted: None,
            order: None,
            result_per_page: 20,
            only_faceted: false,
            ..Default::default()
        };
        paragraph_reader_service
            .search(&request, &PrefilterResult::All)
            .unwrap()
    };

    // Empty query (matches everything)
    let result = paragraph_search("");
    assert_eq!(result.results.len(), 4);

    // Correct terms
    let result = paragraph_search("should enough");
    assert_eq!(result.results.len(), 1);

    // Typos of Levenshtein distance 1
    let result = paragraph_search("shoupd enaugh");
    assert_eq!(result.results.len(), 1);

    // Typos of Levenshtein distance 2
    let result = paragraph_search("sJoupd enaugJ");
    assert_eq!(result.results.len(), 0);

    // Exact match
    let result = paragraph_search("\"should\"");
    assert_eq!(result.results.len(), 1);

    // Exact match with typo
    let result = paragraph_search("\"shoudl\"");
    assert_eq!(result.results.len(), 0);

    // Exact match with typo and a correct term
    let result = paragraph_search("\"shoudl\" enough");
    assert_eq!(result.results.len(), 1);

    // Exact match with typo and term with typo (distance 1)
    let result = paragraph_search("\"shoudl\" enoguh");
    assert_eq!(result.results.len(), 1);

    // Exact match with typo and term with typo (distance 2)
    let result = paragraph_search("\"shoudl\" eonguh");
    assert_eq!(result.results.len(), 0);

    // Search with unclosed quotes (we are lenient, so we don't care)

    // Search with invalid tantivy grammar (we don't care as we don't use it)
    let result = paragraph_search("shoupd + enaugh\"");
    assert_eq!(result.results.len(), 1);
    let result = paragraph_search("shoupd + enaugh");
    assert_eq!(result.results.len(), 1);

    Ok(())
}

#[test]
fn test_min_score_filter() -> anyhow::Result<()> {
    let shard_id = "shard1".to_string();
    let (rid, resource) = create_resource(shard_id.clone());
    let paragraph_reader_service = test_reader(&resource);

    let mut request = ParagraphSearchRequest {
        id: shard_id,
        uuid: rid,
        result_per_page: 20,
        ..Default::default()
    };

    request.min_score = 0.0;
    let result = paragraph_reader_service
        .search(&request, &PrefilterResult::All)
        .unwrap();
    assert_eq!(result.results.len(), 4);

    request.min_score = 30.0;
    let result = paragraph_reader_service
        .search(&request, &PrefilterResult::All)
        .unwrap();
    assert_eq!(result.results.len(), 0);
    // XXX: this shows the weird behavior we have implemented. Here total makes no sense
    assert_eq!(result.total, 4);

    Ok(())
}

#[test]
fn test_faceted_search() -> anyhow::Result<()> {
    let shard_id = "shard1".to_string();
    let (rid, resource) = create_resource(shard_id.clone());
    let paragraph_reader_service = test_reader(&resource);
    let faceted = Faceted {
        labels: vec!["".to_string(), "/l".to_string(), "/e".to_string(), "/c".to_string()],
    };

    // Filter with facets and order
    let request = ParagraphSearchRequest {
        id: shard_id,
        uuid: rid,
        faceted: Some(faceted.clone()),
        result_per_page: 20,
        ..Default::default()
    };
    let result = paragraph_reader_service
        .search(&request, &PrefilterResult::All)
        .unwrap();
    assert_eq!(result.facets.len(), 3);
    assert!(!result.facets.contains_key(""));
    assert!(result.facets.contains_key("/c"));
    assert!(result.facets.contains_key("/e"));
    assert!(result.facets.contains_key("/l"));

    Ok(())
}

#[test]
fn test_order_by() -> anyhow::Result<()> {
    let shard_id = "shard1".to_string();
    let (rid, resource) = create_resource(shard_id.clone());
    let paragraph_reader_service = test_reader(&resource);

    let order = OrderBy {
        sort_by: OrderField::Created as i32,
        r#type: 0,
        ..Default::default()
    };

    // TODO: we need two resources to test any order (creation/modification dates)

    // Use a sort order
    let request = ParagraphSearchRequest {
        id: "shard1".to_string(),
        uuid: "".to_string(),
        body: "this is the".to_string(),
        order: Some(order),
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };
    let result = paragraph_reader_service
        .search(&request, &PrefilterResult::All)
        .unwrap();
    assert_eq!(result.total, 3);

    Ok(())
}

#[test]
fn test_paragraph_streaming() {
    let shard_id = "shard1".to_string();
    let (rid, resource) = create_resource(shard_id.clone());
    let paragraph_reader_service = test_reader(&resource);

    let request = StreamRequest {
        shard_id: None,
        ..Default::default()
    };
    let iter = paragraph_reader_service.iterator(&request).unwrap();
    let count = iter.count();
    assert_eq!(count, 4);
}

#[test]
fn test_query_parsing() -> anyhow::Result<()> {
    // This test validates we are properly removing single quotes from queries
    // to avoid issues with tantivy + our own parsing.
    //
    // For further context, see:
    // https://github.com/nuclia/nucliadb/pull/3216
    //
    let shard_id = "shard1".to_string();
    let (rid, resource) = create_resource(shard_id.clone());
    let paragraph_reader_service = test_reader(&resource);

    // Only one paragraph  matches
    let mut search = ParagraphSearchRequest {
        id: shard_id,
        uuid: rid,
        body: "".to_string(),
        faceted: None,
        order: None,
        result_per_page: 20,
        only_faceted: false,
        ..Default::default()
    };

    search.body = "some document".to_string();
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 1);

    search.body = "some ' document".to_string();
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 1);

    Ok(())
}
