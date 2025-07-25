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

/// Helper struct to build fields with content
struct Field<'a> {
    field_id: &'a str,
    // (text, labels)
    paragraphs: Vec<(&'a str, Vec<String>)>,
    // field labels
    labels: Vec<String>,
}

fn create_resource(shard_id: String) -> (String, Resource) {
    let rid = "f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string();
    let fields = vec![
        Field {
            field_id: "title",
            paragraphs: vec![(DOC1_TI, vec!["/c/ool".to_string()])],
            labels: vec!["/e/mylabel".to_string()],
        },
        Field {
            field_id: "mytext",
            paragraphs: vec![
                (DOC1_P1, vec!["/e/myentity".to_string()]),
                (
                    DOC1_P2,
                    vec!["/tantivy".to_string(), "/test".to_string(), "/label1".to_string()],
                ),
                (DOC1_P3, vec!["/three".to_string(), "/label2".to_string()]),
            ],
            labels: vec!["/f/body".to_string(), "/l/mylabel2".to_string()],
        },
    ];

    create_resource_with_fields(shard_id, rid, fields)
}

fn create_tricky_resource(shard_id: String) -> (String, Resource) {
    let rid = "c0ab922f-d739-8a68-b49e-fad80278cbb0".to_string();
    let fields = vec![
        Field {
            field_id: "title",
            paragraphs: vec![("That's a too *tricky* resource", vec![])],
            labels: vec![],
        },
        Field {
            field_id: "mytext",
            paragraphs: vec![
                ("It's very important to do-stuff", vec![]),
                ("It's not that important to do-stuff", vec![]),
                ("W'h'a't a -w-e-i-r-d p\"ara\"gra\"ph", vec![]),
            ],
            labels: vec![],
        },
    ];

    create_resource_with_fields(shard_id, rid, fields)
}

fn create_resource_with_fields(shard_id: String, rid: String, fields: Vec<Field>) -> (String, Resource) {
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

    let mut texts = HashMap::new();
    let mut field_paragraphs = HashMap::new();

    for Field {
        field_id,
        paragraphs,
        labels,
    } in fields.into_iter()
    {
        let mut text = String::new();
        let mut position = 0;
        let mut index_paragraphs = HashMap::new();

        for (index, (paragraph, paragraph_labels)) in paragraphs.into_iter().enumerate() {
            // incrementaly build the whole text
            text.push_str(paragraph);

            let paragraph_pb = IndexParagraph {
                field: field_id.to_string(),
                start: position,
                end: position + (paragraph.len() as i32),
                index: index as u64,
                labels: paragraph_labels,
                ..Default::default()
            };
            let paragraph_id = format!("{rid}/{field_id}/{}-{}", paragraph_pb.start, paragraph_pb.end);
            index_paragraphs.insert(paragraph_id, paragraph_pb);

            // update position for the next iteration
            position += paragraph.len() as i32;
        }

        let text_info = TextInformation { text, labels };
        texts.insert(field_id.to_string(), text_info);

        field_paragraphs.insert(
            field_id.to_string(),
            IndexParagraphs {
                paragraphs: index_paragraphs,
            },
        );
    }

    let resource = Resource {
        shard_id,
        resource: Some(resource_id),
        metadata: Some(metadata),
        texts,
        status: ResourceStatus::Processed as i32,
        paragraphs: field_paragraphs,
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
    let (_, resource) = create_resource(shard_id.clone());
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
    let (_, resource) = create_resource(shard_id.clone());
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

#[test]
fn test_query_parsing_weird_stuff() -> anyhow::Result<()> {
    let shard_id = "shard1".to_string();
    let (rid, resource) = create_tricky_resource(shard_id.clone());
    let paragraph_reader_service = test_reader(&resource);

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

    search.body = "important".to_string();
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 2);

    // removes all stop words except the last and matches "to" exactly (as it's too short for fuzzy)
    search.body = "it's not that to".to_string();
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.ematches, vec!["to"]);
    assert_eq!(result.total, 2);

    search.body = "\"It's very important to do-stuff\"".to_string();
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 1);

    // the word p"ara"gra"ph has been splitted, so `paragraph` doesn't match but `ara` does.
    search.body = "paragraph".to_string();
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 0);
    search.body = "ara".to_string();
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 1);
    search.body = "ph".to_string();
    let result = paragraph_reader_service.search(&search, &PrefilterResult::All).unwrap();
    assert_eq!(result.total, 1);

    Ok(())
}
