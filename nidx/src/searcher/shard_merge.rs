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

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use nidx_protos::{
    DocumentResult, DocumentSearchResponse, FacetResult, FacetResults, GraphSearchResponse, ParagraphResult,
    ParagraphSearchResponse, SearchRequest, SearchResponse, VectorSearchResponse, document_result, order_by,
    paragraph_result,
};

#[derive(Clone, Copy)]
pub struct OrderBy {
    expr: SortExpr,
    descending: bool,
}

#[derive(Clone, Copy)]
pub enum SortExpr {
    Score,
    Date,
}

#[derive(Clone, Copy)]
pub struct Limit(pub usize);

/// Merge mulitiple shard responses into a single one. During the process,
/// results keep the original request sort order and an extra limit is applied
/// to remove unnecessary results
///
/// Note however that date ordering is only possible on fulltext/keyword search
/// (this is validated before searching, so we don't care much here)
///
pub fn merge(shard_responses: Vec<SearchResponse>, order_by: OrderBy, limit: Limit) -> SearchResponse {
    let mut shard_ids = vec![];
    let mut document_responses = Vec::with_capacity(shard_responses.len());
    let mut paragraph_responses = Vec::with_capacity(shard_responses.len());
    let mut vector_responses = Vec::with_capacity(shard_responses.len());
    let mut graph_responses = Vec::with_capacity(shard_responses.len());

    for response in shard_responses {
        shard_ids.extend(response.shard_ids);

        if let Some(document) = response.document {
            document_responses.push(document);
        }
        if let Some(paragraph) = response.paragraph {
            paragraph_responses.push(paragraph);
        }
        if let Some(vector) = response.vector {
            vector_responses.push(vector);
        }
        if let Some(graph) = response.graph {
            graph_responses.push(graph);
        }
    }

    let document =
        (!document_responses.is_empty()).then(|| merge_document_responses(document_responses, order_by, limit));
    let paragraph =
        (!paragraph_responses.is_empty()).then(|| merge_paragraph_responses(paragraph_responses, order_by, limit));
    let vector = (!vector_responses.is_empty()).then(|| merge_vector_responses(vector_responses, limit));
    let graph = (!graph_responses.is_empty()).then(|| merge_graph_responses(graph_responses, limit));

    SearchResponse {
        shard_ids,
        document,
        paragraph,
        vector,
        graph,
    }
}

fn merge_document_responses(
    responses: Vec<DocumentSearchResponse>,
    order_by: OrderBy,
    limit: Limit,
) -> DocumentSearchResponse {
    debug_assert!(!responses.is_empty(), "must pass at least 1 shard response");
    let mut merged = DocumentSearchResponse::default();

    let mut facets = vec![];
    let mut results = vec![];
    for response in responses {
        merged.total += response.total;
        merged.query = response.query;
        merged.next_page = merged.next_page || response.next_page;

        results.push(response.results.into_iter());
        facets.push(response.facets);
    }
    merged.results = results
        .into_iter()
        .kmerge_by(sort_documents_fn(order_by))
        .take(limit.0)
        .collect();
    merged.facets = merge_facets(facets);
    merged
}

#[allow(clippy::type_complexity)]
fn sort_documents_fn(order_by: OrderBy) -> Box<dyn Fn(&DocumentResult, &DocumentResult) -> bool> {
    match order_by.expr {
        SortExpr::Score => {
            debug_assert!(order_by.descending, "order by ascending score is not implemented");
            Box::new(|a, b| match (a.sort_value, b.sort_value) {
                (
                    Some(document_result::SortValue::Score(nidx_protos::ResultScore {
                        bm25: a_bm25,
                        booster: a_booster,
                    })),
                    Some(document_result::SortValue::Score(nidx_protos::ResultScore {
                        bm25: b_bm25,
                        booster: b_booster,
                    })),
                ) => a_bm25.total_cmp(&b_bm25).then(a_booster.total_cmp(&b_booster)).is_gt(),
                _ => {
                    unreachable!("index always return values with the same order_by as we have")
                }
            })
        }
        SortExpr::Date => Box::new(move |a, b| match (a.sort_value, b.sort_value) {
            (Some(document_result::SortValue::Date(a)), Some(document_result::SortValue::Date(b))) => {
                if order_by.descending {
                    // Descending order: from future to past
                    a.seconds.cmp(&b.seconds).then(a.nanos.cmp(&b.nanos)).is_gt()
                } else {
                    // Ascending order: from past to future
                    a.seconds.cmp(&b.seconds).then(a.nanos.cmp(&b.nanos)).is_lt()
                }
            }
            _ => {
                unreachable!("index always return dates as always indexes them");
            }
        }),
    }
}

fn merge_paragraph_responses(
    responses: Vec<ParagraphSearchResponse>,
    order_by: OrderBy,
    limit: Limit,
) -> ParagraphSearchResponse {
    debug_assert!(!responses.is_empty(), "must pass at least 1 shard response");
    let mut merged = ParagraphSearchResponse::default();

    let mut results = vec![];
    let mut facets = vec![];
    let mut ematches = HashSet::new();
    for response in responses {
        merged.total += response.total;
        merged.query = response.query;
        merged.next_page = merged.next_page || response.next_page;

        results.push(response.results.into_iter());
        ematches.extend(response.ematches);
        facets.push(response.facets);
    }
    merged.results = results
        .into_iter()
        .kmerge_by(sort_paragraphs_fn(order_by))
        .take(limit.0)
        .collect();
    merged.facets = merge_facets(facets);
    merged.ematches = ematches.into_iter().collect();

    merged
}

#[allow(clippy::type_complexity)]
fn sort_paragraphs_fn(order_by: OrderBy) -> Box<dyn Fn(&ParagraphResult, &ParagraphResult) -> bool> {
    match order_by.expr {
        SortExpr::Score => {
            debug_assert!(order_by.descending, "order by ascending score is not implemented");
            Box::new(|a, b| match (a.sort_value, b.sort_value) {
                (
                    Some(paragraph_result::SortValue::Score(nidx_protos::ResultScore {
                        bm25: a_bm25,
                        booster: a_booster,
                    })),
                    Some(paragraph_result::SortValue::Score(nidx_protos::ResultScore {
                        bm25: b_bm25,
                        booster: b_booster,
                    })),
                ) => a_bm25.total_cmp(&b_bm25).then(a_booster.total_cmp(&b_booster)).is_gt(),
                _ => {
                    unreachable!("index always return values with the same order_by as we have")
                }
            })
        }
        SortExpr::Date => {
            Box::new(move |a, b| match (a.sort_value, b.sort_value) {
                (Some(paragraph_result::SortValue::Date(a)), Some(paragraph_result::SortValue::Date(b))) => {
                    if order_by.descending {
                        // Descending order: from future to past
                        a.seconds.cmp(&b.seconds).then(a.nanos.cmp(&b.nanos)).is_gt()
                    } else {
                        // Ascending order: from past to future
                        a.seconds.cmp(&b.seconds).then(a.nanos.cmp(&b.nanos)).is_lt()
                    }
                }
                _ => {
                    unreachable!("index always return dates as always indexes them");
                }
            })
        }
    }
}

fn merge_vector_responses(responses: Vec<VectorSearchResponse>, limit: Limit) -> VectorSearchResponse {
    debug_assert!(!responses.is_empty(), "must pass at least 1 shard response");
    let merged = responses
        .into_iter()
        .map(|response| response.documents)
        .kmerge_by(|a, b| a.score >= b.score)
        .take(limit.0)
        .collect();
    VectorSearchResponse { documents: merged }
}

fn merge_graph_responses(responses: Vec<GraphSearchResponse>, _limit: Limit) -> GraphSearchResponse {
    debug_assert!(!responses.is_empty(), "must pass at least 1 shard response");
    let mut merged = GraphSearchResponse::default();

    for response in responses {
        let nodes_offset = merged.nodes.len() as u32;
        let relations_offset = merged.relations.len() as u32;

        // paths contain indexes to nodes and relations, we must offset them
        // while merging responses to maintain valid data
        for mut path in response.graph {
            path.source += nodes_offset;
            path.relation += relations_offset;
            path.destination += nodes_offset;
            merged.graph.push(path);
        }

        merged.nodes.extend(response.nodes);
        merged.relations.extend(response.relations);
        merged.scores.extend(response.scores);
    }

    // TODO: now that we have scores, we can cut. This is not implemented in Python though

    merged
}

fn merge_facets(shards_facets: Vec<HashMap<String, FacetResults>>) -> HashMap<String, FacetResults> {
    // Each shard produces a hashmap of the facet root to counts per actual
    // facet, i.e., {"/l": [{tag: "/l/mylabel", total: 2}, ...], ...}.
    //
    // We must merge updating counts for the same facet in different shards

    let mut counts = HashMap::new();
    for facets in shards_facets {
        for (group, values) in facets {
            for facet_result in values.facetresults {
                counts
                    .entry((group.clone(), facet_result.tag))
                    .and_modify(|total| *total += facet_result.total)
                    .or_insert(facet_result.total);
            }
        }
    }

    let mut merged = HashMap::new();
    for ((group, tag), total) in counts {
        merged
            .entry(group)
            .and_modify(|facet_results: &mut FacetResults| {
                facet_results.facetresults.push(FacetResult {
                    tag: tag.clone(),
                    total,
                })
            })
            .or_insert(FacetResults {
                facetresults: vec![FacetResult { tag, total }],
            });
    }

    merged
}

impl From<&SearchRequest> for OrderBy {
    fn from(value: &SearchRequest) -> Self {
        match value.order {
            Some(order) => {
                let descending = match order.r#type() {
                    order_by::OrderType::Desc => true,
                    order_by::OrderType::Asc => false,
                };
                Self {
                    expr: SortExpr::Date,
                    descending,
                }
            }
            // without explicit order, the greater the score, the most important
            None => Self {
                expr: SortExpr::Score,
                descending: true,
            },
        }
    }
}

#[cfg(test)]
impl Default for OrderBy {
    fn default() -> Self {
        Self {
            expr: SortExpr::Score,
            descending: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nidx_protos::{FacetResult, FacetResults, SearchResponse};

    use crate::searcher::shard_merge::{Limit, OrderBy};

    use super::merge;

    #[test]
    fn test_merge_nothing() {
        let merged = merge(vec![], OrderBy::default(), Limit(20));
        assert!(merged.document.is_none());
        assert!(merged.paragraph.is_none());
        assert!(merged.vector.is_none());
        assert!(merged.graph.is_none());

        let empty = SearchResponse {
            shard_ids: vec![],
            document: None,
            paragraph: None,
            vector: None,
            graph: None,
        };
        let merged = merge(vec![empty.clone(), empty], OrderBy::default(), Limit(20));
        assert!(merged.document.is_none());
        assert!(merged.paragraph.is_none());
        assert!(merged.vector.is_none());
        assert!(merged.graph.is_none());
    }

    mod documents {
        use nidx_protos::document_result;
        use nidx_protos::prost_types::Timestamp;
        use nidx_protos::{DocumentResult, DocumentSearchResponse, FacetResult, SearchResponse};

        use crate::searcher::shard_merge::OrderBy;
        use crate::searcher::shard_merge::{Limit, SortExpr};

        use super::merge;
        use super::proto_facets;

        #[test]
        fn test_merge_documents_query() {
            let document = |query: &str| SearchResponse {
                document: Some(DocumentSearchResponse {
                    query: query.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            };

            // `query` is taken from any request. As the fulltext query is common
            // across shards, it shouldn't mind which one to pick

            let merged = merge(
                vec![document("my query"), document("my query")],
                OrderBy::default(),
                Limit(20),
            );
            assert_eq!(merged.document.unwrap().query, "my query");

            let merged = merge(
                vec![document("my query"), document("my second query")],
                OrderBy::default(),
                Limit(20),
            );
            assert!(
                merged.document.as_ref().unwrap().query == "my query"
                    || merged.document.as_ref().unwrap().query == "my second query"
            )
        }

        #[test]
        fn test_merge_documents_total() {
            let document = |total: i32| SearchResponse {
                document: Some(DocumentSearchResponse {
                    total,
                    ..Default::default()
                }),
                ..Default::default()
            };

            // `total` is the count of matched documents (not necessarily
            // returned). The merged value is the sum of all

            let merged = merge(vec![document(10), document(15)], OrderBy::default(), Limit(20));
            assert_eq!(merged.document.unwrap().total, 25);

            let merged = merge(vec![document(0), document(15)], OrderBy::default(), Limit(20));
            assert_eq!(merged.document.unwrap().total, 15);

            let merged = merge(vec![document(0), document(0)], OrderBy::default(), Limit(20));
            assert_eq!(merged.document.unwrap().total, 0);
        }

        #[test]
        fn test_merge_documents_next_page() {
            let document = |next_page: bool| SearchResponse {
                document: Some(DocumentSearchResponse {
                    next_page,
                    ..Default::default()
                }),
                ..Default::default()
            };

            // `next_page` is true if any shard has next_page as true
            for a in [false, true] {
                for b in [false, true] {
                    let merged = merge(vec![document(a), document(b)], OrderBy::default(), Limit(20));
                    assert_eq!(merged.document.unwrap().next_page, a | b);
                }
            }
        }

        #[test]
        fn test_merge_documents_facets() {
            let document = |facets: Vec<(&str, i32)>| SearchResponse {
                document: Some(DocumentSearchResponse {
                    facets: proto_facets(facets),
                    ..Default::default()
                }),
                ..Default::default()
            };

            // `facets` are aggregated and counts are added together

            let merged = merge(
                vec![
                    document(vec![("/l/label-A", 10), ("/l/label-B", 5)]),
                    document(vec![("/l/label-A", 3), ("/l/label-C", 7), ("/e/table", 12)]),
                    document(vec![("/e/chair", 3), ("/e/table", 20)]),
                ],
                OrderBy::default(),
                Limit(20),
            )
            .document
            .unwrap();
            assert_eq!(merged.facets["/l"].facetresults.len(), 3);
            assert!(merged.facets["/l"].facetresults.contains(&FacetResult {
                tag: "/l/label-A".to_string(),
                total: 13
            }));
            assert!(merged.facets["/l"].facetresults.contains(&FacetResult {
                tag: "/l/label-B".to_string(),
                total: 5
            }));
            assert!(merged.facets["/l"].facetresults.contains(&FacetResult {
                tag: "/l/label-C".to_string(),
                total: 7
            }));
            assert_eq!(merged.facets["/e"].facetresults.len(), 2);
            assert!(merged.facets["/e"].facetresults.contains(&FacetResult {
                tag: "/e/chair".to_string(),
                total: 3
            }));
            assert!(merged.facets["/e"].facetresults.contains(&FacetResult {
                tag: "/e/table".to_string(),
                total: 32
            }));
        }

        #[test]
        fn test_merge_documents_with_limit() {
            let shard = SearchResponse {
                document: Some(DocumentSearchResponse {
                    results: vec![
                        DocumentResult {
                            sort_value: Some(document_result::SortValue::Score(nidx_protos::ResultScore::default())),
                            ..Default::default()
                        };
                        20
                    ],
                    ..Default::default()
                }),
                ..Default::default()
            };

            let merged = merge(vec![shard.clone(), shard.clone()], OrderBy::default(), Limit(50))
                .document
                .unwrap();
            assert_eq!(merged.results.len(), 40);

            let merged = merge(vec![shard.clone(), shard.clone()], OrderBy::default(), Limit(20))
                .document
                .unwrap();
            assert_eq!(merged.results.len(), 20);
        }

        #[test]
        fn test_merge_document_results_by_score() {
            let document = |rid: &str, score: f32, booster: f32| DocumentResult {
                uuid: rid.to_string(),
                field: "a/title".to_string(),
                sort_value: Some(document_result::SortValue::Score(nidx_protos::ResultScore {
                    bm25: score,
                    booster,
                })),
                ..Default::default()
            };
            let response = |documents: Vec<DocumentResult>| SearchResponse {
                document: Some(DocumentSearchResponse {
                    total: 100,
                    results: documents,
                    ..Default::default()
                }),
                ..Default::default()
            };

            let merged = merge(
                vec![
                    response(vec![document("foo", 3.0, 1.0), document("bar", 2.0, 2.0)]),
                    response(vec![document("baz", 4.0, 1.0), document("quux", 2.0, 1.0)]),
                ],
                OrderBy {
                    expr: SortExpr::Score,
                    descending: true,
                },
                Limit(20),
            )
            .document
            .unwrap();
            assert_eq!(merged.results.len(), 4);
            assert_eq!(merged.results[0].uuid, "baz");
            assert_eq!(merged.results[1].uuid, "foo");
            assert_eq!(merged.results[2].uuid, "bar");
            assert_eq!(merged.results[3].uuid, "quux");
        }

        #[test]
        fn test_merge_document_results_by_date() {
            let document = |rid: &str, seconds: i64, nanos: i32| DocumentResult {
                uuid: rid.to_string(),
                field: "a/title".to_string(),
                sort_value: Some(document_result::SortValue::Date(Timestamp { seconds, nanos })),
                ..Default::default()
            };
            let response = |documents: Vec<DocumentResult>| SearchResponse {
                document: Some(DocumentSearchResponse {
                    total: 100,
                    results: documents,
                    ..Default::default()
                }),
                ..Default::default()
            };

            let merged = merge(
                vec![
                    response(vec![document("foo", 3, 1), document("bar", 2, 2)]),
                    response(vec![document("baz", 4, 1), document("quux", 2, 1)]),
                ],
                OrderBy {
                    expr: SortExpr::Date,
                    descending: true,
                },
                Limit(20),
            )
            .document
            .unwrap();
            assert_eq!(merged.results.len(), 4);
            assert_eq!(merged.results[0].uuid, "baz");
            assert_eq!(merged.results[1].uuid, "foo");
            assert_eq!(merged.results[2].uuid, "bar");
            assert_eq!(merged.results[3].uuid, "quux");

            let merged = merge(
                vec![
                    response(vec![document("bar", 2, 2), document("foo", 3, 1)]),
                    response(vec![document("quux", 2, 1), document("baz", 4, 1)]),
                ],
                OrderBy {
                    expr: SortExpr::Date,
                    descending: false,
                },
                Limit(20),
            )
            .document
            .unwrap();
            assert_eq!(merged.results.len(), 4);
            assert_eq!(merged.results[0].uuid, "quux");
            assert_eq!(merged.results[1].uuid, "bar");
            assert_eq!(merged.results[2].uuid, "foo");
            assert_eq!(merged.results[3].uuid, "baz");
        }
    }

    mod paragraphs {
        use nidx_protos::prost_types::Timestamp;
        use nidx_protos::{FacetResult, ParagraphSearchResponse, SearchResponse};
        use nidx_protos::{ParagraphResult, paragraph_result};

        use crate::searcher::shard_merge::OrderBy;
        use crate::searcher::shard_merge::{Limit, SortExpr};

        use super::merge;
        use super::proto_facets;

        #[test]
        fn test_merge_paragraphs_query() {
            let paragraph = |query: &str| SearchResponse {
                paragraph: Some(ParagraphSearchResponse {
                    query: query.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            };

            // `query` is taken from any request. As the keyword query is common
            // across shards, it shouldn't mind which one to pick

            let merged = merge(
                vec![paragraph("my query"), paragraph("my query")],
                OrderBy::default(),
                Limit(20),
            );
            assert_eq!(merged.paragraph.unwrap().query, "my query");

            let merged = merge(
                vec![paragraph("my query"), paragraph("my second query")],
                OrderBy::default(),
                Limit(20),
            );
            assert!(
                merged.paragraph.as_ref().unwrap().query == "my query"
                    || merged.paragraph.as_ref().unwrap().query == "my second query"
            )
        }

        #[test]
        fn test_merge_paragraphs_total() {
            let paragraph = |total: i32| SearchResponse {
                paragraph: Some(ParagraphSearchResponse {
                    total,
                    ..Default::default()
                }),
                ..Default::default()
            };

            // `total` is the count of matched paragraphs (not necessarily
            // returned). The merged value is the sum of all

            let merged = merge(vec![paragraph(10), paragraph(15)], OrderBy::default(), Limit(20));
            assert_eq!(merged.paragraph.unwrap().total, 25);

            let merged = merge(vec![paragraph(0), paragraph(15)], OrderBy::default(), Limit(20));
            assert_eq!(merged.paragraph.unwrap().total, 15);

            let merged = merge(vec![paragraph(0), paragraph(0)], OrderBy::default(), Limit(20));
            assert_eq!(merged.paragraph.unwrap().total, 0);
        }

        #[test]
        fn test_merge_paragraphs_next_page() {
            let paragraph = |next_page: bool| SearchResponse {
                paragraph: Some(ParagraphSearchResponse {
                    next_page,
                    ..Default::default()
                }),
                ..Default::default()
            };

            // `next_page` is true if any shard has next_page as true
            for a in [false, true] {
                for b in [false, true] {
                    let merged = merge(vec![paragraph(a), paragraph(b)], OrderBy::default(), Limit(20));
                    assert_eq!(merged.paragraph.unwrap().next_page, a | b);
                }
            }
        }

        #[test]
        fn test_merge_paragraphs_facets() {
            let paragraph = |facets: Vec<(&str, i32)>| SearchResponse {
                paragraph: Some(ParagraphSearchResponse {
                    facets: proto_facets(facets),
                    ..Default::default()
                }),
                ..Default::default()
            };

            // `facets` are aggregated and counts are added together

            let merged = merge(
                vec![
                    paragraph(vec![("/l/label-A", 10), ("/l/label-B", 5)]),
                    paragraph(vec![("/l/label-A", 3), ("/l/label-C", 7), ("/e/table", 12)]),
                    paragraph(vec![("/e/chair", 3), ("/e/table", 20)]),
                ],
                OrderBy::default(),
                Limit(20),
            )
            .paragraph
            .unwrap();
            assert_eq!(merged.facets["/l"].facetresults.len(), 3);
            assert!(merged.facets["/l"].facetresults.contains(&FacetResult {
                tag: "/l/label-A".to_string(),
                total: 13
            }));
            assert!(merged.facets["/l"].facetresults.contains(&FacetResult {
                tag: "/l/label-B".to_string(),
                total: 5
            }));
            assert!(merged.facets["/l"].facetresults.contains(&FacetResult {
                tag: "/l/label-C".to_string(),
                total: 7
            }));
            assert_eq!(merged.facets["/e"].facetresults.len(), 2);
            assert!(merged.facets["/e"].facetresults.contains(&FacetResult {
                tag: "/e/chair".to_string(),
                total: 3
            }));
            assert!(merged.facets["/e"].facetresults.contains(&FacetResult {
                tag: "/e/table".to_string(),
                total: 32
            }));
        }

        #[test]
        fn test_merge_paragraphs_ematches() {
            let paragraph = |ematches: Vec<String>| SearchResponse {
                paragraph: Some(ParagraphSearchResponse {
                    ematches,
                    ..Default::default()
                }),
                ..Default::default()
            };

            // `ematches` are aggregated without repetition

            let merged = merge(
                vec![
                    paragraph(vec!["A".to_string(), "B".to_string(), "C".to_string()]),
                    paragraph(vec!["A".to_string(), "D".to_string(), "E".to_string()]),
                    paragraph(vec!["F".to_string(), "G".to_string(), "A".to_string()]),
                ],
                OrderBy::default(),
                Limit(20),
            )
            .paragraph
            .unwrap();
            assert_eq!(merged.ematches.len(), 7);
            assert!(merged.ematches.contains(&"A".to_string()));
            assert!(merged.ematches.contains(&"B".to_string()));
            assert!(merged.ematches.contains(&"C".to_string()));
            assert!(merged.ematches.contains(&"D".to_string()));
            assert!(merged.ematches.contains(&"E".to_string()));
            assert!(merged.ematches.contains(&"F".to_string()));
            assert!(merged.ematches.contains(&"G".to_string()));
        }

        #[test]
        fn test_merge_paragraphs_with_limit() {
            let shard = SearchResponse {
                paragraph: Some(ParagraphSearchResponse {
                    results: vec![
                        ParagraphResult {
                            sort_value: Some(paragraph_result::SortValue::Score(nidx_protos::ResultScore::default())),
                            ..Default::default()
                        };
                        20
                    ],
                    ..Default::default()
                }),
                ..Default::default()
            };

            let merged = merge(vec![shard.clone(), shard.clone()], OrderBy::default(), Limit(50))
                .paragraph
                .unwrap();
            assert_eq!(merged.results.len(), 40);

            let merged = merge(vec![shard.clone(), shard.clone()], OrderBy::default(), Limit(20))
                .paragraph
                .unwrap();
            assert_eq!(merged.results.len(), 20);
        }

        #[test]
        fn test_merge_paragraph_results_by_score() {
            let paragraph = |rid: &str, score: f32, booster: f32| ParagraphResult {
                uuid: rid.to_string(),
                field: "a/title".to_string(),
                sort_value: Some(paragraph_result::SortValue::Score(nidx_protos::ResultScore {
                    bm25: score,
                    booster,
                })),
                ..Default::default()
            };
            let response = |paragraphs: Vec<ParagraphResult>| SearchResponse {
                paragraph: Some(ParagraphSearchResponse {
                    total: 100,
                    results: paragraphs,
                    ..Default::default()
                }),
                ..Default::default()
            };

            let merged = merge(
                vec![
                    response(vec![paragraph("foo", 3.0, 1.0), paragraph("bar", 2.0, 2.0)]),
                    response(vec![paragraph("baz", 4.0, 1.0), paragraph("quux", 2.0, 1.0)]),
                ],
                OrderBy {
                    expr: SortExpr::Score,
                    descending: true,
                },
                Limit(20),
            )
            .paragraph
            .unwrap();
            assert_eq!(merged.results.len(), 4);
            assert_eq!(merged.results[0].uuid, "baz");
            assert_eq!(merged.results[1].uuid, "foo");
            assert_eq!(merged.results[2].uuid, "bar");
            assert_eq!(merged.results[3].uuid, "quux");
        }

        #[test]
        fn test_merge_paragraph_results_by_date() {
            let paragraph = |rid: &str, seconds: i64, nanos: i32| ParagraphResult {
                uuid: rid.to_string(),
                field: "a/title".to_string(),
                sort_value: Some(paragraph_result::SortValue::Date(Timestamp { seconds, nanos })),
                ..Default::default()
            };
            let response = |paragraphs: Vec<ParagraphResult>| SearchResponse {
                paragraph: Some(ParagraphSearchResponse {
                    total: 100,
                    results: paragraphs,
                    ..Default::default()
                }),
                ..Default::default()
            };

            let merged = merge(
                vec![
                    response(vec![paragraph("foo", 3, 1), paragraph("bar", 2, 2)]),
                    response(vec![paragraph("baz", 4, 1), paragraph("quux", 2, 1)]),
                ],
                OrderBy {
                    expr: SortExpr::Date,
                    descending: true,
                },
                Limit(20),
            )
            .paragraph
            .unwrap();
            assert_eq!(merged.results.len(), 4);
            assert_eq!(merged.results[0].uuid, "baz");
            assert_eq!(merged.results[1].uuid, "foo");
            assert_eq!(merged.results[2].uuid, "bar");
            assert_eq!(merged.results[3].uuid, "quux");

            let merged = merge(
                vec![
                    response(vec![paragraph("bar", 2, 2), paragraph("foo", 3, 1)]),
                    response(vec![paragraph("quux", 2, 1), paragraph("baz", 4, 1)]),
                ],
                OrderBy {
                    expr: SortExpr::Date,
                    descending: false,
                },
                Limit(20),
            )
            .paragraph
            .unwrap();
            assert_eq!(merged.results.len(), 4);
            assert_eq!(merged.results[0].uuid, "quux");
            assert_eq!(merged.results[1].uuid, "bar");
            assert_eq!(merged.results[2].uuid, "foo");
            assert_eq!(merged.results[3].uuid, "baz");
        }
    }

    mod vectors {
        use nidx_protos::{DocumentScored, DocumentVectorIdentifier, SearchResponse, VectorSearchResponse};

        use crate::searcher::shard_merge::{Limit, OrderBy};

        use super::merge;

        #[test]
        fn test_merge_vector_results() {
            let vector = |id: &str, score: f32| DocumentScored {
                doc_id: Some(DocumentVectorIdentifier { id: id.to_string() }),
                score,
                metadata: None,
                labels: vec![],
            };
            let response = |documents: Vec<DocumentScored>| SearchResponse {
                vector: Some(VectorSearchResponse { documents }),
                ..Default::default()
            };

            // merged results keep scoring order

            let shards = vec![
                response(vec![vector("a", 1.0), vector("b", 0.5)]),
                response(vec![vector("c", 3.2), vector("d", 0.9)]),
            ];

            let merged = merge(shards.clone(), OrderBy::default(), Limit(20)).vector.unwrap();
            assert_eq!(merged.documents[0].doc_id.as_ref().unwrap().id, "c");
            assert_eq!(merged.documents[1].doc_id.as_ref().unwrap().id, "a");
            assert_eq!(merged.documents[2].doc_id.as_ref().unwrap().id, "d");
            assert_eq!(merged.documents[3].doc_id.as_ref().unwrap().id, "b");

            let merged = merge(shards.clone(), OrderBy::default(), Limit(2)).vector.unwrap();
            assert_eq!(merged.documents[0].doc_id.as_ref().unwrap().id, "c");
            assert_eq!(merged.documents[1].doc_id.as_ref().unwrap().id, "a");
        }
    }

    mod graphs {
        use nidx_protos::{
            GraphSearchResponse, RelationNode, SearchResponse, graph_search_response, relation::RelationType,
            relation_node,
        };

        use crate::searcher::shard_merge::{Limit, OrderBy};

        use super::merge;

        #[test]
        fn test_merge_graph_results() {
            let cat = RelationNode {
                value: "cat".to_string(),
                ntype: relation_node::NodeType::Entity.into(),
                subtype: "animals".to_string(),
            };
            let dog = RelationNode {
                value: "dog".to_string(),
                ntype: relation_node::NodeType::Entity.into(),
                subtype: "animals".to_string(),
            };
            let table = RelationNode {
                value: "table".to_string(),
                ntype: relation_node::NodeType::Entity.into(),
                subtype: "objects".to_string(),
            };

            let love = graph_search_response::Relation {
                relation_type: RelationType::Entity.into(),
                label: "love".to_string(),
            };
            let jump = graph_search_response::Relation {
                relation_type: RelationType::Entity.into(),
                label: "jump".to_string(),
            };

            let path = |source, relation, destination| graph_search_response::Path {
                source,
                relation,
                destination,
                ..Default::default()
            };

            let merged = merge(
                vec![
                    SearchResponse {
                        graph: Some(GraphSearchResponse {
                            nodes: vec![cat.clone(), dog.clone()],
                            relations: vec![love.clone()],
                            graph: vec![
                                // cat -love-> dog
                                path(0, 0, 1),
                                // dog -love-> cat
                                path(1, 0, 1),
                            ],
                            scores: vec![0.5, 0.8],
                        }),
                        ..Default::default()
                    },
                    SearchResponse {
                        graph: Some(GraphSearchResponse {
                            nodes: vec![table, cat, dog],
                            relations: vec![jump, love],
                            graph: vec![
                                // cat -love-> dog
                                path(1, 1, 2),
                                // cat -love-> table
                                path(1, 1, 0),
                                // cat -jump-> table
                                path(1, 0, 0),
                            ],
                            scores: vec![0.3, 0.9, 0.7],
                        }),
                        ..Default::default()
                    },
                ],
                OrderBy::default(),
                Limit(20),
            )
            .graph
            .unwrap();

            assert_eq!(merged.nodes.len(), 2 + 3);
            assert_eq!(merged.nodes[0].value, "cat");
            assert_eq!(merged.nodes[0].ntype, relation_node::NodeType::Entity as i32);
            assert_eq!(merged.nodes[0].subtype, "animals");
            assert_eq!(merged.nodes[1].value, "dog");
            assert_eq!(merged.nodes[2].value, "table");
            // merging doesn't dedup nodes
            assert_eq!(merged.nodes[3].value, "cat");
            assert_eq!(merged.nodes[4].value, "dog");

            assert_eq!(merged.relations.len(), 1 + 2);
            assert_eq!(merged.relations[0].label, "love");
            assert_eq!(merged.relations[1].label, "jump");
            // nor relations
            assert_eq!(merged.relations[2].label, "love");

            assert_eq!(merged.graph.len(), 2 + 3);
            let path_indexes = |path: &graph_search_response::Path| (path.source, path.relation, path.destination);
            assert_eq!(path_indexes(&merged.graph[0]), (0, 0, 1));
            assert_eq!(path_indexes(&merged.graph[1]), (1, 0, 1));
            // merge has offset the node/relation index for this shard paths
            assert_eq!(path_indexes(&merged.graph[2]), (3, 2, 4));
            assert_eq!(path_indexes(&merged.graph[3]), (3, 2, 2));
            assert_eq!(path_indexes(&merged.graph[4]), (3, 1, 2));

            assert_eq!(merged.scores.len(), 2 + 3);
            assert_eq!(merged.scores, vec![0.5, 0.8, 0.3, 0.9, 0.7]);
        }
    }

    // Given a list of facets and counts, extract the root and mount the proto result.
    //
    // Example:
    // proto_facets(vec![("/l/label", 20)]) will produce {"/l": {tag: "/l/label", total: 20}}
    fn proto_facets(entries: Vec<(&str, i32)>) -> HashMap<String, FacetResults> {
        let mut tmp = HashMap::new();
        for (facet, count) in entries {
            let root = facet[..2].to_string();
            tmp.entry(root.to_string())
                .and_modify(|l: &mut Vec<(String, i32)>| l.push((facet.to_string(), count)))
                .or_insert(vec![(facet.to_string(), count)]);
        }

        let mut r = HashMap::new();
        for (root, facets) in tmp {
            r.insert(
                root,
                FacetResults {
                    facetresults: facets
                        .into_iter()
                        .map(|(tag, total)| FacetResult { tag, total })
                        .collect(),
                },
            );
        }
        r
    }
}
