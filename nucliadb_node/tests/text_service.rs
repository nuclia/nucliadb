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
// // Copyright (C) 2021 Bosutech XXI S.L.
// //
// // nucliadb is offered under the AGPL v3.0 and as commercial software.
// // For commercial licensing, contact us at info@nuclia.com.
// //
// // AGPL:
// // This program is free software: you can redistribute it and/or modify
// // it under the terms of the GNU Affero General Public License as
// // published by the Free Software Foundation, either version 3 of the
// // License, or (at your option) any later version.
// //
// // This program is distributed in the hope that it will be useful,
// // but WITHOUT ANY WARRANTY; without even the implied warranty of
// // MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// // GNU Affero General Public License for more details.
// //
// // You should have received a copy of the GNU Affero General Public License
// // along with this program. If not, see <http://www.gnu.org/licenses/>.
// //
// pub mod utils;
// use std::collections::{HashMap, HashSet};
// use std::time::{Duration, SystemTime};

// use log::trace;
// use nucliadb_protos::fdbwriter::op_status::Status;
// use nucliadb_protos::nodereader::node_reader_client::NodeReaderClient;
// use nucliadb_protos::nodereader::{
//     order_by, DocumentSearchRequest, Faceted, Filter, OrderBy, ParagraphSearchRequest,
// Timestamps, };
// use nucliadb_protos::noderesources::{self, Paragraph, Paragraphs};
// use nucliadb_protos::nodewriter::node_writer_client::NodeWriterClient;
// use prost_types::Timestamp;
// use tokio::join;
// use tonic::Request;
// use utils::server::test_with_server;

// use crate::utils::create_shard;

// fn create_vector(n_dim: usize, u: f32) -> Vec<f32> {
//     (0..n_dim).map(|_| u).collect()
// }

// fn create_resource1(shard_id: &str) -> noderesources::Resource {
//     let resource_id = noderesources::ResourceId {
//         shard_id: shard_id.to_string(),
//         uuid: "f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string(),
//     };

//     let now = SystemTime::now()
//         .duration_since(SystemTime::UNIX_EPOCH)
//         .unwrap();
//     let timestamp = Timestamp {
//         seconds: now.as_secs() as i64,
//         nanos: 0,
//     };

//     let metadata = noderesources::Metadata {
//         origin: "tests".to_string(),
//         slug: "tests-doc1".to_string(),
//         created: Some(timestamp.clone()),
//         modified: Some(timestamp),
//         icon: "undefined_icon".to_string(),
//     };

//     const DOC1_TI: &str = "This is the first document";
//     const DOC1_P1: &str = "This is the text of the second paragraph.";
//     const DOC1_P2: &str = "This should be enough to test the tantivy.";
//     const DOC1_P3: &str = "But I wanted to make it three anyway.";

//     let ti_title = noderesources::TextInformation {
//         text: DOC1_TI.to_string(),
//         labels: vec!["paragraph".to_string(), "games/sports/football".to_string()],
//     };

//     let ti_body = noderesources::TextInformation {
//         text: DOC1_P1.to_string() + DOC1_P2 + DOC1_P3,
//         labels: vec!["doc1".to_string()],
//     };

//     let mut texts = HashMap::new();
//     texts.insert("title".to_string(), ti_title);
//     texts.insert("body".to_string(), ti_body);

//     let vecs = vec![
//         create_vector(768, 1.0),
//         create_vector(768, 2.0),
//         create_vector(768, 3.0),
//     ];
//     let sentences: HashMap<String, Vec<u8>> = vecs
//         .into_iter()
//         .map(|v| {
//             (
//                 "f56c58ac-b4f9-4d61-a077-f0709d6b5d89/body/250c7835-1736-4776-afa0-08490c647cb0/\
//                  10-20"
//                     .to_string(),
//                 bincode::serialize(&v).unwrap(),
//             )
//         })
//         .collect();

//     let p1 = Paragraph {
//         start: 0,
//         end: DOC1_P1.len() as i32,
//         sentences: sentences.clone(),
//         field: "body".to_string(),
//         labels: vec!["nsfw".to_string()],
//     };

//     let p2 = Paragraph {
//         start: DOC1_P1.len() as i32,
//         end: (DOC1_P1.len() + DOC1_P2.len()) as i32,
//         sentences: sentences.clone(),
//         field: "body".to_string(),
//         labels: vec![
//             "tantivy".to_string(),
//             "test".to_string(),
//             "games/sports/basket".to_string(),
//         ],
//     };

//     let p3 = Paragraph {
//         start: (DOC1_P1.len() + DOC1_P2.len()) as i32,
//         end: (DOC1_P1.len() + DOC1_P2.len() + DOC1_P3.len()) as i32,
//         sentences: sentences.clone(),
//         field: "body".to_string(),
//         labels: vec![
//             "three".to_string(),
//             "games/esports/counter_strike".to_string(),
//         ],
//     };

//     let body_paragraphs = Paragraphs {
//         paragraphs: [
//             ("3a0e5267-a49e-49af-9b92-738e5a1b7126".to_string(), p1),
//             ("d5b20afb-a5d9-4193-aa2e-d56c83356bc0".to_string(), p2),
//             ("a5a21449-654f-4a95-8195-311f0486dcae".to_string(), p3),
//         ]
//         .into_iter()
//         .collect(),
//     };

//     let p4 = Paragraph {
//         start: 0,
//         end: DOC1_TI.len() as i32,
//         sentences,
//         field: "title".to_string(),
//         labels: vec!["cool".to_string()],
//     };

//     let title_paragraphs = Paragraphs {
//         paragraphs: [("f1b7ceff-a198-4e44-9866-315320b0ce14".to_string(), p4)]
//             .into_iter()
//             .collect(),
//     };

//     let paragraphs = [
//         ("body".to_string(), body_paragraphs),
//         ("title".to_string(), title_paragraphs),
//     ]
//     .into_iter()
//     .collect();

//     noderesources::Resource {
//         resource: Some(resource_id),
//         metadata: Some(metadata),
//         texts,
//         status: noderesources::resource::ResourceStatus::Useful as i32,
//         labels: vec!["doc1".to_string()],
//         paragraphs,
//         relations: Vec::new(),
//         paragraphs_to_delete: Vec::new(),
//         sentences_to_delete: Vec::new(),
//         relations_to_delete: Vec::new(),
//     }
// }

// fn create_resource2(shard_id: &str) -> noderesources::Resource {
//     let resource_id = noderesources::ResourceId {
//         shard_id: shard_id.to_string(),
//         uuid: "f56c58ac-b4f9-4d61-a077-ffccaadd0002".to_string(),
//     };

//     let now = SystemTime::now()
//         .duration_since(SystemTime::UNIX_EPOCH)
//         .unwrap();

//     let timestamp = Timestamp {
//         seconds: (now.as_secs() - 120) as i64,
//         nanos: 0,
//     };

//     let metadata = noderesources::Metadata {
//         origin: "tests".to_string(),
//         slug: "tests-doc2".to_string(),
//         created: Some(timestamp.clone()),
//         modified: Some(timestamp),
//         icon: "undefined_icon".to_string(),
//     };

//     const DOC1_TI: &str = "this";

//     let ti_title = noderesources::TextInformation {
//         text: DOC1_TI.to_string(),
//         labels: vec!["paragraph".to_string(), "games/sports/football".to_string()],
//     };

//     let mut texts = HashMap::new();
//     texts.insert("title".to_string(), ti_title);

//     let document_labels = vec!["doc2".to_string()];

//     let vecs = vec![create_vector(768, 1.0)];
//     let sentences: HashMap<String, Vec<u8>> = vecs
//         .into_iter()
//         .map(|v| {
//             (
//                 "f56c58ac-b4f9-4d61-a077-f0709d6b5d89/body/250c7835-1736-4776-afa0-08490c647cb0/\
//                  10-20"
//                     .to_string(),
//                 bincode::serialize(&v).unwrap(),
//             )
//         })
//         .collect();

//     let p_title = Paragraph {
//         start: 0,
//         end: DOC1_TI.len() as i32,
//         sentences,
//         field: "title".to_string(),
//         labels: vec!["cool".to_string()],
//     };

//     let title_paragraphs = Paragraphs {
//         paragraphs: [("f1b7ceff-a198-4e44-9866-315320b0ce14".to_string(), p_title)]
//             .into_iter()
//             .collect(),
//     };

//     let paragraphs = [("title".to_string(), title_paragraphs)]
//         .into_iter()
//         .collect();

//     noderesources::Resource {
//         resource: Some(resource_id),
//         metadata: Some(metadata),
//         texts,
//         status: noderesources::resource::ResourceStatus::Useful as i32,
//         labels: document_labels,
//         paragraphs,
//         relations: Vec::new(),
//         paragraphs_to_delete: Vec::new(),
//         sentences_to_delete: Vec::new(),
//         relations_to_delete: Vec::new(),
//     }
// }

// fn create_resource3(shard_id: &str) -> noderesources::Resource {
//     let resource_id = noderesources::ResourceId {
//         shard_id: shard_id.to_string(),
//         uuid: "f56c58ac-b4f9-4d61-a077-ffccaadd0003".to_string(),
//     };

//     let now = SystemTime::now()
//         .duration_since(SystemTime::UNIX_EPOCH)
//         .unwrap();

//     let timestamp = Timestamp {
//         seconds: (now.as_secs() - 60) as i64,
//         nanos: 0,
//     };

//     let metadata = noderesources::Metadata {
//         origin: "tests".to_string(),
//         slug: "tests-doc3".to_string(),
//         created: Some(timestamp.clone()),
//         modified: Some(timestamp),
//         icon: "undefined_icon".to_string(),
//     };

//     const DOC1_TI: &str = "this";

//     let ti_title = noderesources::TextInformation {
//         text: DOC1_TI.to_string(),
//         labels: vec!["title3".to_string()],
//     };

//     let mut texts = HashMap::new();
//     texts.insert("title".to_string(), ti_title);

//     let document_labels = vec!["doc3".to_string()];

//     let vecs = vec![create_vector(768, 1.0)];
//     let sentences: HashMap<String, Vec<u8>> = vecs
//         .into_iter()
//         .map(|v| {
//             (
//                 "f56c58ac-b4f9-4d61-a077-f0709d6b5d89/body/250c7835-1736-4776-afa0-08490c647cb0/\
//                  10-20"
//                     .to_string(),
//                 bincode::serialize(&v).unwrap(),
//             )
//         })
//         .collect();

//     let p_title = Paragraph {
//         start: 0,
//         end: DOC1_TI.len() as i32,
//         sentences,
//         field: "title".to_string(),
//         labels: vec!["cool".to_string()],
//     };

//     let title_paragraphs = Paragraphs {
//         paragraphs: [("f1b7ceff-a198-4e44-9866-315320b0ce14".to_string(), p_title)]
//             .into_iter()
//             .collect(),
//     };

//     let paragraphs = [("title".to_string(), title_paragraphs)]
//         .into_iter()
//         .collect();

//     noderesources::Resource {
//         resource: Some(resource_id),
//         metadata: Some(metadata),
//         texts,
//         status: noderesources::resource::ResourceStatus::Useful as i32,
//         labels: document_labels,
//         paragraphs,
//         relations: Vec::new(),
//         paragraphs_to_delete: Vec::new(),
//         sentences_to_delete: Vec::new(),
//         relations_to_delete: Vec::new(),
//     }
// }

// #[tokio::test]
// async fn usual_set_resource() {
//     test_with_server(async move {
//         let mut client = NodeWriterClient::connect("http://[::1]:10000")
//             .await
//             .expect("Error creating NodeWriter client");

//         let shard_id = create_shard().await;
//         println!("Created shard id: {}", shard_id);
//         let resource1 = create_resource1(&shard_id);

//         match client.set_resource(resource1).await {
//             Ok(response) => {
//                 assert_eq!(response.get_ref().status as u64, Status::Ok as u64);
//             }
//             Err(e) => panic!("Resource not set succesfully: {}", e),
//         };
//     })
//     .await;
// }

// #[tokio::test]
// async fn set_resource_shard_doesnt_exists() {
//     test_with_server(async move {
//         let mut client = NodeWriterClient::connect("http://[::1]:10000")
//             .await
//             .expect("Error creating NodeWriter client");

//         let resource1 = create_resource1("aaa");

//         let failed = client.set_resource(resource1).await.is_err();

//         assert!(failed);
//     })
//     .await;
// }

// #[tokio::test]
// async fn writing_reading_simultaneosly() {
//     let server_output = test_with_server(async move {
//         let mut writer = NodeWriterClient::connect("http://[::1]:10000")
//             .await
//             .expect("Error creating NodeWriter client");

//         let writer_response = writer
//             .new_shard(Request::new(noderesources::EmptyQuery {}))
//             .await
//             .expect("Error in new_shard request");
//         let shard_id = writer_response.get_ref().id.clone();

//         let resource1 = create_resource1(&shard_id);

//         let writer_task = tokio::spawn(async move {
//             for _ in 1..10 {
//                 let success = writer.set_resource(resource1.clone()).await.is_ok();
//                 assert!(success);
//             }
//         });

//         // Client -------------------------------------------------------------

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let doc_req = DocumentSearchRequest {
//             id: shard_id,
//             uuid: String::new(),
//             body: String::from("first"),
//             filter: None,
//             order: None,
//             faceted: None,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//         };

//         let client_task = tokio::spawn(async move {
//             for _ in 1..10 {
//                 let client_response = client
//                     .document_search(doc_req.clone())
//                     .await
//                     .expect("Document search request failed.");

//                 let results = client_response.get_ref();
//                 assert_eq!(results.total, 1);
//             }
//         });

//         let _ = join!(writer_task, client_task);
//     })
//     .await;

//     println!("{}", server_output);
// }

// // Reader tests ---------------------------------------------------------------

// async fn write_document_test_set(shard_id: &str) {
//     let mut client = NodeWriterClient::connect("http://[::1]:10000")
//         .await
//         .expect("Error creating NodeWriter client");

//     let resource1 = create_resource1(shard_id);

//     match client.set_resource(resource1).await {
//         Ok(response) => {
//             assert_eq!(response.get_ref().status as u64, Status::Ok as u64);
//         }
//         Err(e) => panic!("Resource not set succesfully: {}", e),
//     };
// }

// async fn write_document_test_set2(shard_id: &str) {
//     let mut client = NodeWriterClient::connect("http://[::1]:10000")
//         .await
//         .expect("Error creating NodeWriter client");

//     let resources = vec![
//         create_resource1(shard_id),
//         create_resource2(shard_id),
//         create_resource3(shard_id),
//     ];

//     for resource in resources {
//         match client.set_resource(resource).await {
//             Ok(response) => {
//                 assert_eq!(response.get_ref().status as u64, Status::Ok as u64);
//             }
//             Err(e) => panic!("Resource not set succesfully: {}", e),
//         };
//     }
// }

// #[tokio::test]
// async fn document_text_query_single_word() {
//     let server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set(&shard_id).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let doc_req = DocumentSearchRequest {
//             id: shard_id,
//             uuid: String::new(),
//             body: String::from("first"),
//             filter: None,
//             order: None,
//             faceted: None,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//         };

//         let client_response = client
//             .document_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let results = client_response.get_ref();
//         trace!("results: {:?}", results);
//         assert_eq!(results.total, 1);
//     })
//     .await;

//     println!("{}", server_output);
// }

// #[tokio::test]
// async fn document_text_query_contiguous_word() {
//     let server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set(&shard_id).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let doc_req = DocumentSearchRequest {
//             id: shard_id,
//             uuid: String::new(),
//             body: String::from("first document"),
//             filter: None,
//             order: None,
//             faceted: None,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//         };

//         let client_response = client
//             .document_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let results = client_response.get_ref();
//         assert_eq!(results.total, 1);
//     })
//     .await;

//     println!("{}", server_output);
// }

// #[tokio::test]
// async fn document_text_query_non_contiguous_word() {
//     let server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set(&shard_id).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let doc_req = DocumentSearchRequest {
//             id: shard_id,
//             uuid: String::new(),
//             body: String::from("this enough anyway"),
//             filter: None,
//             order: None,
//             faceted: None,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//         };

//         let client_response = client
//             .document_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let results = client_response.get_ref();
//         assert_eq!(results.total, 1);
//     })
//     .await;

//     println!("{}", server_output);
// }

// #[tokio::test]
// async fn document_simple_filter() {
//     let server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set(&shard_id).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let mut fields = HashMap::new();

//         fields.insert("title".to_string(), "first".to_string());

//         let doc_req = DocumentSearchRequest {
//             id: shard_id.clone(),
//             uuid: String::new(),
//             body: String::from("this enough anyway"),
//             filter: Some(Filter {
//                 tags: vec!["nonexistingword".to_string()],
//             }),
//             order: None,
//             faceted: None,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//         };

//         let client_response = client
//             .document_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let results = client_response.get_ref();
//         assert_eq!(results.total, 0);

//         let doc_req = DocumentSearchRequest {
//             id: shard_id,
//             uuid: String::new(),
//             body: String::from("this enough anyway"),
//             filter: Some(Filter {
//                 tags: vec!["doc1".to_string()],
//             }),
//             order: None,
//             faceted: None,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//         };

//         let client_response = client
//             .document_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let results = client_response.get_ref();
//         assert_eq!(results.total, 1);
//     })
//     .await;

//     println!("{}", server_output);
// }

// #[tokio::test]
// async fn document_faceted() {
//     let server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set(&shard_id).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let mut fields = HashMap::new();

//         fields.insert("title".to_string(), "first".to_string());

//         let doc_req = DocumentSearchRequest {
//             id: shard_id.clone(),
//             uuid: String::new(),
//             body: String::from("this enough anyway"),
//             filter: None,
//             order: None,
//             faceted: Some(Faceted {
//                 tags: vec!["/l".to_string()],
//             }),
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//         };

//         let client_response = client
//             .document_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let response = client_response.get_ref();
//         let mut ff = HashMap::new();

//         for (facet, results) in &response.facets {
//             println!("Facet {}:\n", facet);
//             for facet_result in &results.facetresults {
//                 println!("\t{} - {}", facet_result.tag, facet_result.total);
//                 ff.insert(facet_result.tag.clone(), facet_result.total);
//             }
//         }

//         assert_eq!(*ff.get("/l/doc1").unwrap(), 1);
//     })
//     .await;

//     println!("{}", server_output);
// }

// #[tokio::test]
// async fn paragraph_text_query_title() {
//     let server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set(&shard_id).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let mut fields = HashMap::new();

//         fields.insert("title".to_string(), "first".to_string());

//         let doc_req = ParagraphSearchRequest {
//             id: shard_id,
//             uuid: String::new(),
//             faceted_document: None,
//             faceted_paragraph: None,
//             filter_document: None,
//             filter_paragraph: None,
//             order: None,
//             fields,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//             body: String::new(),
//         };

//         let client_response = client
//             .paragraph_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let results = client_response.get_ref();
//         for r in &results.results {
//             println!("{:?}", r)
//         }

//         // assert_eq!(results.total, 1);
//     })
//     .await;

//     println!("{}", server_output);
// }

// #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
// pub struct TestParagraphSearchResponse {
//     uuid: String,
//     slug: String,
//     start_pos: u64,
//     end_pos: u64,
//     field: String,
// }

// #[tokio::test]
// async fn paragraph_simple_filter() {
//     let server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set(&shard_id).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let mut fields = HashMap::new();
//         fields.insert("title".to_string(), "first".to_string());

//         let doc_req = ParagraphSearchRequest {
//             id: shard_id.clone(),
//             uuid: String::new(),
//             faceted_document: None,
//             faceted_paragraph: None,
//             filter_document: None,
//             filter_paragraph: Some(Filter {
//                 tags: vec!["three".to_string()],
//             }),
//             order: None,
//             fields: fields.clone(),
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//             body: String::new(),
//         };

//         let client_response = client
//             .paragraph_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let results = client_response.get_ref();
//         for r in &results.results {
//             println!("{:?}", r)
//         }

//         let result_set: HashSet<_> = results
//             .results
//             .iter()
//             .map(|r| TestParagraphSearchResponse {
//                 uuid: r.uuid.clone(),
//                 slug: r.slug.clone(),
//                 start_pos: r.start_pos,
//                 end_pos: r.end_pos,
//                 field: r.field.clone(),
//             })
//             .collect();

//         assert_eq!(result_set.len(), 1);
//     })
//     .await;

//     println!("{}", server_output);
// }

// #[tokio::test]
// async fn paragraph_filter_title() {
//     let server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set(&shard_id).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let mut fields = HashMap::new();
//         fields.insert("title".to_string(), "first".to_string());

//         let doc_req = ParagraphSearchRequest {
//             id: shard_id.clone(),
//             uuid: String::new(),
//             faceted_document: None,
//             faceted_paragraph: None,
//             filter_document: None,
//             filter_paragraph: Some(Filter {
//                 tags: vec!["cool".to_string()],
//             }),
//             order: None,
//             fields: fields.clone(),
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//             body: String::new(),
//         };

//         let client_response = client
//             .paragraph_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let paragraph_search_response = client_response.get_ref();

//         assert!(!paragraph_search_response.results.is_empty());
//     })
//     .await;

//     println!("{}", server_output);
// }

// #[tokio::test]
// async fn paragraph_filter_doesnt_exist() {
//     let server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set(&shard_id).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let mut fields = HashMap::new();
//         fields.insert("title".to_string(), "first".to_string());

//         let doc_req = ParagraphSearchRequest {
//             id: shard_id,
//             uuid: String::new(),
//             faceted_document: None,
//             faceted_paragraph: None,
//             filter_document: None,
//             filter_paragraph: Some(Filter {
//                 tags: vec!["nonexistingword".to_string()],
//             }),
//             order: None,
//             fields,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//             body: String::new(),
//         };

//         let client_response = client
//             .paragraph_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let results = client_response.get_ref();
//         for r in &results.results {
//             println!("{:?}", r)
//         }

//         assert_eq!(results.total, 0);
//     })
//     .await;

//     println!("{}", server_output);
// }

// #[tokio::test]
// async fn paragraph_faceted() {
//     let _server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set(&shard_id).await;

//         // Time to tantivy commit
//         tokio::time::sleep(Duration::from_millis(200)).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let mut fields = HashMap::new();
//         fields.insert("title".to_string(), "first".to_string());

//         let doc_req = ParagraphSearchRequest {
//             id: shard_id.clone(),
//             uuid: String::new(),
//             faceted_document: None,
//             faceted_paragraph: Some(Faceted {
//                 tags: vec![
//                     "/l/games/esports".to_string(),
//                     "/l/games/sports".to_string(),
//                 ],
//             }),
//             filter_document: None,
//             filter_paragraph: None,
//             order: None,
//             fields: fields.clone(),
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//             body: String::new(),
//         };

//         let client_response = client
//             .paragraph_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let response = client_response.get_ref();

//         let mut ff = HashMap::new();

//         for (facet, results) in &response.facets {
//             println!("Facet {}:\n", facet);
//             for facet_result in &results.facetresults {
//                 println!("\t{} - {}", facet_result.tag, facet_result.total);
//                 ff.insert(facet_result.tag.clone(), facet_result.total);
//             }
//         }

//         assert!(ff.contains_key("/l/games/esports/counter_strike"));
//         // assert!(ff.contains_key("/l/games/sports/football"));
//         assert!(ff.contains_key("/l/games/sports/basket"));

//         assert_eq!(ff.len(), 2);
//     })
//     .await;

//     // println!("{}", server_output);
// }

// #[tokio::test]
// async fn order_by_created_modified_desc() {
//     let server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set2(&shard_id).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let doc_req = DocumentSearchRequest {
//             id: shard_id.clone(),
//             uuid: String::new(),
//             body: String::from("this"),
//             filter: None,
//             order: Some(OrderBy {
//                 field: "created".to_string(),
//                 r#type: order_by::OrderType::Desc as i32,
//             }),
//             faceted: None,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//         };

//         let client_response = client
//             .document_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let mut id_docs = Vec::new();
//         let results = client_response.get_ref();
//         for r in &results.results {
//             let id = r.uuid.clone().chars().last().unwrap();
//             id_docs.push(id);

//             println!("{:?}", r)
//         }

//         assert_eq!(id_docs, vec!['1', '3', '2']);

//         // ---------------------------- MODIFIED ------------------------------
//         println!("Modified");

//         let doc_req = DocumentSearchRequest {
//             id: shard_id,
//             uuid: String::new(),
//             body: String::from("this"),
//             filter: None,
//             order: Some(OrderBy {
//                 field: "modified".to_string(),
//                 r#type: order_by::OrderType::Desc as i32,
//             }),
//             faceted: None,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//         };

//         let client_response = client
//             .document_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let mut id_docs = Vec::new();
//         let results = client_response.get_ref();
//         for r in &results.results {
//             let id = r.uuid.clone().chars().last().unwrap();
//             id_docs.push(id);

//             println!("{:?}", r)
//         }

//         assert_eq!(id_docs, vec!['1', '3', '2']);
//     })
//     .await;

//     println!("{}", server_output);
// }

// #[tokio::test]
// async fn order_by_created_modified_asc() {
//     let server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set2(&shard_id).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let doc_req = DocumentSearchRequest {
//             id: shard_id.clone(),
//             uuid: String::new(),
//             body: String::from("this"),
//             filter: None,
//             order: Some(OrderBy {
//                 field: "created".to_string(),
//                 r#type: order_by::OrderType::Asc as i32,
//             }),
//             faceted: None,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//         };

//         let client_response = client
//             .document_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let mut id_docs = Vec::new();
//         let results = client_response.get_ref();
//         for r in &results.results {
//             let id = r.uuid.clone().chars().last().unwrap();
//             id_docs.push(id);

//             println!("{:?}", r)
//         }

//         assert_eq!(id_docs, vec!['2', '3', '1']);

//         // ---------------------------- MODIFIED ------------------------------
//         println!("Modified");

//         let doc_req = DocumentSearchRequest {
//             id: shard_id,
//             uuid: String::new(),
//             body: String::from("this"),
//             filter: None,
//             order: Some(OrderBy {
//                 field: "modified".to_string(),
//                 r#type: order_by::OrderType::Asc as i32,
//             }),
//             faceted: None,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: None,
//         };

//         let client_response = client
//             .document_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let mut id_docs = Vec::new();
//         let results = client_response.get_ref();
//         for r in &results.results {
//             let id = r.uuid.clone().chars().last().unwrap();
//             id_docs.push(id);

//             println!("{:?}", r)
//         }

//         assert_eq!(id_docs, vec!['2', '3', '1']);
//     })
//     .await;

//     println!("{}", server_output);
// }

// #[tokio::test]
// async fn timestamp_filtering() {
//     let server_output = test_with_server(async move {
//         let shard_id = create_shard().await;
//         write_document_test_set2(&shard_id).await;

//         let mut client = NodeReaderClient::connect("http://[::1]:10001")
//             .await
//             .expect("Error creating NodeReader client");

//         let mut to_created = Timestamp::from(SystemTime::now());
//         to_created.seconds += 2;

//         let mut from_created = Timestamp::from(SystemTime::now());
//         from_created.seconds -= 2;

//         let timestamps = Timestamps {
//             from_modified: Some(from_created.clone()),
//             to_modified: Some(to_created.clone()),
//             from_created: Some(from_created.clone()),
//             to_created: Some(to_created.clone()),
//         };

//         let doc_req = DocumentSearchRequest {
//             id: shard_id.clone(),
//             uuid: String::new(),
//             body: String::from("this"),
//             filter: None,
//             order: Some(OrderBy {
//                 field: "created".to_string(),
//                 r#type: order_by::OrderType::Desc as i32,
//             }),
//             faceted: None,
//             page_number: 0,
//             result_per_page: 100,
//             timestamps: Some(timestamps),
//         };

//         let client_response = client
//             .document_search(doc_req.clone())
//             .await
//             .expect("Document search request failed.");

//         let results = client_response.get_ref();

//         assert_eq!(results.total, 1);
//     })
//     .await;

//     println!("{}", server_output);
// }

// #[cfg(test)]
// mod tests {
//     use super::reader::NodeReaderService;
//     use super::writer::NodeWriterService;
//     use nucliadb_protos::*;
//     use prost::Message;
//     use std::io::Cursor;
//     #[tokio::test]
//     async fn payload() -> anyhow::Result<()> {
//         let mut writer = NodeWriterService::new();
//         let mut reader = NodeReaderService::new();

//         let resources_dir = std::path::Path::new("/Users/hermegarcia/RustWorkspace/data");
//         let new_shard = writer.new_shard().await;
//         let shard_id = ShardId {
//             id: new_shard.id.clone(),
//         };
//         assert!(resources_dir.exists());
//         for file_path in std::fs::read_dir(&resources_dir).unwrap() {
//             let file_path = file_path.unwrap().path();
//             println!("processing {file_path:?}");
//             let content = std::fs::read(&file_path).unwrap();
//             let resource = Resource::decode(&mut Cursor::new(content)).unwrap();
//             println!("Adding resource {}", file_path.display());
//             let res = writer.set_resource(&shard_id, &resource).await.unwrap();
//             assert!(res.is_ok());
//             println!("Resource added: {}", res.unwrap());
//             let info = reader.get_shard(&shard_id).await.unwrap().get_info().await;
//             println!("Sentences {}", info.sentences);
//             println!("Paragraphs {}", info.paragraphs);
//             println!("resources {}", info.resources);
//         }

//         Ok(())
//     }
// }
