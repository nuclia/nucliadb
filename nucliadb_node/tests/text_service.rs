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
//     async fn set_vector_resources() -> anyhow::Result<()> {
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
//             let content = std::fs::read(&file_path).unwrap();
//             let resource = Resource::decode(&mut Cursor::new(content)).unwrap();
//             println!("Adding resource {}", file_path.display());
//             let res = writer.set_resource(&shard_id, &resource).await.unwrap();
//             assert!(res.is_ok());
//             println!("Resource added: {}", res.unwrap());
//             let info = reader.get_shard(&shard_id).await.unwrap().get_info().await;
//             println!("Sentences {}", info.sentences);
//             let search_request =  VectorSearchRequest {
//                 id: shard_id.id.clone(),
//                 vector: test_query(),
//                 tags: vec![],
//                 reload: true,
//             };

//             let search = reader.vector_search(&shard_id, search_request).await.unwrap().unwrap();
//             for result in search.documents {
//                 println!("{:?} {}", result.doc_id, result.score);
//             }
//         }

//         Ok(())
//     }

//     fn test_query() -> Vec<f32> {
//         vec![
//             -0.049436528235673904,
//             -0.38773781061172485,
//             -0.17886818945407867,
//             -0.030454277992248535,
//             0.03428860753774643,
//             0.08204551786184311,
//             0.00460650073364377,
//             -0.32330188155174255,
//             -0.43998071551322937,
//             -0.1351880431175232,
//             0.12360755354166031,
//             0.27885156869888306,
//             -0.15925291180610657,
//             0.4383751451969147,
//             0.26881900429725647,
//             0.0019903997890651226,
//             0.16406004130840302,
//             -0.32693392038345337,
//             0.03869317099452019,
//             -0.5116795897483826,
//             0.0022286081220954657,
//             0.25140953063964844,
//             0.09547901898622513,
//             0.1958846002817154,
//             -0.018519768491387367,
//             0.2658175528049469,
//             0.1525150090456009,
//             -0.10008413344621658,
//             0.0629483237862587,
//             -0.0019531813450157642,
//             0.24457499384880066,
//             -0.0980241671204567,
//             0.14688469469547272,
//             -0.010260501876473427,
//             0.23916588723659515,
//             0.1790442168712616,
//             -0.1393522322177887,
//             0.6105256676673889,
//             0.250579297542572,
//             0.10159068554639816,
//             -0.13784855604171753,
//             -0.13465233147144318,
//             -0.0805286392569542,
//             0.2171802967786789,
//             0.1141238734126091,
//             0.19120033085346222,
//             -0.012630823068320751,
//             0.24767781794071198,
//             -0.019485019147396088,
//             0.06095578148961067,
//             -0.44299325346946716,
//             -0.13624796271324158,
//             0.5995507836341858,
//             -0.12623687088489532,
//             -0.11214582622051239,
//             -0.6688718795776367,
//             0.1554756462574005,
//             0.4304656982421875,
//             -0.17787300050258636,
//             -0.13858692348003387,
//             0.09385953843593597,
//             -0.4401669502258301,
//             -0.16733253002166748,
//             0.3344966769218445,
//             -0.09421183168888092,
//             -0.31798794865608215,
//             0.2230391800403595,
//             0.20991800725460052,
//             0.6992281079292297,
//             -0.6523186564445496,
//             0.28679484128952026,
//             -0.011288371868431568,
//             -0.09882792085409164,
//             -0.17807520925998688,
//             -0.30326491594314575,
//             -0.578490674495697,
//             0.5809647440910339,
//             -0.4489562213420868,
//             0.1984896957874298,
//             0.27626833319664,
//             0.2845590114593506,
//             -0.04386547952890396,
//             -0.1738799512386322,
//             -0.5188499093055725,
//             0.5538201928138733,
//             0.018621964380145073,
//             0.13483719527721405,
//             -0.20313096046447754,
//             -0.24541331827640533,
//             -0.10394998639822006,
//             -0.3997305631637573,
//             0.1862330138683319,
//             0.19679604470729828,
//             0.22552944719791412,
//             -0.08086320757865906,
//             -0.08899282664060593,
//             0.10238436609506607,
//             -0.15050062537193298,
//             0.15615925192832947,
//             0.2849355936050415,
//             -0.24053001403808594,
//             -0.3372158408164978,
//             -0.1327262967824936,
//             -0.2286124974489212,
//             -0.11610658466815948,
//             -0.5355678200721741,
//             0.08206815272569656,
//             0.08869659900665283,
//             -0.3598068058490753,
//             -0.24762186408042908,
//             -0.3360268473625183,
//             -0.14028261601924896,
//             0.2521595358848572,
//             -0.0446639358997345,
//             0.059339188039302826,
//             -0.3689529597759247,
//             -0.30733487010002136,
//             0.22518007457256317,
//             0.2457120716571808,
//             0.4156748950481415,
//             0.08904077857732773,
//             0.3381456434726715,
//             0.3604297339916229,
//             0.13508419692516327,
//             0.1706492304801941,
//             0.011938470415771008,
//             -0.025581369176506996,
//             -0.309602290391922,
//             -0.08795492351055145,
//             0.01606586202979088,
//             -0.09048226475715637,
//             -0.10575157403945923,
//             -0.35361987352371216,
//             -0.006319573149085045,
//             0.25495919585227966,
//             -0.2458299845457077,
//             0.11361665278673172,
//             0.23901110887527466,
//             0.4505068063735962,
//             -0.32300758361816406,
//             -0.5639845132827759,
//             0.44565051794052124,
//             0.08725068718194962,
//             0.2174830585718155,
//             -0.07259310781955719,
//             -0.3823598027229309,
//             -0.06421837210655212,
//             0.1838890016078949,
//             0.12656614184379578,
//             -0.31386858224868774,
//             -0.2734704911708832,
//             0.2270970195531845,
//             0.1658799648284912,
//             -0.19577381014823914,
//             -0.021605152636766434,
//             0.25203385949134827,
//             0.10264624655246735,
//             -0.2691315710544586,
//             -0.4862293004989624,
//             0.15219828486442566,
//             -0.21772626042366028,
//             0.1554374098777771,
//             0.07202572375535965,
//             -0.21364626288414001,
//             0.049713846296072006,
//             -0.5127191543579102,
//             -0.2590067684650421,
//             -0.1841699182987213,
//             0.004103204235434532,
//             0.07880894839763641,
//             -0.23435911536216736,
//             -0.13028721511363983,
//             -0.68212890625,
//             -0.17920593917369843,
//             -0.174444317817688,
//             0.3365701735019684,
//             0.16479165852069855,
//             -0.16236120462417603,
//             -0.7578742504119873,
//             0.49745720624923706,
//             -0.09300146996974945,
//             0.07398019731044769,
//             -0.4549466371536255,
//             0.296966552734375,
//             -0.3773641884326935,
//             -0.034969761967659,
//             0.080865778028965,
//             0.7448330521583557,
//             0.25245410203933716,
//             0.4852564036846161,
//             -0.00876135379076004,
//             0.08967026323080063,
//             0.3614090383052826,
//             0.043571218848228455,
//             -0.14469686150550842,
//             0.28157034516334534,
//             0.17199921607971191,
//             0.5930328369140625,
//             0.11165208369493484,
//             -0.2646782100200653,
//             0.018568113446235657,
//             -0.26000604033470154,
//             -0.5807605981826782,
//             -0.21353507041931152,
//             -0.4490993618965149,
//             0.4332466125488281,
//             0.2797005772590637,
//             0.09116798639297485,
//             -0.567959189414978,
//             0.06619594246149063,
//             -0.20500175654888153,
//             -0.38998252153396606,
//             0.2732236087322235,
//             -0.2114010453224182,
//             0.36167123913764954,
//             0.27044668793678284,
//             0.18551288545131683,
//             -0.12323587387800217,
//             0.4773629307746887,
//             -0.5148422718048096,
//             0.35399922728538513,
//             -0.45374006032943726,
//             0.42976585030555725,
//             0.06607314944267273,
//             -0.02729417197406292,
//             -0.6509692668914795,
//             0.24287532269954681,
//             -0.37327349185943604,
//             0.07573889940977097,
//             -0.20536603033542633,
//             -0.13849538564682007,
//             0.23021870851516724,
//             -0.48414093255996704,
//             -0.08732166886329651,
//             0.4016159176826477,
//             0.12183058261871338,
//             0.28519728779792786,
//             -0.06350144743919373,
//             0.08115038275718689,
//             0.34129178524017334,
//             0.41991764307022095,
//             0.12879091501235962,
//             0.3557779788970947,
//             -0.0932074710726738,
//             -0.030864380300045013,
//             0.16069073975086212,
//             -0.09889120608568192,
//             -0.4073399007320404,
//             -0.1446319967508316,
//             0.26246076822280884,
//             -0.4748058617115021,
//             0.20600517094135284,
//             -0.233429953455925,
//             0.4283764660358429,
//             0.07801015675067902,
//             0.037180203944444656,
//             -0.11682373285293579,
//             -0.22701458632946014,
//             0.10011861473321915,
//             -0.17560897767543793,
//             0.3573344349861145,
//             -0.6046084761619568,
//             -0.502376139163971,
//             -0.525030255317688,
//             0.09479472041130066,
//             0.111101433634758,
//             -0.39456671476364136,
//             -0.2702665627002716,
//             -0.22005508840084076,
//             0.10988707095384598,
//             -0.12681952118873596,
//             -0.3064597249031067,
//             0.23101140558719635,
//             -0.5275964736938477,
//             -0.4076906442642212,
//             0.09570050984621048,
//             0.3717226982116699,
//             0.19928202033042908,
//             0.5930047631263733,
//             -0.03132243454456329,
//             -0.5660167336463928,
//             0.06252362579107285,
//             0.7250173687934875,
//             -0.1338401436805725,
//             0.5437468886375427,
//             0.13728897273540497,
//             0.23700635135173798,
//             -0.3094630837440491,
//             -0.052303019911050797,
//             -0.2962019443511963,
//             0.36861860752105713,
//             0.3948136568069458,
//             0.12381923198699951,
//             -0.3547701835632324,
//             0.2300267070531845,
//             -0.2463197112083435,
//             -0.345219224691391,
//             0.4898262917995453,
//             -0.4599078595638275,
//             0.3892558515071869,
//             -0.03907664120197296,
//             -0.08864646404981613,
//             -0.1967056393623352,
//             -0.011977250687777996,
//             0.32613223791122437,
//             -0.020049618557095528,
//             0.026610760018229485,
//             -0.1636645793914795,
//             -5.716254711151123,
//             0.19251449406147003,
//             0.23980291187763214,
//             0.34756898880004883,
//             -0.3078044652938843,
//             0.11141901463270187,
//             0.8823001980781555,
//             0.18608145415782928,
//             0.00910898670554161,
//             -0.02077992632985115,
//             -0.10536777228116989,
//             0.5477386116981506,
//             -0.2840781509876251,
//             0.417019784450531,
//             -0.521747887134552,
//             0.36249443888664246,
//             0.09914589673280716,
//             0.22736455500125885,
//             -0.48425909876823425,
//             0.46553847193717957,
//             0.10885564982891083,
//             -0.0037583583034574986,
//             -0.2961176931858063,
//             0.12717977166175842,
//             0.15875740349292755,
//             -0.3959377408027649,
//             -0.11721328645944595,
//             0.06114070117473602,
//             -0.36424702405929565,
//             0.09278829395771027,
//             -0.051568567752838135,
//             -0.1288258135318756,
//             0.1331946849822998,
//             0.1843765676021576,
//             0.012933510355651379,
//             0.11936532706022263,
//             0.2224583774805069,
//             0.12445474416017532,
//             -0.4293934404850006,
//             -0.1772022843360901,
//             -0.34719690680503845,
//             -0.27334675192832947,
//             0.04706209897994995,
//             0.021969180554151535,
//             -0.4825786054134369,
//             -0.2570037245750427,
//             -0.19236037135124207,
//             0.4506286382675171,
//             0.3450096547603607,
//             -0.04155081510543823,
//             -0.10981693863868713,
//             -0.006426787003874779,
//             -0.06813599169254303,
//             0.1722959578037262,
//             -0.22717435657978058,
//             -0.29070940613746643,
//             -0.3182806372642517,
//             0.09870679676532745,
//             0.3230013847351074,
//             -0.6360112428665161,
//             -0.10012061893939972,
//             0.10849897563457489,
//             -0.09890912473201752,
//             -0.16783665120601654,
//             -0.20943787693977356,
//             0.31079748272895813,
//             -0.470274955034256,
//             -0.02042379230260849,
//             0.26563748717308044,
//             0.4485057592391968,
//             0.3813225030899048,
//             0.13989414274692535,
//             -0.024369487538933754,
//             -0.8566377758979797,
//             -0.08300749957561493,
//             -0.4029175043106079,
//             -0.342336505651474,
//             -0.014371177181601524,
//             -0.012148032896220684,
//             0.5253869295120239,
//             -0.24389244616031647,
//             -0.02467428706586361,
//             0.4638451039791107,
//             0.5695465207099915,
//             0.24144138395786285,
//             0.08993692696094513,
//             0.20856329798698425,
//             0.22163115441799164,
//             -0.4619365334510803,
//             0.42840108275413513,
//             0.16339506208896637,
//             -0.19289053976535797,
//             -0.32422807812690735,
//             0.034145403653383255,
//             0.24791347980499268,
//             0.7121299505233765,
//             -0.2754915952682495,
//             -0.6760615706443787,
//             -0.4808255434036255,
//             -0.23129557073116302,
//             0.13823452591896057,
//             0.34669944643974304,
//             -0.08901739865541458,
//             0.07210907340049744,
//             0.35648444294929504,
//             0.0696052610874176,
//             -0.40490788221359253,
//             0.09655950218439102,
//             0.43375271558761597,
//             -0.10788306593894958,
//             -0.07252040505409241,
//             0.00022439246822614223,
//             0.24823789298534393,
//             -0.28870221972465515,
//             0.30475032329559326,
//             0.23793266713619232,
//             0.0859498381614685,
//             -0.4090288579463959,
//             0.15691961348056793,
//             0.009289995767176151,
//             -0.4442693889141083,
//             0.24417074024677277,
//             -0.05822782218456268,
//             0.4059525430202484,
//             -0.13762830197811127,
//             0.1645907163619995,
//             0.08231846243143082,
//             0.3299006223678589,
//             0.07739822566509247,
//             0.4629577398300171,
//             -0.2813040018081665,
//             -0.2910403311252594,
//             -0.2264331728219986,
//             -0.13478779792785645,
//             -0.09772948175668716,
//             0.2258758693933487,
//             -0.6240135431289673,
//             0.154217928647995,
//             0.2481304109096527,
//             0.012511911801993847,
//             -0.28912413120269775,
//             -0.2280394434928894,
//             0.2779674828052521,
//             -0.37714970111846924,
//             0.20981484651565552,
//             -0.17441023886203766,
//             0.6744823455810547,
//             0.18639424443244934,
//             0.18340133130550385,
//             -0.21885059773921967,
//             0.2988523244857788,
//             0.2829044759273529,
//             -0.19084160029888153,
//             -0.04282468557357788,
//             -0.5452829599380493,
//             -0.33046954870224,
//             0.3887947201728821,
//             -0.633573055267334,
//             0.29284387826919556,
//             0.40887272357940674,
//             -0.3082875609397888,
//             -0.46144190430641174,
//             -0.31140267848968506,
//             -0.5235274434089661,
//             0.12941813468933105,
//             -0.02324468083679676,
//             -0.2056846022605896,
//             -0.09471021592617035,
//             0.47335049510002136,
//             -0.3695177435874939,
//             -0.13233879208564758,
//             -0.3960825800895691,
//             -0.06261484324932098,
//             0.06069153547286987,
//             0.12384168803691864,
//             -0.2924750745296478,
//             0.20235279202461243,
//             0.02608354017138481,
//             -0.5945660471916199,
//             -0.00293124676682055,
//             -0.24478469789028168,
//             0.35143008828163147,
//             0.004035519901663065,
//             -0.01819913275539875,
//             -0.20900177955627441,
//             -0.4061185121536255,
//             0.2089434266090393,
//             -0.0347939059138298,
//             -0.5837371349334717,
//             0.010584918782114983,
//             0.8106958270072937,
//             0.2941385805606842,
//             0.1278328150510788,
//             0.18431633710861206,
//             0.5740148425102234,
//             -0.1314437836408615,
//             -0.13735194504261017,
//             0.4939609169960022,
//             0.16223137080669403,
//             -0.009350121021270752,
//             -0.11446764320135117,
//             0.23220545053482056,
//             0.12244246900081635,
//             -0.07539188861846924,
//             -0.47403448820114136,
//             -0.21040581166744232,
//             -0.1812298595905304,
//             0.0033964926842600107,
//             0.051997698843479156,
//             -0.47181084752082825,
//             0.13163599371910095,
//             0.04450765997171402,
//             -0.10071442276239395,
//             0.22506563365459442,
//             0.03204094618558884,
//             -0.26264238357543945,
//             -0.25770825147628784,
//             -0.12184076011180878,
//             -0.0927547812461853,
//             -0.10192013531923294,
//             0.4039716422557831,
//             -0.2526731789112091,
//             -0.43994367122650146,
//             -0.23776157200336456,
//             0.005957856308668852,
//             0.9482496380805969,
//             -0.15066862106323242,
//             -0.3917495310306549,
//             0.18831054866313934,
//             0.6361973285675049,
//             0.2419678270816803,
//             -0.1197468563914299,
//             -0.6699455380439758,
//             -0.034373000264167786,
//             -0.7605501413345337,
//             -0.10243991762399673,
//             -0.024176232516765594,
//             0.18393613398075104,
//             -0.08444762229919434,
//             0.38220784068107605,
//             0.09940394014120102,
//             0.08994793146848679,
//             -0.10702430456876755,
//             -0.3265760838985443,
//             0.06651172041893005,
//             -0.1262771487236023,
//             -0.17603352665901184,
//             -0.1866527497768402,
//             0.11890699714422226,
//             -0.1579289585351944,
//             -0.04527043178677559,
//             0.0072666374035179615,
//             0.44578203558921814,
//             -0.1473284214735031,
//             0.29017147421836853,
//             0.26682427525520325,
//             -0.02900419570505619,
//             0.29832255840301514,
//             -0.06339628249406815,
//             0.20134030282497406,
//             0.10107262432575226,
//             0.26416444778442383,
//             -0.3579222857952118,
//             -0.47667649388313293,
//             -0.3723968267440796,
//             -0.17564480006694794,
//             -0.10666484385728836,
//             -0.4373137652873993,
//             0.018749602138996124,
//             -0.16561134159564972,
//             0.08186151832342148,
//             0.11059995740652084,
//             0.11536756157875061,
//             0.3340449035167694,
//             0.8886587023735046,
//             -0.43105167150497437,
//             -0.1873561590909958,
//             0.6482652425765991,
//             0.3045077323913574,
//             -0.20540614426136017,
//             0.1848640888929367,
//             -0.007395120337605476,
//             -0.21085324883460999,
//             0.1016770452260971,
//             -0.007587754167616367,
//             -0.17867514491081238,
//             0.42817914485931396,
//             -0.05757015198469162,
//             0.18592453002929688,
//             -0.13644486665725708,
//             0.3605743646621704,
//             -0.32334133982658386,
//             0.30402040481567383,
//             0.1208118349313736,
//             -0.13703913986682892,
//             -0.23968829214572906,
//             -0.03239250183105469,
//             -0.3825581669807434,
//             0.37176454067230225,
//             -0.5000677108764648,
//             -0.46167877316474915,
//             -0.4506215453147888,
//             -0.21457213163375854,
//             0.17426697909832,
//             -0.10959114134311676,
//             -0.12642402946949005,
//             0.12893939018249512,
//             0.20223315060138702,
//             -0.04979938268661499,
//             -0.1366860270500183,
//             0.23256750404834747,
//             0.16676823794841766,
//             0.5621985793113708,
//             -0.06437458097934723,
//             -0.4573322534561157,
//             0.30401402711868286,
//             0.05954178795218468,
//             0.41750505566596985,
//             -0.06612703949213028,
//             -0.533744752407074,
//             -0.09830960631370544,
//             -0.011073465459048748,
//             -0.14253853261470795,
//             -0.1560450792312622,
//             0.4377437233924866,
//             0.029874999076128006,
//             0.21540489792823792,
//             -0.003699094755575061,
//             0.1273447573184967,
//             0.15979835391044617,
//             0.27683573961257935,
//             0.02050231210887432,
//             0.05943753942847252,
//             -0.6136995553970337,
//             0.11259671300649643,
//             0.3303419351577759,
//             0.6980512142181396,
//             0.12436885386705399,
//             0.0022151770535856485,
//             0.1949150413274765,
//             -0.17727389931678772,
//             0.14159582555294037,
//             -0.19538044929504395,
//             -0.19781117141246796,
//             0.3034132421016693,
//             -0.18811047077178955,
//             0.39852020144462585,
//             0.3114132285118103,
//             -0.006904635112732649,
//             0.03227578476071358,
//             0.19010135531425476,
//             -0.3599894046783447,
//             -0.2843908965587616,
//             0.008641859516501427,
//             0.2722780406475067,
//             -0.026909660547971725,
//             0.06931641697883606,
//             0.24612265825271606,
//             -0.26905712485313416,
//             -0.7669890522956848,
//             -0.11265508085489273,
//             -0.20650431513786316,
//             -0.06785731017589569,
//             0.4424210488796234,
//             0.19085563719272614,
//             0.04120302200317383,
//             0.07795687764883041,
//             -0.07799956202507019,
//             0.38592010736465454,
//             0.27375563979148865,
//             0.07405593246221542,
//             -0.4511336088180542,
//             0.027584237977862358,
//             0.008759417571127415,
//             0.23760294914245605,
//             -0.32225358486175537,
//             0.3139445185661316,
//             -0.3390963077545166,
//             0.051674265414476395,
//             0.0462198406457901,
//             0.13921421766281128,
//             -0.2993031442165375,
//             0.17915339767932892,
//             -0.2939128875732422,
//             0.017797553911805153,
//             0.19084390997886658,
//             -0.28022632002830505,
//             0.3470957279205322,
//             -0.5897853970527649,
//             0.2418258935213089,
//             -0.03857063129544258,
//             0.08651014417409897,
//             -0.004123278893530369,
//             0.19197647273540497,
//             -0.10518898069858551,
//             0.3279397785663605,
//             0.21404176950454712,
//             0.24734529852867126,
//             0.1436825543642044,
//             -0.014283177442848682,
//             0.0994878038764,
//             0.30250605940818787,
//             0.22142408788204193,
//             -0.2864673435688019,
//             0.21125350892543793,
//             -0.07997559010982513,
//             -0.08108561486005783,
//             -0.15088143944740295,
//             -0.1736246645450592,
//             0.7520857453346252,
//             0.38929426670074463,
//             -0.05639626458287239,
//             -0.07360056787729263,
//             -0.04087391123175621,
//             0.3969426155090332,
//             0.7942463755607605,
//             0.0221339613199234,
//             0.17308442294597626,
//             0.1257106512784958,
//             0.31765252351760864,
//             -0.34739401936531067,
//             -0.21989849209785461,
//             0.0014013248728588223,
//             0.3032960295677185,
//             0.12906023859977722,
//             0.3308161497116089,
//             0.05975518003106117,
//             0.5436338186264038,
//             -0.2547858953475952,
//             0.27362966537475586,
//             0.18802574276924133,
//             0.05643932893872261,
//             -0.06000639498233795,
//             0.3410680294036865,
//             -0.4128369987010956,
//             0.14690417051315308,
//             -0.25249308347702026,
//             -0.2814539968967438,
//             0.1439356952905655,
//             0.08450578898191452,
//             -0.2566152811050415,
//             0.34819331765174866,
//             0.17418798804283142,
//             0.3702946603298187,
//             -0.3703955411911011,
//             -0.41297709941864014,
//             0.20716823637485504,
//             -0.135623961687088,
//             -0.2483864575624466,
//             0.14141151309013367,
//             -0.05492661893367767,
//             -0.22611507773399353,
//             -0.2004321962594986,
//             0.09372448176145554,
//             0.17102095484733582,
//             -0.5483070611953735,
//             -0.19146107137203217,
//             -0.18416117131710052,
//             -0.406789094209671,
//             -0.31944581866264343
//         ]
//     }
// }
