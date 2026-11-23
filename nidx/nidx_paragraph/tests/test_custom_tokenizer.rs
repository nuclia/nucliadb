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

mod common;
use common::TestOpener;
use nidx_paragraph::{CustomTokenizer, ParagraphConfig, ParagraphIndexer, ParagraphSearchRequest, ParagraphSearcher};
use nidx_protos::{IndexParagraph, IndexParagraphs, Resource, ResourceId};
use nidx_tantivy::tokenizers::magic_char::MAGIC_CHAR;
use nidx_types::prefilter::PrefilterResult;
use tempfile::TempDir;

use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use nidx_protos::prost_types::Timestamp;

#[test]
fn test_custom_tokenizers() {
    fn query(reader: &ParagraphSearcher, query: impl Into<String>, expected: HashSet<&str>) {
        let query = query.into();
        let request = ParagraphSearchRequest {
            body: query.clone(),
            result_per_page: 20,
            ..Default::default()
        };
        let response = reader.search(&request, &PrefilterResult::All).unwrap();
        assert_eq!(
            response.total as usize,
            expected.len(),
            "Failed query: '{}'.\nResponse: {:#?}",
            query,
            response
        );

        assert_eq!(response.total, response.results.len() as i32);
        assert!(!response.next_page);

        let mut fields: HashSet<&str> = HashSet::new();
        for result in &response.results {
            fields.insert(&result.field);
        }
        assert_eq!(fields, expected);
    }

    let reader = test_reader();

    // empty query matches all
    query(&reader, "", HashSet::from(["/a/doctor", "/a/teacher", "/a/lawyer"]));

    // exact text
    query(&reader, "Doctor", HashSet::from(["/a/doctor"]));

    // lowercased text
    query(&reader, "doctor", HashSet::from(["/a/doctor"]));

    // "percolator"-style queries
    query(
        &reader,
        "Yesterday, my teacher told me I should visit a doctor",
        HashSet::from(["/a/doctor", "/a/teacher"]),
    );

    // FIXME: percolator queries may need a custom search tokenizer that
    // searches for n-grams on the query. Otherwise, multi words like "medical
    // professional" are indexed as a single term but searched as two, which
    // yield no result.
    query(
        &reader,
        "Yesterday, my teacher told me I should visit a medical professional",
        HashSet::from(["/a/teacher"]),
    );
}

pub fn test_reader() -> ParagraphSearcher {
    let dir = TempDir::new().unwrap();
    let resource = create_resource("shard".to_string());
    let config = ParagraphConfig {
        custom_tokenizer: Some(CustomTokenizer::MagicChar),
    };
    let segment_meta = ParagraphIndexer
        .index_resource(dir.path(), config.clone(), &resource)
        .unwrap()
        .unwrap();

    ParagraphSearcher::open(config, TestOpener::new(vec![(segment_meta, 1i64.into())], vec![])).unwrap()
}

pub fn create_resource(shard_id: String) -> Resource {
    let rid = "f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string();
    let resource_id = ResourceId {
        shard_id: shard_id.to_string(),
        uuid: rid.clone(),
    };

    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let timestamp = Timestamp {
        seconds: now.as_secs() as i64,
        nanos: 0,
    };

    let metadata = nidx_protos::IndexMetadata {
        created: Some(timestamp),
        modified: Some(timestamp),
    };

    let dataset = [
        (
            "a/doctor".to_string(),
            format!("Doctor;Physician;Medical professional").replace(";", &MAGIC_CHAR.to_string()),
        ),
        (
            "a/teacher".to_string(),
            format!("Teacher;Educator;Tutor").replace(";", &MAGIC_CHAR.to_string()),
        ),
        (
            "a/lawyer".to_string(),
            format!("Lawyer;Attorney;Legal counsel;Solicitor").replace(";", &MAGIC_CHAR.to_string()),
        ),
    ];

    let mut texts = HashMap::new();
    let mut paragraphs = HashMap::new();
    for (field_id, text) in dataset {
        let paragraph_id = format!("{rid}/{field_id}/0-{}", text.len());
        paragraphs.insert(
            field_id.clone(),
            IndexParagraphs {
                paragraphs: HashMap::from([(
                    paragraph_id,
                    IndexParagraph {
                        field: field_id.clone(),
                        start: 0,
                        end: text.len() as i32,
                        ..Default::default()
                    },
                )]),
            },
        );
        texts.insert(
            field_id,
            nidx_protos::TextInformation {
                text,
                ..Default::default()
            },
        );
    }

    Resource {
        resource: Some(resource_id),
        metadata: Some(metadata),
        texts,
        status: nidx_protos::resource::ResourceStatus::Processed as i32,
        labels: vec![],
        paragraphs,
        paragraphs_to_delete: vec![],
        shard_id,
        ..Default::default()
    }
}
