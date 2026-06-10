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

use nidx_protos::FilterExpression;
use nidx_protos::StreamRequest;
use nidx_protos::filter_expression::FacetFilter;
mod common;

#[test]
fn test_stream_request_iterator() {
    let reader = common::test_reader();

    // There are two fields in total
    let request = StreamRequest {
        shard_id: None,
        ..Default::default()
    };
    let iter = reader.iterator(&request).unwrap();
    let count = iter.count();
    assert_eq!(count, 2);

    // Filtering on a non-existing label should not yield any result
    let request = StreamRequest {
        shard_id: None,
        filter_expression: Some(FilterExpression {
            expr: Some(nidx_protos::filter_expression::Expr::Facet(FacetFilter {
                facet: "/l/non-existing-label".into(),
            })),
        }),
        ..Default::default()
    };
    let iter = reader.iterator(&request).unwrap();
    let count = iter.count();
    assert_eq!(count, 0);

    // The label /l/mylabel should match the title field
    let request = StreamRequest {
        shard_id: None,
        filter_expression: Some(FilterExpression {
            expr: Some(nidx_protos::filter_expression::Expr::Facet(FacetFilter {
                facet: "/l/mylabel".into(),
            })),
        }),
        ..Default::default()
    };
    let iter = reader.iterator(&request).unwrap();
    let count = iter.count();
    assert_eq!(count, 1);
}
