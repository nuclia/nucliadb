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
