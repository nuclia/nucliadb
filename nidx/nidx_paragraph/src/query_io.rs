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

use crate::schema::ParagraphSchema;
use nidx_types::query_language::{BooleanExpression, BooleanOperation, Operator};
use tantivy::Term;
use tantivy::query::{AllQuery, BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{Facet, IndexRecordOption};

fn translate_literal(literal: &str, schema: &ParagraphSchema) -> Box<dyn Query> {
    let facet = Facet::from_text(literal).unwrap();
    let term = Term::from_facet(schema.facets, &facet);
    Box::new(TermQuery::new(term, IndexRecordOption::Basic))
}

fn translate_not(inner: &BooleanExpression<String>, schema: &ParagraphSchema) -> Box<dyn Query> {
    let mut operands = Vec::with_capacity(2);

    // Check the following issue to see why the additional AllQuery is needed:
    // https://github.com/quickwit-oss/tantivy/issues/2317
    let all_query: Box<dyn Query> = Box::new(AllQuery);
    operands.push((Occur::Must, all_query));

    let subquery = translate_expression(inner, schema);
    operands.push((Occur::MustNot, subquery));

    Box::new(BooleanQuery::new(operands))
}

fn translate_operation(operation: &BooleanOperation<String>, schema: &ParagraphSchema) -> Box<dyn Query> {
    let operator = match operation.operator {
        Operator::And => Occur::Must,
        Operator::Or => Occur::Should,
    };

    let mut operands = Vec::with_capacity(operation.operands.len());

    for operand in operation.operands.iter() {
        let subquery = translate_expression(operand, schema);
        operands.push((operator, subquery));
    }

    Box::new(BooleanQuery::new(operands))
}

pub fn translate_expression(expression: &BooleanExpression<String>, schema: &ParagraphSchema) -> Box<dyn Query> {
    match expression {
        BooleanExpression::Not(inner) => translate_not(inner, schema),
        BooleanExpression::Literal(literal) => translate_literal(literal, schema),
        BooleanExpression::Operation(operation) => translate_operation(operation, schema),
    }
}
