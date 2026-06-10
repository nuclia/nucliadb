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
