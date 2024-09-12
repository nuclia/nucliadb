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

use crate::schema::TextSchema;
use nucliadb_core::query_language::{BooleanExpression, BooleanOperation, Operator};
use tantivy::query::{AllQuery, BooleanQuery, Occur, PhraseQuery, Query, TermQuery};
use tantivy::schema::{Facet, IndexRecordOption};
use tantivy::tokenizer::TokenizerManager;
use tantivy::Term;

fn translate_label_to_facet_query(literal: &str, schema: &TextSchema) -> Box<dyn Query> {
    let facet = Facet::from_text(literal).unwrap();
    let term = Term::from_facet(schema.facets, &facet);
    Box::new(TermQuery::new(term, IndexRecordOption::Basic))
}

fn translate_keyword_to_text_query(literal: &str, schema: &TextSchema) -> Box<dyn Query> {
    // Tokenize the literal in the same way we tokenize the text field at indexing time
    let tokenizer = TokenizerManager::default().get("default").unwrap();
    let mut token_stream = tokenizer.token_stream(literal);
    let mut terms = Vec::new();
    while let Some(token) = token_stream.next() {
        terms.push(Term::from_field_text(schema.text, &token.text));
    }
    // Create a query using the tokenized terms
    if terms.len() == 1 {
        Box::new(TermQuery::new(terms[0].clone(), IndexRecordOption::Basic))
    } else {
        Box::new(PhraseQuery::new(terms))
    }
}

fn translate_not(inner: &BooleanExpression, schema: &TextSchema, is_keyword: bool) -> Box<dyn Query> {
    let mut operands = Vec::with_capacity(2);

    // Check the following issue to see why the additional AllQuery is needed:
    // https://github.com/quickwit-oss/tantivy/issues/2317
    let all_query: Box<dyn Query> = Box::new(AllQuery);
    operands.push((Occur::Must, all_query));

    let subquery = translate_expression(inner, schema, is_keyword);
    operands.push((Occur::MustNot, subquery));

    Box::new(BooleanQuery::new(operands))
}

fn translate_operation(operation: &BooleanOperation, schema: &TextSchema, is_keyword: bool) -> Box<dyn Query> {
    let operator = match operation.operator {
        Operator::And => Occur::Must,
        Operator::Or => Occur::Should,
    };

    let mut operands = Vec::with_capacity(operation.operands.len());

    for operand in operation.operands.iter() {
        let subquery = translate_expression(operand, schema, is_keyword);
        operands.push((operator, subquery));
    }

    Box::new(BooleanQuery::new(operands))
}

fn translate_expression(expression: &BooleanExpression, schema: &TextSchema, is_keyword: bool) -> Box<dyn Query> {
    // is_keyword is used to determine if the query should be translated to a facet query or a text query
    match expression {
        BooleanExpression::Not(inner) => translate_not(inner, schema, is_keyword),
        BooleanExpression::Literal(literal) => match is_keyword {
            true => translate_keyword_to_text_query(literal, schema),
            false => translate_label_to_facet_query(literal, schema),
        },
        BooleanExpression::Operation(operation) => translate_operation(operation, schema, is_keyword),
    }
}

pub fn translate_labels_expression(expression: &BooleanExpression, schema: &TextSchema) -> Box<dyn Query> {
    translate_expression(expression, schema, false)
}

pub fn translate_keywords_expression(expression: &BooleanExpression, schema: &TextSchema) -> Box<dyn Query> {
    translate_expression(expression, schema, true)
}
