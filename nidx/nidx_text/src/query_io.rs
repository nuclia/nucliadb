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
use tantivy::Term;
use tantivy::query::{PhraseQuery, Query, TermQuery};
use tantivy::schema::IndexRecordOption;
use tantivy::tokenizer::TokenizerManager;

pub fn translate_keyword_to_text_query(literal: &str, schema: &TextSchema) -> Box<dyn Query> {
    // Tokenize the literal in the same way we tokenize the text field at indexing time
    let mut tokenizer = TokenizerManager::default().get("default").unwrap();
    let mut token_stream = tokenizer.token_stream(literal);
    let mut terms = Vec::new();
    while let Some(token) = token_stream.next() {
        terms.push(Term::from_field_text(schema.text, &token.text));
    }
    // Create a query using the tokenized terms
    if terms.is_empty() {
        // This can happen for empty or non-alphanumeric keywords, fallback to avoid panic'ing
        Box::new(TermQuery::new(
            Term::from_field_text(schema.text, literal),
            IndexRecordOption::Basic,
        ))
    } else if terms.len() == 1 {
        Box::new(TermQuery::new(terms[0].clone(), IndexRecordOption::Basic))
    } else {
        Box::new(PhraseQuery::new(terms))
    }
}
