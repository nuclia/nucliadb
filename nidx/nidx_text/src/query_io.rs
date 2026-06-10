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
