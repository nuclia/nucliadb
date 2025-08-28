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
mod fuzzy_parser;
mod keyword_parser;
mod stop_words;
mod tokenizer;

use tantivy::query::Query;
use tokenizer::{Token, tokenize_query_infallible};
use tracing::error;

use crate::{
    schema::ParagraphSchema,
    search_query::{SharedTermC, TermCollector},
};

pub use fuzzy_parser::FUZZY_DISTANCE;
use stop_words::remove_stop_words;

/// Alias to make clippy happier. A FallbackQuery is no more than a query with a list of errors
/// found while parsing
pub type FallbackQuery = (Box<dyn Query>, Vec<String>);

/// `Parser` is the nidx keyword grammar query parser
///
/// It allows some behavior configuration and query parsing capabilities. Query
/// parsing results in two different queries: keyword and fuzzy. The keyword
/// query is the most exact representation of the user query, while the fuzzy
/// one leaves more room for mistakes and should match more (but worse) results
///
pub struct Parser<'a> {
    schema: &'a ParagraphSchema,
    last_fuzzy_term_as_prefix: bool,
}

/// A parsed query containing an exact keyword query and a fallback fuzzy one.
pub struct ParsedQuery {
    pub keyword: Box<dyn Query>,
    pub fuzzy: Box<dyn Query>,
    pub term_collector: SharedTermC,
}

impl<'a> Parser<'a> {
    pub fn new_with_schema(schema: &'a ParagraphSchema) -> Self {
        Self {
            schema,
            last_fuzzy_term_as_prefix: false,
        }
    }

    /// Last literal term found in the query will be parsed as fuzzy prefix.
    /// This is useful for autocompletion/suggest features
    pub fn last_fuzzy_term_as_prefix(mut self) -> Self {
        self.last_fuzzy_term_as_prefix = true;
        self
    }

    pub fn parse(&self, query: &str) -> ParsedQuery {
        let tokenized = tokenize_query_infallible(query);
        let tokenized = remove_stop_words(tokenized);

        let mut term_collector = TermCollector::new();
        for token in tokenized.iter() {
            match token {
                Token::Literal(value) | Token::Quoted(value) => {
                    term_collector.log_eterm(value.to_string());
                }
                Token::Excluded(_) => {}
            }
        }
        let shared_term_collector = SharedTermC::from(term_collector);

        let keyword = match keyword_parser::parse_keyword_query(&tokenized, self.schema) {
            Ok(q) => q,
            Err((q, errors)) => {
                for err in errors {
                    error!(?query, parser = "keyword", err);
                }
                q
            }
        };

        let fuzzy = match fuzzy_parser::parse_fuzzy_query(
            &tokenized,
            shared_term_collector.clone(),
            self.last_fuzzy_term_as_prefix,
        ) {
            Ok(q) => q,
            Err((q, errors)) => {
                for err in errors {
                    error!(?query, parser = "fuzzy", err);
                }
                q
            }
        };

        ParsedQuery {
            keyword,
            fuzzy,
            term_collector: shared_term_collector,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use itertools::Itertools;

    #[test]
    fn test_stop_word_removal() {
        let tests = [
            (
                "nuclia is a database for unstructured data",
                "nuclia database unstructured data",
            ),
            (
                "nuclia is a database for the",
                // keeps last term even if is a stop word
                "nuclia database the",
            ),
            (
                "nuclia \"is\" a database for the",
                // keeps last term even if is a stop word
                "nuclia \"is\" database the",
            ),
            (
                "nuclia \"...\" a database for the",
                // keeps last term even if is a stop word
                "nuclia database the",
            ),
            ("is a for and", "and"),
            ("what does stop is?", "stop is"),
            ("", ""),
            (
                "comment s'appelle le train à grande vitesse",
                "comment appelle train grande vitesse",
            ),
            (
                "¿Qué significa la palabra sentence en español?",
                "significa palabra sentence español",
            ),
            ("Per què les vaques no són de color rosa?", "vaques color rosa"),
            ("How can I learn to make a flat white?", "learn make flat white"),
            ("Qué es escalada en bloque?", "escalada bloque"),
            (
                "Wer hat gesagt: \"Kaffeetrinken ist integraler Bestandteil des Kletterns\"?",
                "wer gesagt \"kaffeetrinken ist integraler bestandteil des kletterns\"",
            ),
            (
                "i pistacchi siciliani sono i migliori al mondo",
                "pistacchi siciliani migliori mondo",
            ),
        ];

        for (query, expected) in tests {
            let tokenized = tokenize_query_infallible(query);
            let without_stop_words = remove_stop_words(tokenized);
            let clean = without_stop_words
                .into_iter()
                .map(|token| match token {
                    Token::Literal(lit) => lit,
                    Token::Quoted(quoted) => format!("\"{quoted}\"").into(),
                    Token::Excluded(excluded) => format!("-{excluded}").into(),
                })
                .join(" ");
            assert_eq!(clean, expected);
        }
    }
}
