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
use tokenizer::{Token, tokenize_query};

use crate::{
    schema::ParagraphSchema,
    search_query::{SharedTermC, TermCollector},
};

use stop_words::remove_stop_words;

pub use stop_words::is_stop_word;

/// Parser configuration parameters. With this object, one can customize the
/// parsing process.
///
/// It uses a builder pattern to build this struct
pub struct ParserConfig<'a> {
    schema: &'a ParagraphSchema,
    fuzzy: FuzzyConfig,
}

struct FuzzyConfig {
    last_literal_as_fuzzy_prefix: bool,
}

#[allow(non_snake_case)]
pub struct ParserConfigBuilder<'a> {
    schema: &'a ParagraphSchema,
    fuzzy__last_literal_as_fuzzy_prefix: bool,
}

/// A parsed query containing an exact keyword query and a fallback fuzzy one.
pub struct ParsedQuery {
    pub keyword: Box<dyn Query>,
    pub fuzzy: Box<dyn Query>,
    pub term_collector: SharedTermC,
}

pub fn parse_query<'a>(query: &'a str, config: ParserConfig) -> anyhow::Result<ParsedQuery> {
    let (rest, tokenized): (&'a str, Vec<Token<'a>>) = tokenize_query(query).unwrap();
    // TODO: we should think about parser errors, as they should never happen is
    // our parser is correct (as we have a lenient implementation)
    debug_assert!(rest.is_empty(), "we expect to parse the whole query");

    let tokenized = remove_stop_words(tokenized);

    let mut term_collector = TermCollector::new();
    for token in tokenized.iter() {
        if let Token::Literal(literal) = token {
            term_collector.log_eterm(literal.to_string());
        }
    }
    let shared_term_collector = SharedTermC::from(term_collector);

    let keyword = keyword_parser::parse_keyword_query(&tokenized, config.schema);
    let fuzzy = fuzzy_parser::parse_fuzzy_query(
        &tokenized,
        shared_term_collector.clone(),
        config.fuzzy.last_literal_as_fuzzy_prefix,
    );
    Ok(ParsedQuery {
        keyword,
        fuzzy,
        term_collector: shared_term_collector,
    })
}

impl ParserConfig<'_> {
    pub fn builder(schema: &ParagraphSchema) -> ParserConfigBuilder {
        ParserConfigBuilder {
            schema,
            fuzzy__last_literal_as_fuzzy_prefix: false,
        }
    }
}

impl<'a> ParserConfigBuilder<'a> {
    pub fn build(self) -> ParserConfig<'a> {
        ParserConfig {
            schema: self.schema,
            fuzzy: FuzzyConfig {
                last_literal_as_fuzzy_prefix: self.fuzzy__last_literal_as_fuzzy_prefix,
            },
        }
    }

    pub fn last_literal_as_fuzzy_prefix(&mut self) {
        self.fuzzy__last_literal_as_fuzzy_prefix = true;
    }
}
