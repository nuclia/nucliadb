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

use tantivy::Term;
use tantivy::query::AllQuery;
use tantivy::query::BooleanQuery;
use tantivy::query::Occur;
use tantivy::query::PhraseQuery;
use tantivy::query::Query;
use tantivy::query::TermQuery;
use tantivy::schema::IndexRecordOption;

use crate::fuzzy_query::FuzzyTermQuery;
use crate::schema::ParagraphSchema;
use crate::search_query::SharedTermC;

use super::tokenizer::Token;

/// Minimum length required to be considered a fuzzy word. Words with smaller
/// length than this are considered too short to be fuzzy. This is done to avoid
/// too much noise from short terms.
const MIN_FUZZY_LEN: usize = 3;

/// Minimum length required to be considered a fuzzy prefix. This is again don
/// eto avoid too much noise from short term.
const MIN_FUZZY_PREFIX_LEN: usize = 4;

/// Levenshtein distance used by all fuzzy terms.
pub const FUZZY_DISTANCE: u8 = 1;

/// Convert a tokenized query into a tantivy fuzzy query
///
/// A fuzzy query will match similarly to a keyword query but some terms will be
/// searched with certain Levenshtein distance. Quoted and excluded terms will
/// remain the same, but literals will be elegible to be fuzzy. All long enough
/// literals will be searched as fuzzy. The last literal can be searched as a
/// fuzzy prefix if a suggest-like feature is desired.
///
pub fn parse_fuzzy_query(query: &[Token], term_collector: SharedTermC, last_literal_as_prefix: bool) -> Box<dyn Query> {
    // We use usize::MAX as a discriminant when we don't want/care about the
    // last index. This value won't match as there's no query that long.
    let last_literal_index = if last_literal_as_prefix {
        query.iter().enumerate().fold(
            usize::MAX,
            |acc, (idx, token)| {
                if matches!(token, &Token::Literal(_)) { idx } else { acc }
            },
        )
    } else {
        usize::MAX
    };

    let mut subqueries = vec![];
    let schema = ParagraphSchema::new();

    for (i, item) in query.iter().enumerate() {
        match item {
            Token::Literal(literal) => {
                let term = Term::from_field_text(schema.text, literal);
                let distance = FUZZY_DISTANCE;
                let transposition_cost_one = true;

                let q: Box<dyn Query>;
                if literal.len() < MIN_FUZZY_LEN {
                    // to avoid noise, we don't want to match too short terms as fuzzy
                    q = Box::new(TermQuery::new(term, IndexRecordOption::Basic));
                } else if last_literal_as_prefix && (i == last_literal_index) && literal.len() >= MIN_FUZZY_PREFIX_LEN {
                    q = Box::new(FuzzyTermQuery::new_prefix(
                        term,
                        distance,
                        transposition_cost_one,
                        term_collector.clone(),
                    ));
                } else {
                    q = Box::new(FuzzyTermQuery::new(
                        term,
                        distance,
                        transposition_cost_one,
                        term_collector.clone(),
                    ));
                }
                subqueries.push((Occur::Should, q));
            }
            Token::Quoted(quoted) => {
                let mut terms: Vec<Term> = quoted
                    .split_whitespace()
                    .map(|word| Term::from_field_text(schema.text, word))
                    .collect();

                #[allow(clippy::comparison_chain)]
                if terms.len() == 1 {
                    // phrase queries must have more than one term, so we use a term query
                    let term = terms.remove(0); // safe because terms.len() == 1
                    let q: Box<dyn Query> = Box::new(TermQuery::new(term, IndexRecordOption::Basic));
                    subqueries.push((Occur::Should, q));
                } else if terms.len() > 1 {
                    let q: Box<dyn Query> = Box::new(PhraseQuery::new(terms));
                    subqueries.push((Occur::Should, q));
                }
            }
            Token::Excluded(excluded) => {
                let q: Box<dyn Query> = Box::new(BooleanQuery::new(vec![
                    (Occur::Must, Box::new(AllQuery)),
                    (
                        Occur::MustNot,
                        Box::new(TermQuery::new(
                            Term::from_field_text(schema.text, excluded),
                            IndexRecordOption::Basic,
                        )),
                    ),
                ]));
                subqueries.push((Occur::Should, q));
            }
        }
    }

    if subqueries.is_empty() {
        Box::new(AllQuery)
    } else if subqueries.len() == 1 {
        subqueries.pop().unwrap().1
    } else {
        Box::new(BooleanQuery::new(subqueries))
    }
}
