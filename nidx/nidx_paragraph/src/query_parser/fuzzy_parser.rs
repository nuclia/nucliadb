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
use tantivy::query::Query;
use tantivy::query::TermQuery;
use tantivy::schema::IndexRecordOption;

use crate::fuzzy_query::FuzzyTermQuery;
use crate::schema::ParagraphSchema;
use crate::search_query::SharedTermC;

use super::FallbackQuery;
use super::keyword_parser;
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
pub fn parse_fuzzy_query(
    query: &[Token],
    term_collector: SharedTermC,
    last_literal_as_prefix: bool,
) -> Result<Box<dyn Query>, FallbackQuery> {
    let mut errors = vec![];

    let last_literal_index = if last_literal_as_prefix {
        query.iter().rposition(|token| matches!(token, Token::Literal(_)))
    } else {
        None
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
                } else if matches!(last_literal_index, Some(idx) if idx == i) && literal.len() >= MIN_FUZZY_PREFIX_LEN {
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
                match keyword_parser::parse_quoted(&schema, quoted) {
                    Ok(q) => {
                        subqueries.push((Occur::Should, q));
                    }
                    Err(error) => {
                        errors.push(error);
                    }
                };
            }
            Token::Excluded(excluded) => {
                let q = keyword_parser::parse_excluded(&schema, excluded);
                subqueries.push((Occur::Should, q));
            }
        }
    }

    let q = if subqueries.is_empty() {
        Box::new(AllQuery)
    } else if subqueries.len() == 1 {
        subqueries.pop().unwrap().1
    } else {
        Box::new(BooleanQuery::new(subqueries))
    };

    if errors.is_empty() { Ok(q) } else { Err((q, errors)) }
}

#[cfg(test)]
mod tests {
    use crate::search_query::TermCollector;

    use super::*;

    #[test]
    fn test_short_literals_do_not_fuzzy() {
        let term_collector = SharedTermC::from(TermCollector::new());

        // literal shorter than MIN_FUZZY_LEN will become a TermQuery
        let literal = "ab";
        assert!(literal.len() < MIN_FUZZY_LEN);
        let query = [Token::Literal(literal.into())];
        let fuzzy = parse_fuzzy_query(&query, term_collector.clone(), false).unwrap();
        assert!(fuzzy.is::<TermQuery>());
        let q = fuzzy.downcast::<TermQuery>().unwrap();
        assert_eq!(q.term().value().as_str(), Some(literal));
    }

    #[test]
    fn test_fuzzy_literals() {
        let term_collector = SharedTermC::from(TermCollector::new());

        let literal = "abcd";
        assert!(literal.len() >= MIN_FUZZY_LEN);
        let query = [Token::Literal(literal.into())];
        let fuzzy = parse_fuzzy_query(&query, term_collector.clone(), false).unwrap();
        assert!(fuzzy.is::<FuzzyTermQuery>());
        assert!(!fuzzy.downcast::<FuzzyTermQuery>().unwrap().is_prefix());
    }

    #[test]
    fn test_fuzzy_prefix() {
        let term_collector = SharedTermC::from(TermCollector::new());

        // literals longer than the min fuzzy prefix become prefix if they are
        // last and the flag is enabled

        let literal = "abcd";
        assert!(literal.len() >= MIN_FUZZY_PREFIX_LEN);
        let query = [Token::Literal(literal.into())];

        let fuzzy = parse_fuzzy_query(&query, term_collector.clone(), false).unwrap();
        assert!(fuzzy.is::<FuzzyTermQuery>());
        assert!(!fuzzy.downcast::<FuzzyTermQuery>().unwrap().is_prefix());

        let fuzzy = parse_fuzzy_query(&query, term_collector.clone(), true).unwrap();
        assert!(fuzzy.is::<FuzzyTermQuery>());
        assert!(fuzzy.downcast::<FuzzyTermQuery>().unwrap().is_prefix());

        // only the last term is fuzzy prefix

        let query = [Token::Literal(literal.into()), Token::Literal(literal.into())];
        let fuzzy = parse_fuzzy_query(&query, term_collector.clone(), true).unwrap();
        assert!(fuzzy.is::<BooleanQuery>());
        let q = fuzzy.downcast::<BooleanQuery>().unwrap();
        let clauses = q.clauses();
        assert_eq!(clauses.len(), 2);
        assert!(clauses[0].1.is::<FuzzyTermQuery>());
        assert!(!clauses[0].1.downcast_ref::<FuzzyTermQuery>().unwrap().is_prefix());
        assert!(clauses[1].1.is::<FuzzyTermQuery>());
        assert!(clauses[1].1.downcast_ref::<FuzzyTermQuery>().unwrap().is_prefix());

        // however, shorter terms won't become prefix

        let literal = "abc";
        assert!(literal.len() < MIN_FUZZY_PREFIX_LEN);
        let query = [Token::Literal(literal.into())];

        let fuzzy = parse_fuzzy_query(&query, term_collector.clone(), false).unwrap();
        assert!(fuzzy.is::<FuzzyTermQuery>());
        assert!(!fuzzy.downcast::<FuzzyTermQuery>().unwrap().is_prefix());

        let fuzzy = parse_fuzzy_query(&query, term_collector.clone(), true).unwrap();
        assert!(fuzzy.is::<FuzzyTermQuery>());
        assert!(!fuzzy.downcast::<FuzzyTermQuery>().unwrap().is_prefix());
    }
}
