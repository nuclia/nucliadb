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
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use nucliadb_protos::ParagraphSearchRequest;
use nucliadb_service_interface::prelude::*;
use tantivy::query::*;
use tantivy::schema::{Facet, IndexRecordOption};
use tantivy::{DocId, InvertedIndexReader, Term};

use crate::fuzzy_query::FuzzyTermQuery;
use crate::schema::ParagraphSchema;
use crate::stop_words::STOP_WORDS;

type QueryP = (Occur, Box<dyn Query>);
type NewFuzz = fn(Term, u8, bool, SharedTermC) -> FuzzyTermQuery;

// Used to identify the terms matched by tantivy
#[derive(Clone)]
pub struct TermCollector {
    pub eterms: HashSet<String>,
    pub fterms: HashMap<DocId, Vec<(Arc<InvertedIndexReader>, u64)>>,
}
impl Default for TermCollector {
    fn default() -> Self {
        Self::new()
    }
}
impl TermCollector {
    pub fn new() -> TermCollector {
        TermCollector {
            fterms: HashMap::new(),
            eterms: HashSet::new(),
        }
    }
    pub fn log_eterm(&mut self, term: String) {
        self.eterms.insert(term);
    }
    pub fn log_fterm(&mut self, doc: DocId, data: (Arc<InvertedIndexReader>, u64)) {
        self.fterms.entry(doc).or_insert_with(Vec::new).push(data);
    }
    pub fn get_fterms(&self, doc: DocId) -> Vec<String> {
        let mut terms = Vec::new();
        for (index, term) in self.fterms.get(&doc).iter().flat_map(|v| v.iter()).cloned() {
            let term_dict = index.terms();
            let mut term_s = vec![];
            let found = term_dict.ord_to_term(term, &mut term_s).unwrap_or(false);
            let elem = if found { term_s } else { vec![] };
            match String::from_utf8(elem).ok() {
                Some(v) if v.len() > 2 => terms.push(v),
                _ => (),
            }
        }
        terms
    }
}

#[derive(Default, Clone)]
pub struct SharedTermC(Arc<Mutex<TermCollector>>);
impl SharedTermC {
    pub fn new() -> SharedTermC {
        SharedTermC::default()
    }
    pub fn get_termc(&self) -> TermCollector {
        std::mem::take(&mut self.0.lock().unwrap())
    }
    pub fn set_termc(&self, termc: TermCollector) {
        *self.0.lock().unwrap() = termc;
    }
}

fn term_query_to_fuzzy(
    query: Box<dyn Query>,
    distance: u8,
    termc: SharedTermC,
    with: NewFuzz,
) -> Box<dyn Query> {
    let term_query: &TermQuery = query.downcast_ref().unwrap();
    let term = term_query.term().clone();
    let mut terms = termc.get_termc();
    term.as_str()
        .into_iter()
        .for_each(|t| terms.log_eterm(t.to_string()));
    termc.set_termc(terms);
    Box::new(with(term, distance, true, termc))
}

fn queryp_map(
    queries: Vec<QueryP>,
    distance: u8,
    as_prefix: usize,
    termc: SharedTermC,
) -> Vec<QueryP> {
    queries
        .into_iter()
        .enumerate()
        .map(|(id, (_, query))| {
            let query = if query.is::<TermQuery>() && id == as_prefix {
                term_query_to_fuzzy(query, distance, termc.clone(), FuzzyTermQuery::new_prefix)
            } else if query.is::<TermQuery>() {
                term_query_to_fuzzy(query, distance, termc.clone(), FuzzyTermQuery::new)
            } else if query.is::<PhraseQuery>() {
                let phrase: &PhraseQuery = query.downcast_ref().unwrap();
                let mut total = vec![];
                for term in phrase.phrase_terms() {
                    total.append(&mut term.value_bytes().to_vec());
                    total.append(&mut b" ".to_vec());
                }
                if let Ok(mut eterm) = String::from_utf8(total) {
                    eterm.pop();
                    let mut terms = termc.get_termc();
                    terms.log_eterm(eterm);
                    termc.set_termc(terms);
                }
                query
            } else {
                query
            };
            (Occur::Must, query)
        })
        .collect()
}

fn flat_bool_query(query: BooleanQuery, collector: (usize, Vec<QueryP>)) -> (usize, Vec<QueryP>) {
    query
        .clauses()
        .iter()
        .map(|(occur, subq)| (*occur, subq.box_clone()))
        .fold(collector, |(mut id, mut c), (occur, subq)| {
            if subq.is::<BooleanQuery>() {
                let subq: Box<BooleanQuery> = subq.downcast().unwrap();
                flat_bool_query(*subq, (id, c))
            } else if subq.is::<TermQuery>() {
                id = c.len();
                c.push((occur, subq));
                (id, c)
            } else {
                c.push((occur, subq));
                (id, c)
            }
        })
}

fn flat_and_adapt(query: Box<dyn Query>, distance: u8, termc: SharedTermC) -> Vec<QueryP> {
    let (queries, as_prefix) = if query.is::<BooleanQuery>() {
        let query: Box<BooleanQuery> = query.downcast().unwrap();
        let (as_prefix, queries) = flat_bool_query(*query, (usize::MAX, vec![]));
        (queries, as_prefix)
    } else if query.is::<TermQuery>() {
        let queries = vec![(Occur::Must, query)];
        let as_prefix = 0;
        (queries, as_prefix)
    } else {
        let queries = vec![(Occur::Must, query)];
        let as_prefix = 1;
        (queries, as_prefix)
    };
    queryp_map(queries, distance, as_prefix, termc)
}

fn fuzzied_queries(query: Box<dyn Query>, distance: u8, termc: SharedTermC) -> Vec<QueryP> {
    if query.is::<AllQuery>() {
        vec![(Occur::Should, query)]
    } else {
        flat_and_adapt(query, distance, termc)
    }
}

fn parse_query(parser: &QueryParser, text: &str) -> Box<dyn Query> {
    if text.is_empty() {
        Box::new(AllQuery) as Box<dyn Query>
    } else {
        parser
            .parse_query(text)
            .ok()
            .unwrap_or_else(|| Box::new(AllQuery))
    }
}

/// Removes all fuzzy terms identified as stop word.
///
/// A stop word is any fuzzy term that match the following criterias:
/// - Presents in the given list of stop words
/// - Is **NOT** the last term in the query
///
/// The last term of the query is a prefix fuzzy term and must be preserved.
fn remove_stop_words<'a>(query: &'a str, stop_words: &[&str]) -> Cow<'a, str> {
    match query.rsplit_once(' ') {
        Some((query, last_term)) => query
            .split(' ')
            .filter(|term| !stop_words.contains(term))
            .chain([last_term])
            .join(" ")
            .into(),
        None => query.into(),
    }
}

pub fn create_query(
    parser: &QueryParser,
    text: &str,
    search: &ParagraphSearchRequest,
    schema: &ParagraphSchema,
    distance: u8,
) -> (Box<dyn Query>, SharedTermC, Box<dyn Query>) {
    let (quotes, mut reg) =
        text.split(' ')
            .into_iter()
            .fold((vec![], String::new()), |(mut quotes, mut reg), crnt| {
                if crnt.starts_with('"') && crnt.ends_with('"') {
                    quotes.push(crnt);
                } else {
                    reg.push_str(crnt);
                    reg.push(' ');
                }
                (quotes, reg)
            });
    reg.pop();
    let termc = SharedTermC::new();
    let query = parse_query(parser, &reg);
    let fuzzy_query = parse_query(parser, &remove_stop_words(&reg, &STOP_WORDS[..]));
    let mut fuzzies = fuzzied_queries(fuzzy_query, distance, termc.clone());
    let mut originals = vec![(Occur::Must, query)];
    for quote in quotes {
        let query = parser.parse_query(quote).unwrap();
        fuzzies.push((Occur::Must, query.box_clone()));
        originals.push((Occur::Must, query));
    }
    if !search.uuid.is_empty() {
        let term = Term::from_field_text(schema.uuid, &search.uuid);
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        fuzzies.push((Occur::Must, Box::new(term_query.clone())));
        originals.push((Occur::Must, Box::new(term_query)))
    }

    // Fields
    search
        .fields
        .iter()
        .map(|value| format!("/{}", value))
        .flat_map(|facet_key| Facet::from_text(&facet_key).ok().into_iter())
        .for_each(|facet| {
            let facet_term = Term::from_facet(schema.field, &facet);
            let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
            fuzzies.push((Occur::Must, Box::new(facet_term_query.clone())));
            originals.push((Occur::Must, Box::new(facet_term_query)));
        });

    // Add filter
    search
        .filter
        .iter()
        .flat_map(|f| f.tags.iter())
        .flat_map(|facet_key| Facet::from_text(facet_key).ok().into_iter())
        .for_each(|facet| {
            let facet_term = Term::from_facet(schema.facets, &facet);
            let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
            fuzzies.push((Occur::Must, Box::new(facet_term_query.clone())));
            originals.push((Occur::Must, Box::new(facet_term_query)));
        });
    let original = Box::new(BooleanQuery::new(originals));
    let fuzzied = Box::new(BoostQuery::new(Box::new(BooleanQuery::new(fuzzies)), 0.5));
    (original, termc, fuzzied)
}

#[cfg(test)]
mod tests {
    use tantivy::schema::Field;

    use super::*;
    fn dummy_term_query() -> Box<dyn Query> {
        let field = Field::from_field_id(0);
        let term = Term::from_field_u64(field, 0);
        Box::new(TermQuery::new(term, IndexRecordOption::Basic))
    }

    #[test]
    fn test() {
        let subqueries0: Vec<_> = vec![dummy_term_query; 12]
            .into_iter()
            .map(|f| (Occur::Must, f()))
            .collect();
        let subqueries1: Vec<_> = vec![dummy_term_query; 12]
            .into_iter()
            .map(|f| (Occur::Must, f()))
            .collect();
        let boolean0: Box<dyn Query> = Box::new(BooleanQuery::new(subqueries0));
        let boolean1: Box<dyn Query> = Box::new(BooleanQuery::new(subqueries1));
        let nested = BooleanQuery::new(vec![(Occur::Should, boolean0), (Occur::Should, boolean1)]);
        let adapted = flat_and_adapt(Box::new(nested), 2, SharedTermC::new());
        assert_eq!(adapted.len(), 24);
        assert!(adapted.iter().all(|(occur, _)| *occur == Occur::Must));
        assert!(adapted
            .iter()
            .all(|(_, query)| query.is::<FuzzyTermQuery>()));
    }

    #[test]
    fn it_removes_stop_word_fterms() {
        let stop_words = &["is", "a", "for", "and"];
        let tests = [
            (
                "nuclia is a database for unstructured data",
                "nuclia database unstructured data",
            ),
            (
                "nuclia is a",
                // keeps last term even if is a stop word
                "nuclia a",
            ),
            ("is a for and", "and"),
        ];

        for (query, expected_fuzzy_query) in tests {
            let fuzzy_query = remove_stop_words(query, &stop_words[..]);

            assert_eq!(fuzzy_query, expected_fuzzy_query);
        }
    }
}
