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
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use nucliadb_core::protos::prost_types::Timestamp as ProstTimestamp;
use nucliadb_core::protos::{ParagraphSearchRequest, StreamRequest, SuggestRequest};
use tantivy::query::*;
use tantivy::schema::{Facet, Field, IndexRecordOption};
use tantivy::{DocId, InvertedIndexReader, Term};

use crate::fuzzy_query::FuzzyTermQuery;
use crate::schema::{self, ParagraphSchema};
use crate::stop_words::is_stop_word;
type QueryP = (Occur, Box<dyn Query>);

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
    pub fn from(termc: TermCollector) -> SharedTermC {
        SharedTermC(Arc::new(Mutex::new(termc)))
    }
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

fn term_to_fuzzy(
    query: Box<dyn Query>,
    distance: u8,
    termc: SharedTermC,
    as_prefix: bool,
) -> Box<dyn Query> {
    let term_query: &TermQuery = query.downcast_ref().unwrap();
    let term = term_query.term().clone();
    let term_as_str = term.as_str();
    let should_be_prefixed = term_as_str
        .map(|s| as_prefix && s.len() > 3)
        .unwrap_or_default();
    if should_be_prefixed {
        Box::new(FuzzyTermQuery::new_prefix(term, distance, true, termc))
    } else {
        Box::new(FuzzyTermQuery::new(term, distance, true, termc))
    }
}

fn queryp_map(
    queries: Vec<QueryP>,
    distance: u8,
    as_prefix: Option<usize>,
    termc: SharedTermC,
) -> Vec<QueryP> {
    queries
        .into_iter()
        .enumerate()
        .map(|(id, (_, query))| {
            let query = if query.is::<TermQuery>() {
                term_to_fuzzy(
                    query,
                    distance,
                    termc.clone(),
                    as_prefix.map_or(false, |v| id == v),
                )
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

fn flat_and_adapt(
    query: Box<dyn Query>,
    prefixed: bool,
    distance: u8,
    termc: SharedTermC,
) -> Vec<QueryP> {
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
    queryp_map(
        queries,
        distance,
        if prefixed { Some(as_prefix) } else { None },
        termc,
    )
}

fn fuzzied_queries(
    query: Box<dyn Query>,
    prefixed: bool,
    distance: u8,
    termc: SharedTermC,
) -> Vec<QueryP> {
    if query.is::<AllQuery>() {
        vec![]
    } else {
        flat_and_adapt(query, prefixed, distance, termc)
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
fn remove_stop_words(query: &str) -> Cow<'_, str> {
    match query.rsplit_once(' ') {
        Some((query, last_term)) => query
            .split(' ')
            .filter(|term| !is_stop_word(&term.to_lowercase()))
            .chain([last_term])
            .join(" ")
            .into(),
        None => query.into(),
    }
}

#[derive(Debug)]
struct ProcessedQuery {
    fuzzy_query: String,
    regular_query: String,
}
fn preprocess_raw_query(query: &str, tc: &mut TermCollector) -> ProcessedQuery {
    let mut regular_query = String::new();
    let mut fuzzy_query = String::new();
    let mut quote_starts = vec![];
    let mut quote_ends = vec![];
    let mut start = 0;
    for (i, d) in query.match_indices('\"').enumerate() {
        if i % 2 == 0 {
            quote_starts.push(d.0)
        } else {
            quote_ends.push(d.0)
        }
    }
    for (qstart, qend) in quote_starts.into_iter().zip(quote_ends.into_iter()) {
        let quote = query[(qstart + 1)..qend].trim();
        let unquote = query[start..qstart].trim();
        let unquote = remove_stop_words(unquote);

        unquote
            .split(' ')
            .filter(|s| !s.is_empty())
            .for_each(|t| tc.log_eterm(t.to_string()));
        tc.log_eterm(quote.to_string());

        if !regular_query.is_empty() {
            regular_query.push(' ');
        }
        regular_query.push_str(&unquote);
        regular_query.push(' ');
        regular_query.push('"');
        regular_query.push_str(quote);
        regular_query.push('"');

        if !fuzzy_query.is_empty() {
            fuzzy_query.push(' ');
        }
        fuzzy_query.push_str(&unquote);

        start = qend + 1;
    }
    if start < query.len() {
        let tail = query[start..].trim();
        let tail = remove_stop_words(tail);

        tail.split(' ')
            .filter(|s| !s.is_empty())
            .for_each(|t| tc.log_eterm(t.to_string()));

        if !regular_query.is_empty() {
            regular_query.push(' ');
        }
        regular_query.push_str(&tail);

        if !fuzzy_query.is_empty() {
            fuzzy_query.push(' ');
        }
        fuzzy_query.push_str(&tail);
    }
    ProcessedQuery {
        regular_query,
        fuzzy_query,
    }
}

fn produce_date_range_query(
    field: Field,
    from: Option<ProstTimestamp>,
    to: Option<ProstTimestamp>,
) -> Option<RangeQuery> {
    if from.is_none() && to.is_none() {
        return None;
    }

    let left_date_time = from.map(|t| schema::timestamp_to_datetime_utc(&t));
    let right_date_time = to.map(|t| schema::timestamp_to_datetime_utc(&t));
    let left_term = left_date_time.map(|t| Term::from_field_date(field, &t));
    let right_term = right_date_time.map(|t| Term::from_field_date(field, &t));
    let left_bound = left_term.map(Bound::Included).unwrap_or(Bound::Unbounded);
    let right_bound = right_term.map(Bound::Included).unwrap_or(Bound::Unbounded);
    let xtype = tantivy::schema::Type::Date;
    let query = RangeQuery::new_term_bounds(field, xtype, &left_bound, &right_bound);
    Some(query)
}

pub fn suggest_query(
    parser: &QueryParser,
    text: &str,
    request: &SuggestRequest,
    schema: &ParagraphSchema,
    distance: u8,
) -> (Box<dyn Query>, SharedTermC, Box<dyn Query>) {
    let mut term_collector = TermCollector::default();
    let processed = preprocess_raw_query(text, &mut term_collector);
    let query = parse_query(parser, &processed.regular_query);
    let fuzzy_query = parse_query(parser, &processed.fuzzy_query);
    let termc = SharedTermC::from(term_collector);
    let mut fuzzies = fuzzied_queries(fuzzy_query, true, distance, termc.clone());
    let mut originals = vec![(Occur::Must, query)];
    let term = Term::from_field_u64(schema.repeated_in_field, 0);
    let term_query = TermQuery::new(term, IndexRecordOption::Basic);
    fuzzies.push((Occur::Must, Box::new(term_query.clone())));
    originals.push((Occur::Must, Box::new(term_query)));

    // Fields
    request
        .fields
        .iter()
        .map(|value| format!("/{value}"))
        .flat_map(|facet_key| Facet::from_text(&facet_key).ok().into_iter())
        .for_each(|facet| {
            let facet_term = Term::from_facet(schema.field, &facet);
            let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
            fuzzies.push((Occur::Must, Box::new(facet_term_query.clone())));
            originals.push((Occur::Must, Box::new(facet_term_query)));
        });

    // Filters
    request
        .filter
        .iter()
        .flat_map(|f| f.field_labels.iter())
        .flat_map(|facet_key| Facet::from_text(facet_key).ok().into_iter())
        .for_each(|facet| {
            let facet_term = Term::from_facet(schema.facets, &facet);
            let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
            fuzzies.push((Occur::Must, Box::new(facet_term_query.clone())));
            originals.push((Occur::Must, Box::new(facet_term_query)));
        });

    if originals.len() == 1 && originals[0].1.is::<AllQuery>() {
        let original = originals.pop().unwrap().1;
        let fuzzy = Box::new(BooleanQuery::new(vec![]));
        (original, termc, fuzzy)
    } else {
        if processed.fuzzy_query.is_empty() {
            fuzzies.clear();
        }
        let original = Box::new(BooleanQuery::new(originals));
        let fuzzied = Box::new(BoostQuery::new(Box::new(BooleanQuery::new(fuzzies)), 0.5));
        (original, termc, fuzzied)
    }
}

pub fn search_query(
    parser: &QueryParser,
    text: &str,
    search: &ParagraphSearchRequest,
    schema: &ParagraphSchema,
    distance: u8,
    with_advance: Option<Box<dyn Query>>,
) -> (Box<dyn Query>, SharedTermC, Box<dyn Query>) {
    let mut term_collector = TermCollector::default();
    let processed = preprocess_raw_query(text, &mut term_collector);
    let query = parse_query(parser, &processed.regular_query);
    let fuzzy_query = parse_query(parser, &processed.fuzzy_query);
    let termc = SharedTermC::from(term_collector);
    let mut fuzzies = fuzzied_queries(fuzzy_query, false, distance, termc.clone());
    let mut originals = vec![(Occur::Must, query)];
    if let Some(advance) = with_advance {
        originals.push((Occur::Must, advance.box_clone()));
        fuzzies.push((Occur::Must, advance));
    }
    if !search.uuid.is_empty() {
        let term = Term::from_field_text(schema.uuid, &search.uuid);
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        fuzzies.push((Occur::Must, Box::new(term_query.clone())));
        originals.push((Occur::Must, Box::new(term_query)))
    }
    if !search.with_duplicates {
        let term = Term::from_field_u64(schema.repeated_in_field, 0);
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        fuzzies.push((Occur::Must, Box::new(term_query.clone())));
        originals.push((Occur::Must, Box::new(term_query)))
    }
    if let Some(time_ranges) = search.timestamps.as_ref() {
        let modified = produce_date_range_query(
            schema.modified,
            time_ranges.from_modified.clone(),
            time_ranges.to_modified.clone(),
        );
        let created = produce_date_range_query(
            schema.created,
            time_ranges.from_created.clone(),
            time_ranges.to_created.clone(),
        );

        if let Some(modified) = modified {
            fuzzies.push((Occur::Must, Box::new(modified.clone())));
            originals.push((Occur::Must, Box::new(modified)));
        }

        if let Some(created) = created {
            fuzzies.push((Occur::Must, Box::new(created.clone())));
            originals.push((Occur::Must, Box::new(created)));
        }
    }
    // Fields
    let mut field_filter: Vec<(Occur, Box<dyn Query>)> = vec![];
    search
        .fields
        .iter()
        .map(|value| format!("/{value}"))
        .flat_map(|facet_key| Facet::from_text(&facet_key).ok().into_iter())
        .for_each(|facet| {
            let facet_term = Term::from_facet(schema.field, &facet);
            let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
            field_filter.push((Occur::Should, Box::new(facet_term_query)));
        });
    if !field_filter.is_empty() {
        let field_filter = Box::new(BooleanQuery::new(field_filter));
        fuzzies.push((Occur::Must, field_filter.clone()));
        originals.push((Occur::Must, field_filter));
    }
    // Label filters
    let mut label_filters: Vec<Box<dyn Query>> = vec![];
    search
        .filter
        .iter()
        .flat_map(|f| f.field_labels.iter())
        .flat_map(|facet_key| Facet::from_text(facet_key).ok().into_iter())
        .for_each(|facet| {
            let facet_term = Term::from_facet(schema.facets, &facet);
            let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
            label_filters.push(Box::new(facet_term_query));
        });
    search
        .filter
        .iter()
        .flat_map(|f| f.paragraph_labels.iter())
        .flat_map(|facet_key| Facet::from_text(facet_key).ok().into_iter())
        .for_each(|facet| {
            let facet_term = Term::from_facet(schema.facets, &facet);
            let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
            label_filters.push(Box::new(facet_term_query));
        });
    if !label_filters.is_empty() {
        let label_filters_query = Box::new(BooleanQuery::intersection(label_filters));
        fuzzies.push((Occur::Must, label_filters_query.clone()));
        originals.push((Occur::Must, label_filters_query));
    }

    // Keys filter
    let mut key_filters: Vec<Box<dyn Query>> = vec![];
    search.key_filters.iter().for_each(|key| {
        let mut key_filter: Vec<Box<dyn Query>> = vec![];

        let parts: Vec<String> = key.split('/').map(str::to_string).collect();
        let uuid = &parts[0];
        let term = Term::from_field_text(schema.uuid, uuid);
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        key_filter.push(Box::new(term_query));

        if parts.len() >= 3 {
            let mut field_key: String = "/".to_owned();
            field_key.push_str(&parts[1..3].join("/"));
            let facet = Facet::from_text(&field_key).unwrap();
            let facet_term = Term::from_facet(schema.field, &facet);
            let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
            key_filter.push(Box::new(facet_term_query));
        }

        let key_filter_query = Box::new(BooleanQuery::intersection(key_filter));
        key_filters.push(key_filter_query);
    });
    if !key_filters.is_empty() {
        let key_filters_query = Box::new(BooleanQuery::union(key_filters));
        fuzzies.push((Occur::Must, key_filters_query.clone()));
        originals.push((Occur::Must, key_filters_query));
    }

    if originals.len() == 1 && originals[0].1.is::<AllQuery>() {
        let original = originals.pop().unwrap().1;
        let fuzzy = Box::new(BooleanQuery::new(vec![]));
        (original, termc, fuzzy)
    } else {
        if processed.fuzzy_query.is_empty() {
            fuzzies.clear();
        }
        let original = Box::new(BooleanQuery::new(originals));
        let fuzzied = Box::new(BoostQuery::new(Box::new(BooleanQuery::new(fuzzies)), 0.5));
        (original, termc, fuzzied)
    }
}

pub fn streaming_query(schema: &ParagraphSchema, request: &StreamRequest) -> Box<dyn Query> {
    let mut queries: Vec<(Occur, Box<dyn Query>)> = vec![];
    queries.push((Occur::Must, Box::new(AllQuery)));
    request
        .filter
        .iter()
        .flat_map(|f| f.labels.iter())
        .flat_map(|facet_key| Facet::from_text(facet_key).ok().into_iter())
        .for_each(|facet| {
            let facet_term = Term::from_facet(schema.facets, &facet);
            let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
            queries.push((Occur::Must, Box::new(facet_term_query)));
        });
    Box::new(BooleanQuery::new(queries))
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
    fn test_preprocessor() {
        let text = "own test \"This is great\"";
        let mut term_collector = TermCollector::default();
        let _ = preprocess_raw_query(text, &mut term_collector);
        let terms: HashSet<_> = term_collector.eterms.iter().map(|s| s.as_str()).collect();
        let expect = HashSet::from(["This is great", "test"]);
        assert_eq!(terms, expect);

        let text = "The test \"is correct\" always";
        let mut term_collector = TermCollector::default();
        let processed = preprocess_raw_query(text, &mut term_collector);
        let terms: HashSet<_> = term_collector.eterms.iter().map(|s| s.as_str()).collect();
        let expect = HashSet::from(["test", "always", "is correct"]);
        assert_eq!(terms, expect);
        assert_eq!(processed.regular_query, "test \"is correct\" always");
        assert_eq!(processed.fuzzy_query, "test always");
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
        let adapted = flat_and_adapt(Box::new(nested), true, 2, SharedTermC::new());
        assert_eq!(adapted.len(), 24);
        assert!(adapted.iter().all(|(occur, _)| *occur == Occur::Must));
        assert!(adapted
            .iter()
            .all(|(_, query)| query.is::<FuzzyTermQuery>()));
    }

    #[test]
    fn it_removes_stop_word_fterms() {
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
            ("is a for and", "and"),
            ("what does stop is?", "stop is?"),
            ("", ""),
            (
                "comment s'appelle le train à grande vitesse",
                "comment s'appelle train grande vitesse",
            ),
            (
                "¿Qué significa la palabra sentence en español?",
                "¿Qué significa palabra sentence español?",
            ),
            (
                "Per què les vaques no són de color rosa?",
                "vaques color rosa?",
            ),
            (
                "How can I learn to make a flat white?",
                "learn make flat white?",
            ),
            ("Qué es escalada en bloque?", "escalada bloque?"),
            (
                "Wer hat gesagt: 'Kaffeetrinken ist integraler Bestandteil des Kletterns'?",
                "Wer gesagt: 'Kaffeetrinken integraler Bestandteil Kletterns'?",
            ),
            (
                "i pistacchi siciliani sono i migliori al mondo?",
                "pistacchi siciliani migliori mondo?",
            ),
        ];

        for (query, expected_fuzzy_query) in tests {
            let fuzzy_query = remove_stop_words(query);
            assert_eq!(fuzzy_query, expected_fuzzy_query);
        }
    }
}
