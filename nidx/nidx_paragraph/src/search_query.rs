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
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tantivy::index::Index;
use tantivy::query::*;
use tantivy::schema::{Facet, IndexRecordOption};
use tantivy::{DocId, InvertedIndexReader, Term};

use nidx_protos::StreamRequest;
use nidx_types::prefilter::PrefilterResult;
use nidx_types::query_language::BooleanExpression;

use crate::query_io::translate_expression;
use crate::query_parser::{ParsedQuery, Parser};
use crate::request_types::{ParagraphSearchRequest, ParagraphSuggestRequest};
use crate::schema::ParagraphSchema;
use crate::set_query::SetQuery;

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
        self.fterms.entry(doc).or_default().push(data);
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
    pub fn get_termc(&self) -> TermCollector {
        std::mem::take(&mut self.0.lock().unwrap())
    }
    pub fn set_termc(&self, termc: TermCollector) {
        *self.0.lock().unwrap() = termc;
    }
}

fn filter_query(
    schema: &ParagraphSchema,
    prefilter: &PrefilterResult,
    paragraph_formula: &Option<BooleanExpression<String>>,
    filter_or: bool,
) -> Option<Box<dyn Query>> {
    let mut filter_terms = vec![];
    let operator = if filter_or { Occur::Should } else { Occur::Must };

    // Paragraph filter
    if let Some(formula) = &paragraph_formula {
        let query = translate_expression(formula, schema);
        filter_terms.push((operator, query));
    }

    // Prefilter
    if let PrefilterResult::Some(field_keys) = prefilter {
        let set_query = Box::new(SetQuery::new(
            schema.field_uuid,
            field_keys
                .iter()
                .map(|x| format!("{}{}", x.resource_id.simple(), x.field_id)),
        ));
        filter_terms.push((operator, set_query));
    }

    if !filter_terms.is_empty() {
        Some(Box::new(BooleanQuery::new(filter_terms)))
    } else {
        None
    }
}

pub fn suggest_query(
    request: &ParagraphSuggestRequest,
    prefilter: &PrefilterResult,
    schema: &ParagraphSchema,
) -> (Box<dyn Query>, SharedTermC, Box<dyn Query>) {
    let query = &request.body;
    let parser = Parser::new_with_schema(schema).last_fuzzy_term_as_prefix();
    let ParsedQuery {
        keyword,
        fuzzy,
        term_collector,
    } = parser.parse(query);

    let mut originals = vec![(Occur::Must, keyword)];
    let mut fuzzies = vec![(Occur::Must, fuzzy)];
    let term = Term::from_field_u64(schema.repeated_in_field, 0);
    let term_query = TermQuery::new(term, IndexRecordOption::Basic);
    fuzzies.push((Occur::Must, Box::new(term_query.clone())));
    originals.push((Occur::Must, Box::new(term_query)));

    let filter_query = filter_query(schema, prefilter, &request.filtering_formula, request.filter_or);
    if let Some(query) = filter_query {
        originals.push((Occur::Must, query.box_clone()));
        fuzzies.push((Occur::Must, query));
    }

    if originals.len() == 1 && originals[0].1.is::<AllQuery>() {
        let original = originals.pop().unwrap().1;
        let fuzzy = Box::new(BooleanQuery::new(vec![]));
        (original, term_collector, fuzzy)
    } else {
        let original = Box::new(BooleanQuery::new(originals));
        let fuzzied = Box::new(BoostQuery::new(Box::new(BooleanQuery::new(fuzzies)), 0.5));
        (original, term_collector, fuzzied)
    }
}

pub fn search_query(
    request: &ParagraphSearchRequest,
    prefilter: &PrefilterResult,
    index: &Index,
    schema: &ParagraphSchema,
) -> (Box<dyn Query>, SharedTermC, Box<dyn Query>) {
    let query = &request.body;
    let parser = Parser::new_with_schema(schema).last_fuzzy_term_as_prefix();
    let ParsedQuery {
        keyword,
        fuzzy,
        term_collector,
    } = parser.parse(query);

    let mut keyword_subqueries = vec![(Occur::Must, keyword)];
    let mut fuzzy_subqueries = vec![(Occur::Must, fuzzy)];

    if let Some(advanced_query) = &request.advanced_query {
        // The advanced query allow users to leverage tantivy grammar in our system. We are not
        // interested in failing here, so we use the lenient parser and ignore errors
        let parser = QueryParser::for_index(index, vec![schema.text]);
        let (advanced_query, _errors) = parser.parse_query_lenient(advanced_query);

        keyword_subqueries.push((Occur::Must, advanced_query.box_clone()));
        fuzzy_subqueries.push((Occur::Must, advanced_query));
    }

    let filter_query = filter_query(schema, prefilter, &request.filtering_formula, request.filter_or);
    if let Some(query) = filter_query {
        keyword_subqueries.push((Occur::Must, query.box_clone()));
        fuzzy_subqueries.push((Occur::Must, query));
    }

    if !request.with_duplicates {
        let term = Term::from_field_u64(schema.repeated_in_field, 0);
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        fuzzy_subqueries.push((Occur::Must, Box::new(term_query.clone())));
        keyword_subqueries.push((Occur::Must, Box::new(term_query)))
    }

    let keyword = if keyword_subqueries.len() == 1 {
        keyword_subqueries.pop().unwrap().1
    } else {
        // we don't check empty as there's always 1 element
        Box::new(BooleanQuery::new(keyword_subqueries))
    };
    let fuzzy = if fuzzy_subqueries.len() == 1 {
        fuzzy_subqueries.pop().unwrap().1
    } else {
        // we don't check empty as there's always 1 element
        Box::new(BoostQuery::new(
            Box::new(BooleanQuery::new(fuzzy_subqueries)),
            // REVIEW: arbitrary fuzzy boost, why do we have this value?
            0.5,
        ))
    };

    (keyword, term_collector, fuzzy)
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
