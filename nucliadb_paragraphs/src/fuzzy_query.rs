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

use std::collections::HashMap;
use std::io;
use std::ops::Range;
use std::sync::Arc;

use levenshtein_automata::{Distance, LevenshteinAutomatonBuilder, DFA};
use once_cell::sync::Lazy;
use tantivy::query::{BitSetDocSet, ConstScorer, Explanation, Query, Scorer, Weight};
use tantivy::schema::{Field, IndexRecordOption, Term};
use tantivy::termdict::{TermDictionary, TermStreamer};
use tantivy::TantivyError::InvalidArgument;
use tantivy::{DocId, DocSet, Score, Searcher, SegmentReader, TantivyError};
use tantivy_common::BitSet;
use tantivy_fst::Automaton;

use crate::search_query::SharedTermC;

/// A weight struct for Fuzzy Term and Regex Queries
pub struct AutomatonWeight<A> {
    terms: SharedTermC,
    field: Field,
    automaton: Arc<A>,
}

impl<A> AutomatonWeight<A>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
{
    /// Create a new AutomationWeight
    pub fn new<IntoArcA: Into<Arc<A>>>(
        field: Field,
        automaton: IntoArcA,
        terms: SharedTermC,
    ) -> AutomatonWeight<A> {
        AutomatonWeight {
            field,
            terms,
            automaton: automaton.into(),
        }
    }

    fn automaton_stream<'a>(
        &'a self,
        term_dict: &'a TermDictionary,
    ) -> io::Result<TermStreamer<'a, &'a A>> {
        let automaton: &A = &self.automaton;
        let term_stream_builder = term_dict.search(automaton);
        term_stream_builder.into_stream()
    }
}

impl<A> Weight for AutomatonWeight<A>
where
    A: Automaton + Send + Sync + 'static,
    A::State: Clone,
{
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);
        let inverted_index = reader.inverted_index(self.field)?;
        let term_dict = inverted_index.terms();
        let mut termc = self.terms.get_termc();
        let mut term_stream = self.automaton_stream(term_dict)?;
        while term_stream.advance() {
            let term_key = term_stream.term_ord();
            let term_info = term_stream.value();
            let mut block_segment_postings = inverted_index
                .read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
            loop {
                let docs = block_segment_postings.docs();
                if docs.is_empty() {
                    break;
                }
                for &doc in docs {
                    termc.log_fterm(doc, (inverted_index.clone(), term_key));
                    doc_bitset.insert(doc);
                }
                block_segment_postings.advance();
            }
        }
        self.terms.set_termc(termc);
        let doc_bitset = BitSetDocSet::from(doc_bitset);
        let const_scorer = ConstScorer::new(doc_bitset, boost);
        Ok(Box::new(const_scorer))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) == doc {
            Ok(Explanation::new("AutomatonScorer", 1.0))
        } else {
            Err(TantivyError::InvalidArgument(
                "Document does not exist".to_string(),
            ))
        }
    }
}

pub(crate) struct DfaWrapper(pub DFA);

impl Automaton for DfaWrapper {
    type State = u32;

    fn start(&self) -> Self::State {
        self.0.initial_state()
    }

    fn is_match(&self, state: &Self::State) -> bool {
        match self.0.distance(*state) {
            Distance::Exact(_) => true,
            Distance::AtLeast(_) => false,
        }
    }

    fn can_match(&self, state: &u32) -> bool {
        *state != levenshtein_automata::SINK_STATE
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.0.transition(*state, byte)
    }
}

/// A range of Levenshtein distances that we will build DFAs for our terms
/// The computation is exponential, so best keep it to low single digits
const VALID_LEVENSHTEIN_DISTANCE_RANGE: Range<u8> = 0..3;

static LEV_BUILDER: Lazy<HashMap<(u8, bool), LevenshteinAutomatonBuilder>> = Lazy::new(|| {
    let mut lev_builder_cache = HashMap::new();
    // TODO make population lazy on a `(distance, val)` basis
    for distance in VALID_LEVENSHTEIN_DISTANCE_RANGE {
        for &transposition in &[false, true] {
            let lev_automaton_builder = LevenshteinAutomatonBuilder::new(distance, transposition);
            lev_builder_cache.insert((distance, transposition), lev_automaton_builder);
        }
    }
    lev_builder_cache
});

#[derive(Clone)]
pub struct FuzzyTermQuery {
    termc: SharedTermC,
    /// What term are we searching
    term: Term,
    /// How many changes are we going to allow
    distance: u8,
    /// Should a transposition cost 1 or 2?
    transposition_cost_one: bool,
    ///
    prefix: bool,
}

impl std::fmt::Debug for FuzzyTermQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Fuzzy")
    }
}
impl FuzzyTermQuery {
    /// Creates a new Fuzzy Query
    pub fn new(
        term: Term,
        distance: u8,
        transposition_cost_one: bool,
        termc: SharedTermC,
    ) -> FuzzyTermQuery {
        FuzzyTermQuery {
            term,
            termc,
            distance,
            transposition_cost_one,
            prefix: false,
        }
    }

    /// Creates a new Fuzzy Query of the Term prefix
    pub fn new_prefix(
        term: Term,
        distance: u8,
        transposition_cost_one: bool,
        termc: SharedTermC,
    ) -> FuzzyTermQuery {
        FuzzyTermQuery {
            term,
            termc,
            distance,
            transposition_cost_one,
            prefix: true,
        }
    }

    fn specialized_weight(&self) -> tantivy::Result<AutomatonWeight<DfaWrapper>> {
        // LEV_BUILDER is a HashMap, whose `get` method returns an Option
        match LEV_BUILDER.get(&(self.distance, self.transposition_cost_one)) {
            // Unwrap the option and build the Ok(AutomatonWeight)
            Some(automaton_builder) => {
                let term_text = self.term.as_str().ok_or_else(|| {
                    tantivy::TantivyError::InvalidArgument(
                        "The fuzzy term query requires a string term.".to_string(),
                    )
                })?;
                let automaton = if self.prefix {
                    automaton_builder.build_prefix_dfa(term_text)
                } else {
                    automaton_builder.build_dfa(term_text)
                };
                Ok(AutomatonWeight::new(
                    self.term.field(),
                    DfaWrapper(automaton),
                    self.termc.clone(),
                ))
            }
            None => Err(InvalidArgument(format!(
                "Levenshtein distance of {} is not allowed. Choose a value in the {:?} range",
                self.distance, VALID_LEVENSHTEIN_DISTANCE_RANGE
            ))),
        }
    }
}

impl Query for FuzzyTermQuery {
    fn weight(
        &self,
        _searcher: &Searcher,
        _scoring_enabled: bool,
    ) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight()?))
    }
}
