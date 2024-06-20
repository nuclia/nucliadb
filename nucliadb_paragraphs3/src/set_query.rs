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

use std::collections::HashSet;
use std::sync::Arc;

use tantivy::{DocId, DocSet, Score, Searcher, SegmentReader};
use tantivy_common::BitSet;

use tantivy::query::{BitSetDocSet, ConstScorer, Explanation, Query, Scorer, Weight};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::TantivyError;

/// A Term Set Query matches all of the documents containing any of the Term provided
#[derive(Debug, Clone)]
pub struct SetQuery {
    set: SetWightWrapper,
}

impl SetQuery {
    /// Create a Term Set Query
    pub fn new(field: Field, values: Vec<String>) -> Self {
        let values = values.into_iter().collect();
        let set = SetWightWrapper::new(SetWeight {
            field,
            values,
        });

        SetQuery {
            set,
        }
    }
}

impl Query for SetQuery {
    fn weight(&self, _searcher: &Searcher, _scoring_enabled: bool) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(self.set.clone()))
    }
}

#[derive(Clone, Debug)]
struct SetWightWrapper(Arc<SetWeight>);

impl SetWightWrapper {
    fn new(v: SetWeight) -> Self {
        Self(Arc::new(v))
    }
}

#[derive(Debug)]
struct SetWeight {
    field: Field,
    values: HashSet<String>,
}

impl Weight for SetWightWrapper {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);
        let inverted_index = reader.inverted_index(self.0.field)?;
        let term_dict = inverted_index.terms();
        let mut term_stream = term_dict.stream()?;
        while term_stream.advance() {
            let k = String::from_utf8_lossy(term_stream.key());
            if !self.0.values.contains(k.as_ref()) {
                continue;
            };

            let term_info = term_stream.value();
            let mut block_segment_postings =
                inverted_index.read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
            loop {
                let docs = block_segment_postings.docs();
                if docs.is_empty() {
                    break;
                }
                for &doc in docs {
                    doc_bitset.insert(doc);
                }
                block_segment_postings.advance();
            }
        }
        let doc_bitset = BitSetDocSet::from(doc_bitset);
        let const_scorer = ConstScorer::new(doc_bitset, boost);
        Ok(Box::new(const_scorer))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) == doc {
            Ok(Explanation::new("SetScorer", 1.0))
        } else {
            Err(TantivyError::InvalidArgument("Document does not exist".to_string()))
        }
    }
}
