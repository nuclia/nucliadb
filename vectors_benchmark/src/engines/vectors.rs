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

use nucliadb_vectors::data_point::{DataPoint, Elem, LabelDictionary};
use nucliadb_vectors::data_point_provider::*;

use super::VectorEngine;

struct Request<'a>(usize, &'a [f32]);
impl<'a> SearchRequest for Request<'a> {
    fn with_duplicates(&self) -> bool {
        true
    }
    fn get_query(&self) -> &[f32] {
        self.1
    }

    fn get_labels(&self) -> &[String] {
        &[]
    }

    fn no_results(&self) -> usize {
        self.0
    }
}
impl VectorEngine for Index {
    fn add_batch(&mut self, batch_id: String, keys: Vec<String>, embeddings: Vec<Vec<f32>>) {
        let temporal_mark = TemporalMark::now();
        let mut elems = vec![];
        for (key, vector) in keys.into_iter().zip(embeddings.into_iter()) {
            let elem = Elem::new(key, vector, LabelDictionary::new(vec![]));
            elems.push(elem);
        }
        let new_dp = DataPoint::new(self.get_location(), elems, Some(temporal_mark)).unwrap();
        let lock = self.get_elock().unwrap();
        self.add(new_dp, &lock);
        self.delete(&batch_id, temporal_mark, &lock);
        self.commit(lock).unwrap();
    }

    fn search(&self, no_results: usize, query: &[f32]) {
        let lock = self.get_slock().unwrap();
        self.search(&Request(no_results, query), &lock).unwrap();
    }
}
