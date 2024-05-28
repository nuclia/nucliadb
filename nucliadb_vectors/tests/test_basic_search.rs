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

use nucliadb_core::NodeResult;
use nucliadb_vectors::config::VectorType;
use nucliadb_vectors::data_point_provider::SearchRequest;
use nucliadb_vectors::formula::Formula;
use nucliadb_vectors::{
    config::{Similarity, VectorConfig},
    data_point::{self, DataPointPin, Elem, LabelDictionary},
    data_point_provider::{reader::Reader, writer::Writer},
};
use rstest::rstest;
use tempfile::tempdir;

fn elem(index: usize) -> Elem {
    let mut vector: Vec<f32> = [0.0; DIMENSION].into();
    vector[index] = 1.0;

    Elem::new(format!("key_{index}"), vector, LabelDictionary::default(), None)
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
struct Request {
    vector: Vec<f32>,
    formula: Formula,
}

impl SearchRequest for Request {
    fn with_duplicates(&self) -> bool {
        false
    }
    fn get_query(&self) -> &[f32] {
        &self.vector
    }

    fn get_filter(&self) -> &Formula {
        &self.formula
    }

    fn no_results(&self) -> usize {
        10
    }
    fn min_score(&self) -> f32 {
        -1.0
    }
}

const DIMENSION: usize = 128;

#[rstest]
fn test_basic_search(
    #[values(Similarity::Dot, Similarity::Cosine)] similarity: Similarity,
    #[values(VectorType::DenseF32Unaligned, VectorType::DenseF32 { dimension: DIMENSION })] vector_type: VectorType,
) -> NodeResult<()> {
    let workdir = tempdir()?;
    let index_path = workdir.path().join("vectors");
    let config = VectorConfig {
        similarity,
        vector_type,
        ..Default::default()
    };

    // Write some data
    let mut writer = Writer::new(&index_path, config.clone(), "abc".into())?;
    let data_point_pin = DataPointPin::create_pin(&index_path)?;
    data_point::create(&data_point_pin, (0..DIMENSION).map(elem).collect(), None, &config)?;
    writer.add_data_point(data_point_pin)?;
    writer.commit()?;

    // Search for a specific element
    let reader = Reader::open(&index_path)?;
    let search_for = elem(5);
    let results = reader.search(&Request {
        vector: search_for.vector,
        formula: Formula::new(),
    })?;
    assert_eq!(results.len(), 10);
    assert_eq!(results[0].id(), search_for.key);
    assert_eq!(results[0].score(), 1.0);
    assert_eq!(results[1].score(), 0.0);

    // Search near a few elements
    let mut vector: Vec<f32> = [0.0; DIMENSION].into();
    vector[42] = 0.7;
    vector[43] = 0.6;
    vector[44] = 0.5;
    vector[45] = 0.4;
    let reader = Reader::open(&index_path)?;
    let results = reader.search(&Request {
        vector,
        formula: Formula::new(),
    })?;
    assert_eq!(results.len(), 10);
    assert_eq!(results[0].id(), elem(42).key);
    assert!(results[0].score() > 0.2);
    assert_eq!(results[1].id(), elem(43).key);
    assert!(results[1].score() > 0.2);
    assert_eq!(results[2].id(), elem(44).key);
    assert!(results[2].score() > 0.2);
    assert_eq!(results[3].id(), elem(45).key);
    assert!(results[3].score() > 0.2);
    assert_eq!(results[5].score(), 0.0);

    Ok(())
}
