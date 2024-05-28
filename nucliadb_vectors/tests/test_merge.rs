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

use std::time::{Duration, SystemTime};

use nucliadb_core::{vectors::MergeParameters, NodeResult};
use nucliadb_vectors::{
    config::VectorConfig,
    data_point::{self, DataPointPin, Elem, LabelDictionary},
    data_point_provider::{reader::Reader, writer::Writer},
};
use tempfile::tempdir;

fn elem(index: usize) -> Elem {
    let mut vector: Vec<f32> = [0.0; 100].into();
    vector[index] = 1.0;

    Elem::new(format!("key_{index}"), vector, LabelDictionary::default(), None)
}

#[test]
fn test_concurrent_merge_delete() -> NodeResult<()> {
    let workdir = tempdir()?;
    let index_path = workdir.path().join("vectors");
    let config = VectorConfig::default();
    let mut writer = Writer::new(&index_path, config.clone(), "abc".into())?;

    let now = SystemTime::now();
    let past = now - Duration::from_secs(5);

    // Create two segments
    let data_point_pin = DataPointPin::create_pin(&index_path)?;
    data_point::create(&data_point_pin, vec![elem(0)], Some(past), &config)?;
    writer.add_data_point(data_point_pin)?;
    writer.commit()?;

    let data_point_pin = DataPointPin::create_pin(&index_path)?;
    data_point::create(&data_point_pin, vec![elem(1)], Some(past), &config)?;
    writer.add_data_point(data_point_pin)?;
    writer.commit()?;

    // Merge the two segments and simultaneously remove something
    let to_delete = elem(0);
    let mut merge = writer
        .prepare_merge(MergeParameters {
            max_nodes_in_merge: 1000,
            segments_before_merge: 2,
            maximum_deleted_entries: 25_000,
        })?
        .unwrap();

    // This can be thought as concurrent, since `merge.run()` does not touch the index writer
    writer.record_delete(&to_delete.key, SystemTime::now());
    writer.commit()?;
    let results = merge.run()?;

    writer.record_merge(results.as_ref())?;

    // Finally, check that the deleted element cannot be found
    let reader = Reader::open(&index_path)?;
    let keys = reader.keys()?;
    assert_eq!(keys, vec!["key_1"]);

    Ok(())
}
