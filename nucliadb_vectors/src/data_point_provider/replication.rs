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

use std::{
    collections::HashMap,
    fs::{self, File},
    io::Cursor,
    path::Path,
};

use nucliadb_core::RawReplicaState;

use crate::VectorR;

use super::state::{read_state, State};

pub(crate) fn get_segment_ids(location: &Path) -> VectorR<Vec<String>> {
    let state = read_state(File::open(location.join("state.bincode"))?)?;
    Ok(segment_ids(&state))
}

fn segment_ids(state: &State) -> Vec<String> {
    state.data_point_list.iter().map(|d| d.to_string()).collect()
}

pub(crate) fn get_index_files(
    location: &Path,
    relative_path: &str,
    ignored_segment_ids: &[String],
) -> VectorR<RawReplicaState> {
    let mut metadata_files = HashMap::new();
    let state_data = fs::read(location.join("state.bincode"))?;
    let state = read_state(Cursor::new(&state_data))?;
    metadata_files.insert(format!("{relative_path}/state.bincode"), state_data);
    metadata_files.insert(format!("{relative_path}/metadata.json"), fs::read(location.join("metadata.json"))?);

    let mut files = Vec::new();
    for segment_id in segment_ids(&state) {
        if ignored_segment_ids.contains(&segment_id) {
            continue;
        }
        files.push((
            format!("{relative_path}/{segment_id}/index.hnsw"),
            File::open(location.join(format!("{segment_id}/index.hnsw")))?,
        ));
        files.push((
            format!("{relative_path}/{segment_id}/journal.json"),
            File::open(location.join(format!("{segment_id}/journal.json")))?,
        ));
        files.push((
            format!("{relative_path}/{segment_id}/nodes.kv"),
            File::open(location.join(format!("{segment_id}/nodes.kv")))?,
        ));
    }

    Ok(RawReplicaState {
        metadata_files,
        files,
    })
}
