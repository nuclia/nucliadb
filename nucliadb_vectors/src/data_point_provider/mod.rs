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

pub mod garbage_collector;
pub mod reader;
pub mod writer;

pub mod replication;
mod state;

use crate::config::VectorConfig;
use crate::data_types::dtrie_ram::DTrie;
use crate::data_types::DeleteLog;
use crate::formula::Formula;
use crate::VectorR;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::time::SystemTime;

pub use crate::data_point::Neighbour;

const METADATA: &str = "metadata.json";
const STATE: &str = "state.bincode";
const TEMP_STATE: &str = "temp_state.bincode";
const OPENING_FLAG: &str = ".opening";
const WRITING_FLAG: &str = ".writing";

pub trait SearchRequest {
    fn get_query(&self) -> &[f32];
    fn get_filter(&self) -> &Formula;
    fn no_results(&self) -> usize;
    fn with_duplicates(&self) -> bool;
    fn min_score(&self) -> f32;
}

#[derive(Clone, Copy)]
struct TimeSensitiveDLog<'a> {
    dlog: &'a DTrie,
    time: SystemTime,
}
impl<'a> DeleteLog for TimeSensitiveDLog<'a> {
    fn is_deleted(&self, key: &[u8]) -> bool {
        self.dlog.get(key).map(|t| t > self.time).unwrap_or_default()
    }
}

struct IndexMetadata;
impl IndexMetadata {
    pub fn write(config: &VectorConfig, path: &Path) -> VectorR<()> {
        let mut writer = BufWriter::new(File::create(path.join(METADATA))?);
        serde_json::to_writer(&mut writer, config)?;
        Ok(writer.flush()?)
    }
    pub fn open(path: &Path) -> VectorR<Option<VectorConfig>> {
        let path = &path.join(METADATA);
        if !path.is_file() {
            return Ok(None);
        }
        let mut reader = BufReader::new(File::open(path)?);
        Ok(Some(serde_json::from_reader(&mut reader)?))
    }
}
