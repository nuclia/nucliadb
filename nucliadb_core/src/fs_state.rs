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

use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::time::SystemTime;

use fs2::FileExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

pub type FsResult<O> = std::result::Result<O, FsError>;

#[derive(Debug, Error)]
pub enum FsError {
    #[error("Serialization error: {0}")]
    ParsingError(#[from] bincode::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

mod names {
    pub const STATE: &str = "state.bincode";
    pub const TEMP: &str = "temp_state.bincode";
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version(SystemTime);

fn write_state<S>(path: &Path, state: &S) -> FsResult<()>
where S: Serialize {
    let temporal_path = path.join(names::TEMP);
    let state_path = path.join(names::STATE);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temporal_path)?;

    file.lock_exclusive()?;
    let mut file_buf = BufWriter::new(&mut file);
    bincode::serialize_into(&mut file_buf, state)?;
    file_buf.flush()?;
    std::fs::rename(&temporal_path, state_path)?;
    std::mem::drop(file_buf);
    file.unlock()?;

    Ok(())
}

fn read_state<S>(path: &Path) -> FsResult<(Version, S)>
where S: DeserializeOwned {
    let file = OpenOptions::new()
        .read(true)
        .open(path.join(names::STATE))?;
    file.lock_shared()?;
    let modified = file.metadata()?.modified()?;
    let mut buff = BufReader::new(&file);
    let state: S = bincode::deserialize_from(&mut buff)?;
    file.unlock()?;
    Ok((Version(modified), state))
}

pub fn initialize_disk<S, F>(path: &Path, with: F) -> FsResult<()>
where
    F: Fn() -> S,
    S: Serialize,
{
    if !path.join(names::STATE).is_file() {
        write_state(path, &with())?;
    }
    Ok(())
}

pub fn atomic_write<S>(path: &Path, state: &S) -> FsResult<()>
where S: Serialize {
    write_state(path, state)
}

pub fn load_state<S>(path: &Path) -> FsResult<(Version, S)>
where S: DeserializeOwned {
    read_state(path)
}
pub fn crnt_version(path: &Path) -> FsResult<Version> {
    let meta = std::fs::metadata(path.join(names::STATE))?;
    Ok(Version(meta.modified()?))
}
