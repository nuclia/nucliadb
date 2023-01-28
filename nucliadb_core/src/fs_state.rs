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

use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
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
    pub const LOCK: &str = "lk.lock";
    pub const STATE: &str = "state.bincode";
    pub const TEMP: &str = "temp_state.bincode";
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version(SystemTime);

fn write_state<S>(path: &Path, state: &S) -> FsResult<()>
where S: Serialize {
    let temporal_path = path.join(names::TEMP);
    let state_path = path.join(names::STATE);
    let mut file = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temporal_path)?,
    );
    bincode::serialize_into(&mut file, state)?;
    file.flush()?;
    std::fs::rename(&temporal_path, state_path)?;
    Ok(())
}

fn read_state<S>(path: &Path) -> FsResult<S>
where S: DeserializeOwned {
    let mut file = BufReader::new(
        OpenOptions::new()
            .read(true)
            .open(path.join(names::STATE))?,
    );
    Ok(bincode::deserialize_from(&mut file)?)
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

pub fn exclusive_lock(path: &Path) -> FsResult<ELock> {
    Ok(ELock::new(path)?)
}
pub fn shared_lock(path: &Path) -> FsResult<SLock> {
    Ok(SLock::new(path)?)
}

pub fn persist_state<S>(lock: &ELock, state: &S) -> FsResult<()>
where S: Serialize {
    write_state(lock.as_ref(), state)
}

pub fn load_state<S>(lock: &Lock) -> FsResult<S>
where S: DeserializeOwned {
    read_state(lock.as_ref())
}
pub fn crnt_version(lock: &Lock) -> FsResult<Version> {
    let meta = std::fs::metadata(lock.path.join(names::STATE))?;
    Ok(Version(meta.modified()?))
}

/// A Lock that may be exclusive or shared
/// Useful when the code would work in either case.
pub struct Lock {
    path: PathBuf,
    #[allow(unused)]
    lock: File,
}
impl Lock {
    fn open_lock(path: &Path) -> io::Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join(names::LOCK))?;
        Ok(file)
    }
    fn exclusive(path: &Path) -> io::Result<Lock> {
        let path = path.to_path_buf();
        let lock = Lock::open_lock(&path)?;
        lock.lock_exclusive()?;
        Ok(Lock { lock, path })
    }
    fn shared(path: &Path) -> io::Result<Lock> {
        let path = path.to_path_buf();
        let lock = Lock::open_lock(&path)?;
        lock.lock_shared()?;
        Ok(Lock { lock, path })
    }
}
impl AsRef<Path> for Lock {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

/// A exclusive lock
pub struct ELock(Lock);
impl ELock {
    pub(super) fn new(path: &Path) -> io::Result<ELock> {
        Lock::exclusive(path).map(ELock)
    }
}
impl std::ops::Deref for ELock {
    type Target = Lock;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl AsRef<Path> for ELock {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

/// A shared lock
pub struct SLock(Lock);
impl SLock {
    pub fn new(path: &Path) -> io::Result<SLock> {
        Lock::shared(path).map(SLock)
    }
}
impl std::ops::Deref for SLock {
    type Target = Lock;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl AsRef<Path> for SLock {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[derive(Serialize, serde::Deserialize, Default)]
    struct State {
        n: usize,
    }

    #[test]
    fn test() {
        let dir = TempDir::new().unwrap();
        initialize_disk(dir.path(), State::default).unwrap();
        let lock = exclusive_lock(dir.path()).unwrap();
        assert!(dir.path().join(names::STATE).is_file());
        assert!(dir.path().join(names::LOCK).is_file());
        let v0 = crnt_version(&lock).unwrap();
        std::mem::drop(lock);
        let lock = exclusive_lock(dir.path()).unwrap();
        assert!(dir.path().join(names::STATE).is_file());
        assert!(dir.path().join(names::LOCK).is_file());
        assert_eq!(v0, crnt_version(&lock).unwrap());
        std::thread::sleep(std::time::Duration::from_millis(100));
        write_state(dir.path(), &State::default()).unwrap();
        let new_version = crnt_version(&lock).unwrap();
        assert!(v0 < new_version);
    }
}
