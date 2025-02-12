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
use std::path::PathBuf;

pub mod prefilter;
pub mod query_language;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(i64);
impl From<i64> for Seq {
    fn from(value: i64) -> Self {
        Self(value)
    }
}
impl From<u64> for Seq {
    fn from(value: u64) -> Self {
        Self(value as i64)
    }
}
impl From<Seq> for i64 {
    fn from(value: Seq) -> Self {
        value.0
    }
}
impl From<&Seq> for i64 {
    fn from(value: &Seq) -> Self {
        value.0
    }
}

#[derive(Clone, Debug)]
pub struct SegmentMetadata<T> {
    pub path: PathBuf,
    pub records: usize,
    pub index_metadata: T,
}

/// The metadata needed to open an index: a list of segments and deletions.
pub trait OpenIndexMetadata<T> {
    /// List of segments and Seq
    fn segments(&self) -> impl Iterator<Item = (SegmentMetadata<T>, Seq)>;
    /// List of deletions and Seq
    fn deletions(&self) -> impl Iterator<Item = (&String, Seq)>;
}
