// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
    /// Segments in ascending `Seq` order (oldest first).
    fn segments(&self) -> impl DoubleEndedIterator<Item = (SegmentMetadata<T>, Seq)>;
    /// Deletions in ascending `Seq` order (oldest first).
    fn deletions(&self) -> impl DoubleEndedIterator<Item = (&String, Seq)>;
}
