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

use nucliadb_protos::*;

use crate::service_interface::*;

pub struct ParagraphConfig {
    pub path: PathBuf,
}
#[derive(Debug)]
pub struct ParagraphError {
    pub msg: String,
}

impl std::fmt::Display for ParagraphError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.msg)
    }
}

impl InternalError for ParagraphError {}

pub struct ParagraphIterator(Box<dyn Iterator<Item = ParagraphItem> + Send>);
impl ParagraphIterator {
    pub fn new<I>(inner: I) -> ParagraphIterator
    where I: Iterator<Item = ParagraphItem> + Send + 'static {
        ParagraphIterator(Box::new(inner))
    }
}
impl Iterator for ParagraphIterator {
    type Item = ParagraphItem;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub trait ParagraphReader:
    ReaderChild<Request = ParagraphSearchRequest, Response = ParagraphSearchResponse>
{
    fn iterator(&self, request: &StreamRequest) -> InternalResult<ParagraphIterator>;
    fn suggest(&self, request: &SuggestRequest) -> InternalResult<Self::Response>;
    fn count(&self) -> InternalResult<usize>;
}

pub trait ParagraphWriter: WriterChild {}
