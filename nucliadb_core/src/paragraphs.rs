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

use crate::prelude::*;
use crate::protos::*;
use crate::query_language::BooleanExpression;
use crate::IndexFiles;

pub type ParagraphsReaderPointer = Box<dyn ParagraphReader>;
pub type ParagraphsWriterPointer = Box<dyn ParagraphWriter>;
pub type ProtosRequest = ParagraphSearchRequest;
pub type ProtosResponse = ParagraphSearchResponse;

#[derive(Debug, Clone)]
pub struct ParagraphConfig {
    pub path: PathBuf,
}

pub struct ParagraphIterator(Box<dyn Iterator<Item = ParagraphItem> + Send>);
impl ParagraphIterator {
    pub fn new<I>(inner: I) -> ParagraphIterator
    where
        I: Iterator<Item = ParagraphItem> + Send + 'static,
    {
        ParagraphIterator(Box::new(inner))
    }
}
impl Iterator for ParagraphIterator {
    type Item = ParagraphItem;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

// In an ideal world this should be part of the actual request, but since
// we use protos all the way down the stack here we are. Once the protos use
// is restricted to only the upper layer, this type won't be needed anymore.
#[derive(Clone, Default)]
pub struct ParagraphsContext {
    pub filtering_formula: Option<BooleanExpression>,
}

pub trait ParagraphReader: std::fmt::Debug + Send + Sync {
    fn search(&self, request: &ProtosRequest, context: &ParagraphsContext) -> NodeResult<ProtosResponse>;
    fn suggest(&self, request: &SuggestRequest) -> NodeResult<ProtosResponse>;
    fn iterator(&self, request: &StreamRequest) -> NodeResult<ParagraphIterator>;
    fn stored_ids(&self) -> NodeResult<Vec<String>>;
    fn count(&self) -> NodeResult<usize>;
}

pub trait ParagraphWriter: std::fmt::Debug + Send + Sync {
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()>;
    fn delete_resource(&mut self, resource_id: &ResourceId) -> NodeResult<()>;
    fn garbage_collection(&mut self) -> NodeResult<()>;
    fn count(&self) -> NodeResult<usize>;
    fn get_segment_ids(&self) -> NodeResult<Vec<String>>;
    fn get_index_files(&self, ignored_segment_ids: &[String]) -> NodeResult<IndexFiles>;
}
