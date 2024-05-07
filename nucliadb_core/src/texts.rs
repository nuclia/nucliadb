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
use crate::query_planner::{PreFilterRequest, PreFilterResponse};
use crate::IndexFiles;

pub type TextsReaderPointer = Box<dyn FieldReader>;
pub type TextsWriterPointer = Box<dyn FieldWriter>;
pub type ProtosRequest = DocumentSearchRequest;
pub type ProtosResponse = DocumentSearchResponse;

#[derive(Debug, Clone)]
pub struct TextConfig {
    pub path: PathBuf,
}

pub struct DocumentIterator(Box<dyn Iterator<Item = DocumentItem> + Send>);
impl DocumentIterator {
    pub fn new<I>(inner: I) -> DocumentIterator
    where
        I: Iterator<Item = DocumentItem> + Send + 'static,
    {
        DocumentIterator(Box::new(inner))
    }
}
impl Iterator for DocumentIterator {
    type Item = DocumentItem;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub trait FieldReader: std::fmt::Debug + Send + Sync {
    fn search(&self, request: &ProtosRequest) -> NodeResult<ProtosResponse>;
    fn stored_ids(&self) -> NodeResult<Vec<String>>;
    fn prefilter(&self, request: &PreFilterRequest) -> NodeResult<PreFilterResponse>;
    fn iterator(&self, request: &StreamRequest) -> NodeResult<DocumentIterator>;
    fn count(&self) -> NodeResult<usize>;
}

pub trait FieldWriter: std::fmt::Debug + Send + Sync {
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()>;
    fn delete_resource(&mut self, resource_id: &ResourceId) -> NodeResult<()>;
    fn garbage_collection(&mut self) -> NodeResult<()>;
    fn count(&self) -> NodeResult<usize>;
    fn get_segment_ids(&self) -> NodeResult<Vec<String>>;
    fn get_index_files(&self, ignored_segment_ids: &[String]) -> NodeResult<IndexFiles>;
}
