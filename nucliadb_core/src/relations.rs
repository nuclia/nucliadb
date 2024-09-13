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
use crate::IndexFiles;

pub type RelationsReaderPointer = Box<dyn RelationsReader>;
pub type RelationsWriterPointer = Box<dyn RelationsWriter>;
pub type ProtosRequest = RelationSearchRequest;
pub type ProtosResponse = RelationSearchResponse;

#[derive(Clone)]
pub struct RelationConfig {
    pub path: PathBuf,
}

pub trait RelationsReader: std::fmt::Debug + Send + Sync {
    fn search(&self, request: &ProtosRequest) -> NodeResult<ProtosResponse>;
    fn stored_ids(&self) -> NodeResult<Vec<String>>;
    fn get_edges(&self) -> NodeResult<EdgeList>;
    fn count(&self) -> NodeResult<usize>;
}

pub trait RelationsWriter: std::fmt::Debug + Send + Sync {
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()>;
    fn delete_resource(&mut self, resource_id: &ResourceId) -> NodeResult<()>;
    fn garbage_collection(&mut self) -> NodeResult<()>;
    fn count(&self) -> NodeResult<usize>;
    fn get_segment_ids(&self) -> NodeResult<Vec<String>>;
    fn get_index_files(&self, ignored_segment_ids: &[String]) -> NodeResult<IndexFiles>;
}
