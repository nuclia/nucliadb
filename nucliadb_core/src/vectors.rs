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
use std::sync::{Arc, RwLock};

use crate::metrics::vectors::MergeSource;
use crate::prelude::*;
use crate::protos::*;
use crate::query_language::BooleanExpression;
use crate::Channel;
use crate::IndexFiles;

pub type VectorsReaderPointer = Arc<RwLock<dyn VectorReader>>;
pub type VectorsWriterPointer = Arc<RwLock<dyn VectorWriter>>;
pub type ProtosRequest = VectorSearchRequest;
pub type ProtosResponse = VectorSearchResponse;

#[derive(Debug, Clone, Copy)]
pub struct MergeContext {
    pub max_nodes_in_merge: usize,
    pub segments_before_merge: usize,
    pub source: MergeSource,
}

#[derive(Clone)]
pub struct VectorConfig {
    pub similarity: Option<VectorSimilarity>,
    pub path: PathBuf,
    pub vectorset: PathBuf,
    pub channel: Channel,
}

// In an ideal world this should be part of the actual request, but since
// we use protos all the way down the stack here we are. Once the protos use
// is restricted to only the upper layer, this type won't be needed anymore.
#[derive(Clone, Default)]
pub struct VectorsContext {
    pub filtering_formula: Option<BooleanExpression>,
}

pub struct MergeMetrics {
    pub merged: usize,
    pub left: usize,
}

pub trait VectorReader: std::fmt::Debug + Send + Sync {
    fn search(&self, request: &ProtosRequest, context: &VectorsContext) -> NodeResult<ProtosResponse>;
    fn stored_ids(&self) -> NodeResult<Vec<String>>;
    fn count(&self, vectorset: &str) -> NodeResult<usize>;

    fn update(&mut self) -> NodeResult<()>;
}

pub trait VectorWriter: std::fmt::Debug + Send + Sync {
    fn count(&self) -> NodeResult<usize>;
    fn get_segment_ids(&self) -> NodeResult<Vec<String>>;
    fn get_index_files(&self, ignored_segment_ids: &[String]) -> NodeResult<IndexFiles>;
    fn list_vectorsets(&self) -> NodeResult<Vec<String>>;

    fn merge(&mut self, context: MergeContext) -> NodeResult<MergeMetrics>;
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()>;
    fn delete_resource(&mut self, resource_id: &ResourceId) -> NodeResult<()>;
    fn garbage_collection(&mut self) -> NodeResult<()>;
    fn force_garbage_collection(&mut self) -> NodeResult<()>;
    fn remove_vectorset(&mut self, setid: &VectorSetId) -> NodeResult<()>;
    fn add_vectorset(&mut self, setid: &VectorSetId, similarity: VectorSimilarity) -> NodeResult<()>;
    fn reload(&mut self) -> NodeResult<()>;
}
