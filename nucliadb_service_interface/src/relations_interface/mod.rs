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

#[derive(Clone)]
pub struct RelationConfig {
    pub path: PathBuf,
}

#[derive(Debug)]
pub struct RelationError {
    pub msg: String,
}

pub trait RelationReader:
    ReaderChild<Request = RelationSearchRequest, Response = RelationSearchResponse>
{
    fn get_edges(&self) -> InternalResult<EdgeList>;
    fn get_node_types(&self) -> InternalResult<TypeList>;
    fn count(&self) -> InternalResult<usize>;
}

pub trait RelationWriter: WriterChild {
    fn join_graph(&mut self, graph: &JoinGraph) -> InternalResult<()>;
    fn delete_nodes(&mut self, graph: &DeleteGraphNodes) -> InternalResult<()>;
}

impl std::fmt::Display for RelationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.msg)
    }
}

impl InternalError for RelationError {}
