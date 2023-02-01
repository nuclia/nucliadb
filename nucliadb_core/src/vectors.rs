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

use nucliadb_protos::*;

use crate::prelude::*;

pub type VectorsReaderPointer = Arc<dyn VectorReader>;
pub type VectorsWriterPointer = Arc<RwLock<dyn VectorWriter>>;

#[derive(Clone)]
pub struct VectorConfig {
    pub no_results: Option<usize>,
    pub path: PathBuf,
    pub vectorset: PathBuf,
}

pub trait VectorReader:
    ReaderChild<Request = VectorSearchRequest, Response = VectorSearchResponse>
{
    fn count(&self, vectorset: &str) -> NodeResult<usize>;
}

pub trait VectorWriter: WriterChild {
    fn list_vectorsets(&self) -> NodeResult<Vec<String>>;
    fn add_vectorset(&mut self, setid: &VectorSetId) -> NodeResult<()>;
    fn remove_vectorset(&mut self, setid: &VectorSetId) -> NodeResult<()>;
}
