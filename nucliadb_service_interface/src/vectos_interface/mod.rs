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
use nucliadb_protos::*;

use crate::service_interface::*;
#[derive(Clone)]
pub struct VectorConfig {
    pub no_results: Option<usize>,
    pub path: String,
    pub vectorset: String,
}

pub trait VectorReader:
    ReaderChild<Request = VectorSearchRequest, Response = VectorSearchResponse>
{
    fn count(&self, vectorset: &str) -> InternalResult<usize>;
}

pub trait VectorWriter: WriterChild {
    fn list_vectorsets(&self) -> InternalResult<Vec<String>>;
    fn add_vectorset(&mut self, setid: &VectorSetId) -> InternalResult<()>;
    fn remove_vectorset(&mut self, setid: &VectorSetId) -> InternalResult<()>;
}

#[derive(Debug)]
pub struct VectorError {
    pub msg: String,
}

impl std::fmt::Display for VectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.msg)
    }
}

impl InternalError for VectorError {}
