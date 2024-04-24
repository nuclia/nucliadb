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

use nucliadb_core::prelude::*;
use nucliadb_core::Channel;
use serde::{Deserialize, Serialize};
use std::path::Path;

const VECTORS_VERSION: u32 = 1;
const PARAGRAPHS_VERSION: u32 = 3;
const RELATIONS_VERSION: u32 = 2;
const TEXTS_VERSION: u32 = 2;

#[derive(Serialize, Deserialize)]
pub struct Versions {
    pub paragraphs: u32,
    pub vectors: u32,
    pub texts: u32,
    pub relations: u32,
}

impl Versions {
    pub fn load(versions_file: &Path) -> NodeResult<Versions> {
        let versions_json = std::fs::read_to_string(versions_file)?;
        let versions: Versions = serde_json::from_str(&versions_json)?;
        Ok(versions)
    }

    pub fn create(versions_file: &Path, _channel: Channel) -> NodeResult<Versions> {
        let versions = Versions {
            paragraphs: PARAGRAPHS_VERSION,
            vectors: VECTORS_VERSION,
            texts: TEXTS_VERSION,
            relations: RELATIONS_VERSION,
        };
        let serialized = serde_json::to_string(&versions)?;
        std::fs::write(versions_file, serialized)?;
        Ok(versions)
    }
}
