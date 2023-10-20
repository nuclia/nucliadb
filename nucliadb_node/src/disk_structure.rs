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

//! Files and directories organization
//!
//! The following diagram shows how the file system is organized in the index
//! node (without details about each individual index organization):
//!
//! DATA_PATH
//! |-- SHARDS_DIR
//!     |-- <shard-uuid>
//!         |-- PARAGRAPHS_DIR
//!         |-- RELATIONS_DIR
//!         |-- TEXTS_DIR
//!         |-- VECTORSET_DIR
//!         |-- VECTORS_DIR

use std::path::{Path, PathBuf};

pub const VERSION_FILE: &str = "versions.json";
pub const METADATA_FILE: &str = "metadata.json";
pub const GENERATION_FILE: &str = "generation";

pub const SHARDS_DIR: &str = "shards";

pub const PARAGRAPHS_DIR: &str = "paragraph";
pub const RELATIONS_DIR: &str = "relations";
pub const TEXTS_DIR: &str = "text";
pub const VECTORSET_DIR: &str = "vectorset";
pub const VECTORS_DIR: &str = "vectors";

/// Path where shard `id` is stored
pub fn shard_path_by_id(shards_path: &Path, id: &str) -> PathBuf {
    shards_path.join(id)
}
