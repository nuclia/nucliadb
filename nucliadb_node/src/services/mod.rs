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

mod versions;
// Main services
pub mod reader;
pub mod writer;

/// Files and directories hierarchy
///
/// DATA_PATH
/// |-- SHARDS_DIR
///     |-- <shard-uuid>
///         |-- PARAGRAPHS_DIR
///         |-- RELATIONS_DIR
///         |-- TEXTS_DIR
///         |-- VECTORSET_DIR
///         |-- VECTORS_DIR
pub mod shard_disk_structure {
    use std::path::{Path, PathBuf};

    pub const VERSION_FILE: &str = "versions.json";
    pub const METADATA_FILE: &str = "metadata.json";

    pub const SHARDS_DIR: &str = "shards";

    pub const PARAGRAPHS_DIR: &str = "paragraph";
    pub const RELATIONS_DIR: &str = "relations";
    pub const TEXTS_DIR: &str = "text";
    pub const VECTORSET_DIR: &str = "vectorset";
    pub const VECTORS_DIR: &str = "vectors";

    /// Path to index node metadata file
    pub fn metadata_path(data_path: &Path) -> PathBuf {
        data_path.join(METADATA_FILE)
    }

    /// Path where all shards are stored
    pub fn shards_path(data_path: &Path) -> PathBuf {
        data_path.join(SHARDS_DIR)
    }

    /// Path where shard `id` is stored
    pub fn shard_path_by_id(data_path: &Path, id: &str) -> PathBuf {
        shards_path(data_path).join(id)
    }
}
