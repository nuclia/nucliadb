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

mod shard_disk_structure {
    pub const VERSION_FILE: &str = "versions.json";
    pub const VECTORS_DIR: &str = "vectors";
    pub const VECTORSET_DIR: &str = "vectorset";
    pub const TEXTS_DIR: &str = "text";
    pub const PARAGRAPHS_DIR: &str = "paragraph";
    pub const RELATIONS_DIR: &str = "relations";
}
