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
pub mod fields_interface;
pub mod paragraphs_interface;
pub mod relations_interface;
pub mod service_interface;
pub mod vectos_interface;

pub mod dependencies {
    pub extern crate anyhow;
    pub extern crate async_std;
    pub extern crate async_trait;
    pub extern crate nucliadb_protos;
    pub extern crate prost_types;
    pub extern crate tempdir;
    pub extern crate tokio;
    pub extern crate tracing;
}

pub mod prelude {
    pub use crate::dependencies::*;
    pub use crate::fields_interface::*;
    pub use crate::paragraphs_interface::*;
    pub use crate::relations_interface::*;
    pub use crate::service_interface::*;
    pub use crate::vectos_interface::*;
}
