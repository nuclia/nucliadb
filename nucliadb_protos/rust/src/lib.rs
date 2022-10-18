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

#![allow(clippy::derive_partial_eq_without_eq)]

pub mod fdbwriter;
mod knowledgebox;
mod nodereader;
mod noderesources;
mod nodewriter;
mod resources;
mod utils;

extern crate serde;

// writer should not serialized to rust as its only ingest and brings conflict
// pub use fdbwriter::*;
pub use knowledgebox::*;
pub use nodereader::*;
pub use noderesources::*;
pub use nodewriter::*;
pub use resources::*;
pub use utils::*;
