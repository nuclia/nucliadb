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

pub mod utils {
    tonic::include_proto!("utils");
}

pub mod nodereader {
    tonic::include_proto!("nodereader");
}

pub mod noderesources {
    tonic::include_proto!("noderesources");
}

pub mod nodewriter {
    tonic::include_proto!("nodewriter");
}

pub mod nidx {
    tonic::include_proto!("nidx");
}

pub mod kb_usage {
    tonic::include_proto!("kb_usage");
}

pub use nodereader::*;
pub use noderesources::*;
pub use nodewriter::*;
pub use utils::*;

// A couple of helpful re-exports
pub mod prost {
    pub use prost::Message as _;
}

pub mod prost_types {
    pub use prost_types::Timestamp;
}
