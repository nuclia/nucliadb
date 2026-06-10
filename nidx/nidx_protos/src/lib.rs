// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

pub use nidx::*;
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
