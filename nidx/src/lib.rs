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
pub mod api;
mod errors;
pub mod grpc_server;
mod import_export;
pub mod indexer;
pub mod metadata;
pub mod metrics;
pub mod scheduler;
pub mod searcher;
mod segment_store;
pub mod settings;
pub mod tool;
mod utilization_tracker;
pub mod worker;

#[cfg(feature = "telemetry")]
pub mod telemetry;

#[cfg(feature = "telemetry")]
pub mod control;

pub use metadata::NidxMetadata;
pub use settings::Settings;
