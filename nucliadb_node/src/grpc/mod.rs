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

//! gRPC servers for index node reader and writer.
//!
//! This is a high level interface that gives access to the index node
//! functionalities

pub mod grpc_reader;
pub mod grpc_writer;
pub mod middleware;

pub use grpc_reader::NodeReaderGRPCDriver;
pub use grpc_writer::NodeWriterGRPCDriver;
// Alias for more readable imports
pub use {grpc_reader as reader, grpc_writer as writer};
