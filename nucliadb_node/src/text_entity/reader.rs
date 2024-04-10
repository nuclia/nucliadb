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

use nucliadb_core::texts::{TextConfig, TextReader};
use nucliadb_core::{node_error, NodeResult};
use nucliadb_texts::reader::TextReaderService as TextReaderServiceV1;
use nucliadb_texts2::reader::TextReaderService as TextReaderServiceV2;
use std::sync::RwLock;

pub type TextRPointer = Box<RwLock<dyn TextReader>>;

pub fn new(version: u32, config: &TextConfig) -> NodeResult<TextRPointer> {
    match version {
        1 => TextReaderServiceV1::start(config).map(|i| Box::new(RwLock::new(i)) as TextRPointer),
        2 => TextReaderServiceV2::start(config).map(|i| Box::new(RwLock::new(i)) as TextRPointer),
        v => Err(node_error!("Invalid text version {v}")),
    }
}
