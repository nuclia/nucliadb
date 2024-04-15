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

use nucliadb_core::paragraphs::{ParagraphConfig, ParagraphWriter};
use nucliadb_core::{node_error, NodeResult};
use nucliadb_paragraphs::writer::ParagraphWriterService as ParagraphWriterServiceV1;
use nucliadb_paragraphs2::writer::ParagraphWriterService as ParagraphWriterServiceV2;
use std::sync::RwLock;

pub type ParagraphWPointer = RwLock<Box<dyn ParagraphWriter>>;

pub fn new(version: u32, config: &ParagraphConfig) -> NodeResult<ParagraphWPointer> {
    match version {
        1 => ParagraphWriterServiceV1::start(config).map(|i| RwLock::new(Box::new(i) as Box<dyn ParagraphWriter>)),
        2 => ParagraphWriterServiceV2::start(config).map(|i| RwLock::new(Box::new(i) as Box<dyn ParagraphWriter>)),
        v => Err(node_error!("Invalid paragraphs version {v}")),
    }
}
