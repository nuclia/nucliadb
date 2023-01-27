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
use crate::*;

pub const MAX_VERSION: u32 = 1;

pub type RParagraphs = Arc<dyn ParagraphReader>;
pub type WParagraphs = Arc<RwLock<dyn ParagraphWriter>>;

pub fn open_reader(config: &ParagraphConfig, version: u32) -> NodeResult<RParagraphs> {
    match version {
        1 => nucliadb_paragraphs::reader::ParagraphReaderService::open(config)
            .map(|v| Arc::new(v) as RParagraphs),
        v => Err(node_error!("Invalid paragraphs  version {v}")),
    }
}

pub fn open_writer(config: &ParagraphConfig, version: u32) -> NodeResult<WParagraphs> {
    match version {
        1 => nucliadb_paragraphs::writer::ParagraphWriterService::open(config)
            .map(|v| Arc::new(RwLock::new(v)) as WParagraphs),
        v => Err(node_error!("Invalid paragraphs  version {v}")),
    }
}

pub fn create_reader(config: &ParagraphConfig, version: u32) -> NodeResult<RParagraphs> {
    match version {
        1 => nucliadb_paragraphs::reader::ParagraphReaderService::new(config)
            .map(|v| Arc::new(v) as RParagraphs),
        v => Err(node_error!("Invalid paragraphs version {v}")),
    }
}

pub fn create_writer(config: &ParagraphConfig, version: u32) -> NodeResult<WParagraphs> {
    match version {
        1 => nucliadb_paragraphs::writer::ParagraphWriterService::new(config)
            .map(|v| Arc::new(RwLock::new(v)) as WParagraphs),
        v => Err(node_error!("Invalid paragraphs  version {v}")),
    }
}
