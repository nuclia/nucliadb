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

pub const MAX_VERSION: u32 = 0;

pub type RTexts = Arc<dyn FieldReader>;
pub type WTexts = Arc<RwLock<dyn FieldWriter>>;

pub fn open_reader(config: &TextConfig, version: u32) -> NodeResult<RTexts> {
    match version {
        0 => nucliadb_texts::reader::TextReaderService::open(config).map(|v| Arc::new(v) as RTexts),
        v => Err(node_error!("Invalid text  version {v}")),
    }
}

pub fn open_writer(config: &TextConfig, version: u32) -> NodeResult<WTexts> {
    match version {
        0 => nucliadb_texts::writer::TextWriterService::open(config)
            .map(|v| Arc::new(RwLock::new(v)) as WTexts),
        v => Err(node_error!("Invalid text  version {v}")),
    }
}
pub fn create_reader(config: &TextConfig, version: u32) -> NodeResult<RTexts> {
    match version {
        0 => nucliadb_texts::reader::TextReaderService::new(config).map(|v| Arc::new(v) as RTexts),
        v => Err(node_error!("Invalid text  version {v}")),
    }
}

pub fn create_writer(config: &TextConfig, version: u32) -> NodeResult<WTexts> {
    match version {
        0 => nucliadb_texts::writer::TextWriterService::new(config)
            .map(|v| Arc::new(RwLock::new(v)) as WTexts),
        v => Err(node_error!("Invalid text  version {v}")),
    }
}
