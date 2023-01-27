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

use std::path::Path;

use nucliadb_service_interface::prelude::*;
use serde::{Deserialize, Serialize};

const VECTORS_VERSION: u32 = 1;
const PARAGRAPHS_VERSION: u32 = 1;
const RELATIONS_VERSION: u32 = 1;
const TEXTS_VERSION: u32 = 1;

fn paragraphs_default() -> u32 {
    PARAGRAPHS_VERSION
}
fn vectors_default() -> u32 {
    VECTORS_VERSION
}
fn texts_default() -> u32 {
    TEXTS_VERSION
}
fn relations_default() -> u32 {
    RELATIONS_VERSION
}

#[derive(Serialize, Deserialize)]
pub struct Versions {
    #[serde(default = "paragraphs_default")]
    pub version_paragraphs: u32,
    #[serde(default = "vectors_default")]
    pub version_vectors: u32,
    #[serde(default = "texts_default")]
    pub version_texts: u32,
    #[serde(default = "relations_default")]
    pub version_relations: u32,
}

impl Default for Versions {
    fn default() -> Self {
        Versions {
            version_paragraphs: PARAGRAPHS_VERSION,
            version_texts: VECTORS_VERSION,
            version_vectors: TEXTS_VERSION,
            version_relations: RELATIONS_VERSION,
        }
    }
}

impl Versions {
    pub fn get_vectors_reader(&self, config: &VectorConfig) -> NodeResult<RVectors> {
        match self.version_vectors {
            1 => nucliadb_vectors::service::VectorReaderService::open(config)
                .map(|i| encapsulate_reader(i) as RVectors),
            v => Err(node_error!("Invalid vectors  version {v}")),
        }
    }
    pub fn get_paragraphs_reader(&self, config: &ParagraphConfig) -> NodeResult<RParagraphs> {
        match self.version_paragraphs {
            1 => nucliadb_paragraphs::reader::ParagraphReaderService::open(config)
                .map(|i| encapsulate_reader(i) as RParagraphs),
            v => Err(node_error!("Invalid paragraphs  version {v}")),
        }
    }

    pub fn get_texts_reader(&self, config: &TextConfig) -> NodeResult<RTexts> {
        match self.version_texts {
            1 => nucliadb_texts::reader::TextReaderService::open(config)
                .map(|i| encapsulate_reader(i) as RTexts),
            v => Err(node_error!("Invalid text  version {v}")),
        }
    }

    pub fn get_relations_reader(&self, config: &RelationConfig) -> NodeResult<RRelations> {
        match self.version_relations {
            1 => nucliadb_relations::service::RelationsReaderService::open(config)
                .map(|i| encapsulate_reader(i) as RRelations),
            v => Err(node_error!("Invalid relations  version {v}")),
        }
    }

    pub fn get_vectors_writer(&self, config: &VectorConfig) -> NodeResult<WVectors> {
        match self.version_vectors {
            1 => nucliadb_vectors::service::VectorWriterService::start(config)
                .map(|i| encapsulate_writer(i) as WVectors),
            v => Err(node_error!("Invalid vectors  version {v}")),
        }
    }
    pub fn get_paragraphs_writer(&self, config: &ParagraphConfig) -> NodeResult<WParagraphs> {
        match self.version_paragraphs {
            1 => nucliadb_paragraphs::writer::ParagraphWriterService::start(config)
                .map(|i| encapsulate_writer(i) as WParagraphs),
            v => Err(node_error!("Invalid paragraphs  version {v}")),
        }
    }

    pub fn get_texts_writer(&self, config: &TextConfig) -> NodeResult<WTexts> {
        match self.version_texts {
            1 => nucliadb_texts::writer::TextWriterService::start(config)
                .map(|i| encapsulate_writer(i) as WTexts),
            v => Err(node_error!("Invalid text  version {v}")),
        }
    }

    pub fn get_relations_writer(&self, config: &RelationConfig) -> NodeResult<WRelations> {
        match self.version_relations {
            1 => nucliadb_relations::service::RelationsWriterService::start(config)
                .map(|i| encapsulate_writer(i) as WRelations),
            v => Err(node_error!("Invalid relations  version {v}")),
        }
    }

    pub fn load(versions_file: &Path) -> NodeResult<Versions> {
        let versions_json = std::fs::read_to_string(versions_file)?;
        Ok(serde_json::from_str(&versions_json)?)
    }
    pub fn load_or_create(versions_file: &Path) -> NodeResult<Versions> {
        if versions_file.exists() {
            Versions::load(versions_file)
        } else {
            let versions = Versions::default();
            let serialized = serde_json::to_string(&versions)?;
            std::fs::write(&versions_file, serialized)?;
            Ok(versions)
        }
    }
}
