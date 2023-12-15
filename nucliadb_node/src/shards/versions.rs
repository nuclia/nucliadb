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

use nucliadb_core::prelude::*;
use nucliadb_core::Channel;
use serde::{Deserialize, Serialize};

const VECTORS_VERSION: u32 = 1;
const PARAGRAPHS_VERSION: u32 = 1;
const RELATIONS_VERSION: u32 = 1;
const TEXTS_VERSION: u32 = 1;
const DEPRECATED_CONFIG: &str = "config.json";

#[derive(Serialize, Deserialize)]
pub struct Versions {
    #[serde(default)]
    version_paragraphs: Option<u32>,
    #[serde(default)]
    version_vectors: Option<u32>,
    #[serde(default)]
    version_texts: Option<u32>,
    #[serde(default)]
    version_relations: Option<u32>,
}

impl Versions {
    fn deprecated_versions_exists(versions_file: &Path) -> bool {
        versions_file
            .parent()
            .map(|v| v.join(DEPRECATED_CONFIG))
            .map(|v| v.exists())
            .unwrap_or_default()
    }
    fn new_from_deprecated() -> Versions {
        Versions {
            version_paragraphs: Some(1),
            version_vectors: Some(1),
            version_texts: Some(1),
            version_relations: Some(1),
        }
    }
    fn new(channel: Channel) -> Versions {
        let mut rel_version = RELATIONS_VERSION;
        if channel == Channel::EXPERIMENTAL {
            rel_version = 2;
        }
        Versions {
            version_paragraphs: Some(PARAGRAPHS_VERSION),
            version_vectors: Some(VECTORS_VERSION),
            version_texts: Some(TEXTS_VERSION),
            version_relations: Some(rel_version),
        }
    }
    fn fill_gaps(&mut self) -> bool {
        let mut modified = false;
        if self.version_paragraphs.is_none() {
            self.version_paragraphs = Some(PARAGRAPHS_VERSION);
            modified = true;
        }
        if self.version_relations.is_none() {
            self.version_relations = Some(RELATIONS_VERSION);
            modified = true;
        }
        if self.version_texts.is_none() {
            self.version_texts = Some(TEXTS_VERSION);
            modified = true;
        }
        if self.version_vectors.is_none() {
            self.version_vectors = Some(VECTORS_VERSION);
            modified = true;
        }
        modified
    }
    pub fn version_paragraphs(&self) -> u32 {
        self.version_paragraphs.unwrap_or(PARAGRAPHS_VERSION)
    }
    pub fn version_vectors(&self) -> u32 {
        self.version_vectors.unwrap_or(VECTORS_VERSION)
    }
    pub fn version_texts(&self) -> u32 {
        self.version_texts.unwrap_or(TEXTS_VERSION)
    }
    pub fn version_relations(&self) -> u32 {
        self.version_relations.unwrap_or(RELATIONS_VERSION)
    }
    pub fn get_vectors_reader(&self, config: &VectorConfig) -> NodeResult<VectorsReaderPointer> {
        match self.version_vectors {
            Some(1) => nucliadb_vectors::service::VectorReaderService::start(config)
                .map(|i| encapsulate_reader(i) as VectorsReaderPointer),
            Some(v) => Err(node_error!("Invalid vectors version {v}")),
            None => Err(node_error!("Corrupted version file")),
        }
    }
    pub fn get_paragraphs_reader(
        &self,
        config: &ParagraphConfig,
    ) -> NodeResult<ParagraphsReaderPointer> {
        match self.version_paragraphs {
            Some(1) => nucliadb_paragraphs::reader::ParagraphReaderService::start(config)
                .map(|i| encapsulate_reader(i) as ParagraphsReaderPointer),
            Some(v) => Err(node_error!("Invalid paragraphs version {v}")),
            None => Err(node_error!("Corrupted version file")),
        }
    }

    pub fn get_texts_reader(&self, config: &TextConfig) -> NodeResult<TextsReaderPointer> {
        match self.version_texts {
            Some(1) => nucliadb_texts::reader::TextReaderService::start(config)
                .map(|i| encapsulate_reader(i) as TextsReaderPointer),
            Some(v) => Err(node_error!("Invalid text version {v}")),
            None => Err(node_error!("Corrupted version file")),
        }
    }

    pub fn get_relations_reader(
        &self,
        config: &RelationConfig,
    ) -> NodeResult<RelationsReaderPointer> {
        match self.version_relations {
            Some(1) => nucliadb_relations::service::RelationsReaderService::start(config)
                .map(|i| encapsulate_reader(i) as RelationsReaderPointer),
            Some(2) => nucliadb_relations2::reader::RelationsReaderService::start(config)
                .map(|i| encapsulate_reader(i) as RelationsReaderPointer),
            Some(v) => Err(node_error!("Invalid relations version {v}")),
            None => Err(node_error!("Corrupted version file")),
        }
    }

    pub fn get_vectors_writer(&self, config: &VectorConfig) -> NodeResult<VectorsWriterPointer> {
        match self.version_vectors {
            Some(1) => nucliadb_vectors::service::VectorWriterService::start(config)
                .map(|i| encapsulate_writer(i) as VectorsWriterPointer),
            Some(v) => Err(node_error!("Invalid vectors version {v}")),
            None => Err(node_error!("Corrupted version file")),
        }
    }
    pub fn get_paragraphs_writer(
        &self,
        config: &ParagraphConfig,
    ) -> NodeResult<ParagraphsWriterPointer> {
        match self.version_paragraphs {
            Some(1) => nucliadb_paragraphs::writer::ParagraphWriterService::start(config)
                .map(|i| encapsulate_writer(i) as ParagraphsWriterPointer),
            Some(v) => Err(node_error!("Invalid paragraphs version {v}")),
            None => Err(node_error!("Corrupted version file")),
        }
    }

    pub fn get_texts_writer(&self, config: &TextConfig) -> NodeResult<TextsWriterPointer> {
        match self.version_texts {
            Some(1) => nucliadb_texts::writer::TextWriterService::start(config)
                .map(|i| encapsulate_writer(i) as TextsWriterPointer),
            Some(v) => Err(node_error!("Invalid text version {v}")),
            None => Err(node_error!("Corrupted version file")),
        }
    }

    pub fn get_relations_writer(
        &self,
        config: &RelationConfig,
    ) -> NodeResult<RelationsWriterPointer> {
        match self.version_relations {
            Some(1) => nucliadb_relations::service::RelationsWriterService::start(config)
                .map(|i| encapsulate_writer(i) as RelationsWriterPointer),
            Some(2) => nucliadb_relations2::writer::RelationsWriterService::start(config)
                .map(|i| encapsulate_writer(i) as RelationsWriterPointer),
            Some(v) => Err(node_error!("Invalid relations version {v}")),
            None => Err(node_error!("Corrupted version file")),
        }
    }

    pub fn load(versions_file: &Path) -> NodeResult<Versions> {
        if versions_file.exists() {
            let versions_json = std::fs::read_to_string(versions_file)?;
            let mut versions: Versions = serde_json::from_str(&versions_json)?;
            versions.fill_gaps();
            Ok(versions)
        } else if Versions::deprecated_versions_exists(versions_file) {
            // In this case is an old index, therefore we create the versions file
            // with the index versions that where available that moment.
            // The writer will create the file at some point
            Ok(Versions::new_from_deprecated())
        } else {
            Err(node_error!("Versions not found"))
        }
    }
    pub fn load_or_create(versions_file: &Path, channel: Channel) -> NodeResult<Versions> {
        if versions_file.exists() {
            let versions_json = std::fs::read_to_string(versions_file)?;
            let mut versions: Versions = serde_json::from_str(&versions_json)?;
            if versions.fill_gaps() {
                let serialized = serde_json::to_string(&versions)?;
                std::fs::write(versions_file, serialized)?;
            }
            Ok(versions)
        } else if Versions::deprecated_versions_exists(versions_file) {
            // In this case is an old index, therefore we create the versions file
            // with the index versions that where available that moment.
            let versions = Versions::new_from_deprecated();
            let serialized = serde_json::to_string(&versions)?;
            std::fs::write(versions_file, serialized)?;
            Ok(versions)
        } else {
            let versions = Versions::new(channel);
            let serialized = serde_json::to_string(&versions)?;
            std::fs::write(versions_file, serialized)?;
            Ok(versions)
        }
    }
}
