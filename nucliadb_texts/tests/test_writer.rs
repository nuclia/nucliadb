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

mod common;

use nucliadb_core::prelude::*;
use nucliadb_core::NodeResult;
use nucliadb_texts::reader::TextReaderService;
use nucliadb_texts::writer::TextWriterService;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{AllQuery, TermQuery};
use tantivy::schema::{IndexRecordOption, *};
use tempfile::TempDir;

#[test]
fn test_start_new_writer() {
    let dir = TempDir::new().unwrap();
    let config = TextConfig {
        path: dir.path().join("texts"),
    };

    let writer = TextWriterService::start(&config);
    assert!(writer.is_ok());
}

#[test]
fn test_open_writer_for_existent_index() {
    let dir = TempDir::new().unwrap();
    let config = TextConfig {
        path: dir.path().join("texts"),
    };

    let writer = TextWriterService::start(&config).unwrap();
    std::mem::drop(writer);

    let another_writer = TextWriterService::open(&config);

    assert!(another_writer.is_ok());
}

#[test]
#[should_panic]
fn test_two_simultaneous_writers() {
    let dir = TempDir::new().unwrap();
    let config = TextConfig {
        path: dir.path().join("texts"),
    };

    let _writer = TextWriterService::start(&config).unwrap();
    let _another_writer = TextWriterService::open(&config);
}

#[test]
fn test_set_resource() {
    let dir = TempDir::new().unwrap();
    let config = TextConfig {
        path: dir.path().join("texts"),
    };

    let mut writer = TextWriterService::start(&config).unwrap();
    let resource = common::create_resource("shard1".to_string());
    let result = writer.set_resource(&resource);

    assert!(result.is_ok());
}

// TODO: move to another test file and refactor
#[test]
fn test_old_writer_test() -> NodeResult<()> {
    let dir = TempDir::new().unwrap();
    let fsc = TextConfig {
        path: dir.path().join("texts"),
    };

    let mut field_writer_service = TextWriterService::start(&fsc).unwrap();
    let resource1 = common::create_resource("shard1".to_string());
    let _ = field_writer_service.set_resource(&resource1);
    let _ = field_writer_service.set_resource(&resource1);

    let field_reader_service = TextReaderService::start(&fsc).unwrap();

    let reader = field_reader_service;
    let searcher = reader.reader.searcher();

    let query = TermQuery::new(
        Term::from_field_text(field_writer_service.schema.text, "document"),
        IndexRecordOption::Basic,
    );

    let (_top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(2), Count))?;
    assert_eq!(count, 1);

    let (_top_docs, count) = searcher.search(&AllQuery, &(TopDocs::with_limit(10), Count))?;
    assert_eq!(count, 2);
    Ok(())
}
