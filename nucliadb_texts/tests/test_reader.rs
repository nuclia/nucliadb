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

use nucliadb_core::prelude::*;
use nucliadb_texts::reader::TextReaderService;
use nucliadb_texts::writer::TextWriterService;
use tempfile::TempDir;

#[test]
fn test_start_new_reader_after_a_writer() {
    let dir = TempDir::new().unwrap();
    let config = TextConfig {
        path: dir.path().join("texts"),
    };

    let _writer = TextWriterService::start(&config).unwrap();
    let reader = TextReaderService::start(&config);
    assert!(reader.is_ok());
}

#[test]
fn test_open_multiple_readers() {
    let dir = TempDir::new().unwrap();
    let config = TextConfig {
        path: dir.path().join("texts"),
    };

    let _writer = TextWriterService::start(&config).unwrap();
    let reader1 = TextReaderService::start(&config);
    let reader2 = TextReaderService::start(&config);
    let reader3 = TextReaderService::start(&config);
    assert!(reader1.is_ok());
    assert!(reader2.is_ok());
    assert!(reader3.is_ok());
}

#[test]
fn test_start_new_reader_before_a_writer() {
    let dir = TempDir::new().unwrap();
    let config = TextConfig {
        path: dir.path().join("texts"),
    };

    let reader = TextReaderService::start(&config);
    assert!(reader.is_err());
}
