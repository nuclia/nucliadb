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
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

pub fn write_json(
    filename: String,
    values: Vec<Value>,
    merge: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if the file exists and merging is enabled
    if merge && Path::new(&filename).exists() {
        // Load existing JSON data from the file
        let file = File::open(&filename)?;
        let reader = BufReader::new(file);
        let existing_values: Vec<Value> = serde_json::from_reader(reader)?;

        // Merge existing and new values
        let mut merged_values = existing_values.clone();
        for new_value in values {
            merged_values.push(new_value);
        }

        // Write merged values back to the file
        let file = File::create(&filename)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &merged_values)?;
        writer.flush()?;
    } else {
        // Create or overwrite the file with new values
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&filename)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &values)?;
        writer.flush()?;
    }

    println!("File written: {}", filename);
    Ok(())
}
