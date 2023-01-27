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

#![warn(clippy::pedantic)]

use std::path::Path;
use std::{fs, io};

use itertools::Itertools;

fn main() -> io::Result<()> {
    println!("cargo:rerun-if-changed=stop_words");

    let stop_words = fs::read_dir("./stop_words")?
        // first filters out nested directories
        .filter_ok(|entry| entry.file_type().map_or(false, |entry| entry.is_file()))
        // then transforms JSON array into a list of strings
        .map(|entry| {
            let content_file = fs::read_to_string(entry?.path())?;
            let stop_words = serde_json::from_str(&content_file)?;

            Ok(stop_words)
        })
        .flatten_ok::<Vec<String>, io::Error>()
        .collect::<Result<Vec<_>, _>>()?;

    fs::write(
        Path::new("./src/stop_words.rs"),
        format!(
            "pub fn is_stop_word(x:&str) -> bool {{\n {} \n}}",
            stop_words
                .into_iter()
                .map(|word| format!(r#"x == "{word}""#))
                .join("\n|| ")
        ),
    )?;

    Ok(())
}
