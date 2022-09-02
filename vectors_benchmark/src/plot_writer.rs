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

use std::fs::File;
use std::io;
use std::io::Write;
use std::time::SystemTime;

pub struct PlotWriter {
    start: SystemTime,
    idx: usize,
    file: File,
}

impl PlotWriter {
    pub fn new(file: File) -> PlotWriter {
        PlotWriter {
            start: SystemTime::now(),
            idx: 0,
            file,
        }
    }
    pub fn add(&mut self) -> io::Result<()> {
        let x = self.idx;
        let y = self.start.elapsed().unwrap().as_millis();
        self.idx += 1;
        writeln!(self.file, "{x} {y}")
    }
}

impl super::Logger for PlotWriter {
    fn report(&mut self) -> crate::cli::BenchR<()> {
        Ok(self.add()?)
    }
}
