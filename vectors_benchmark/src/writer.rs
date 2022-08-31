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

use std::io::Read;
use std::time::SystemTime;

use super::plot_writer::PlotWriter;
use super::vector_iter::VectorIter;
use super::VectorEngine;

pub fn write_benchmark<Eng, Cnt>(
    batch_size: usize,
    mut engine: Eng,
    mut plotw: PlotWriter,
    vectors: VectorIter<Cnt>,
) where
    Eng: VectorEngine,
    Cnt: Read,
{
    let mut kbatch = vec![];
    let mut vbatch = vec![];
    let mut batch_num = 0;
    let mut batch_id = format!("Batch{batch_num}");
    for (x, vector) in vectors.enumerate() {
        kbatch.push(format!("{batch_id}/{x}"));
        vbatch.push(vector);
        if vbatch.len() == batch_size {
            let now = SystemTime::now();
            engine.add_batch(batch_id, kbatch, vbatch);
            let tick = now.elapsed().unwrap().as_millis();
            plotw.add(x, tick).unwrap();
            batch_num += 1;
            batch_id = format!("Batch{batch_num}");
            kbatch = vec![];
            vbatch = vec![];
        }
    }
}
