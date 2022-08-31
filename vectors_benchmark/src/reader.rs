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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use super::plot_writer::PlotWriter;
use super::query_iter::QueryIter;
use super::VectorEngine;

pub fn read_benchmark<Eng>(
    stop_point: Arc<AtomicBool>,
    no_results: usize,
    engine: Eng,
    mut plotw: PlotWriter,
    queries: QueryIter,
) where
    Eng: VectorEngine,
{
    let mut iter = queries.enumerate();
    while !stop_point.load(Ordering::SeqCst) {
        let (x, query) = iter.next().unwrap();
        let now = SystemTime::now();
        engine.search(no_results, &query);
        let tick = now.elapsed().unwrap().as_millis();
        plotw.add(x, tick).unwrap();
        std::thread::sleep(Duration::from_millis(100));
    }
}
