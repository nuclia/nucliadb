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

mod index_report;
mod layer_coherence_check;

pub fn checks(path: &std::path::Path, output: &std::path::Path) {
    let index = crate::index::Index::writer(path);
    let no_nodes = index.no_nodes();
    println!("Starting checks, {no_nodes} nodes");
    println!("Checking layer coherence");
    layer_coherence_check::check(&index);
    println!("All checks passed");
    println!("Building reports");
    index_report::generate_report(&index, output);
    println!("Reports can be found at {}", output.display());
}
