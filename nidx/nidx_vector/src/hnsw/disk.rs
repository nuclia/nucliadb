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

mod v1;
mod v2;

use crate::{VectorR, hnsw::RAMHnsw};
use std::{any::Any, path::Path};

pub use v1::DiskHnswV1;
pub use v2::DiskHnswV2;

pub fn open_disk_hnsw(path: &Path, prewarm: bool) -> VectorR<Box<dyn DiskHnsw>> {
    let v2 = DiskHnswV2::open(path, prewarm);
    if let Ok(v2) = v2 {
        return Ok(Box::new(v2));
    };
    let v1 = DiskHnswV1::open(path, prewarm)?;
    Ok(Box::new(v1))
}

pub trait DiskHnsw: Sync + Send {
    fn deserialize(&self) -> VectorR<RAMHnsw>;
    fn size(&self) -> usize;
    fn as_any(&self) -> &dyn Any;
}

#[cfg(test)]
mod tests {
    use std::sync::RwLock;

    use tempfile::TempDir;

    use crate::{
        VectorAddr,
        hnsw::{
            DiskHnsw, DiskHnswV1, DiskHnswV2, RAMHnsw,
            ram_hnsw::{EntryPoint, RAMLayer},
        },
    };

    #[test]
    fn upgrade_from_v1_to_v2() -> anyhow::Result<()> {
        let layer0 = RAMLayer {
            out: [
                (
                    VectorAddr(0),
                    RwLock::new(vec![(VectorAddr(1), 0.5), (VectorAddr(2), 0.2)]),
                ),
                (
                    VectorAddr(1),
                    RwLock::new(vec![(VectorAddr(0), 0.5), (VectorAddr(1), 0.7)]),
                ),
                (VectorAddr(2), RwLock::new(vec![(VectorAddr(0), 0.2)])),
            ]
            .into_iter()
            .collect(),
        };
        let layer1 = RAMLayer {
            out: [(VectorAddr(0), RwLock::new(vec![]))].into_iter().collect(),
        };
        let mut graph = RAMHnsw::new();
        graph.layers.push(layer0);
        graph.layers.push(layer1);
        graph.entry_point = EntryPoint {
            node: VectorAddr(0),
            layer: 1,
        };

        let v1dir = TempDir::new()?;
        DiskHnswV1::serialize_to(v1dir.path(), 3, &graph)?;

        let v1 = DiskHnswV1::open(v1dir.path(), false)?;
        let loaded_v1 = v1.deserialize()?;

        let v2dir = TempDir::new()?;
        DiskHnswV2::serialize_to(v2dir.path(), 3, &loaded_v1)?;

        let v2 = DiskHnswV2::open(v2dir.path(), false)?;
        let loaded_v2 = v2.deserialize()?;

        assert_eq!(loaded_v1.entry_point, loaded_v2.entry_point);
        assert_eq!(loaded_v1.layers.len(), loaded_v2.layers.len());
        for l in 0..loaded_v1.layers.len() {
            let layer_v1 = &loaded_v1.layers[l];
            let layer_v2 = &loaded_v2.layers[l];
            assert_eq!(layer_v1.out.keys().len(), layer_v2.out.keys().len());
            for n in loaded_v1.layers[l].out.keys() {
                let node_v1 = layer_v1.out[n].read().unwrap();
                let node_v2 = layer_v2.out[n].read().unwrap();
                assert_eq!(*node_v1, *node_v2);
            }
        }

        Ok(())
    }
}
