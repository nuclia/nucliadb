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
use std::time::SystemTime;

use nucliadb_core::protos::Resource;
use nucliadb_core::vectors::VectorWriter;
use nucliadb_vectors::data_point::DataPointPin;
use nucliadb_vectors::{
    config::VectorConfig,
    data_point::{self, open, NoDLog},
    service::VectorWriterService,
    VectorR,
};

pub struct VectorIndexer;

impl VectorIndexer {
    pub fn new() -> Self {
        VectorIndexer
    }

    pub fn index_resource<'a>(&self, output_dir: &Path, resource: &'a Resource) -> VectorR<(i64, &'a Vec<String>)> {
        let tmp = tempfile::tempdir()?;

        // Index resource
        let mut writer =
            VectorWriterService::create(&tmp.path().join("index"), "Don't care".into(), VectorConfig::default())
                .unwrap();
        writer.set_resource(resource.into()).unwrap();

        // Copy just the segment to the output directory
        let segments = writer.get_segment_ids().unwrap();
        if segments.is_empty() {
            return Ok((0, &resource.sentences_to_delete));
        }
        assert!(segments.len() <= 1, "Expected a single segment");
        std::fs::rename(tmp.path().join("index").join(&segments[0]), output_dir)?;

        return Ok((writer.count().unwrap() as i64, &resource.sentences_to_delete));
    }

    pub fn merge(&self, work_dir: &Path, segments: &[i64]) -> VectorR<String> {
        // Rename (nucliadb_vectors wants uuid, we use i64 as segment ids) and open the segments
        let segment_ids: Vec<_> = segments
            .iter()
            .map(|s| {
                let uuid = uuid::Uuid::new_v4();
                std::fs::rename(work_dir.join(s.to_string()), work_dir.join(uuid.to_string())).unwrap();
                uuid
            })
            .map(|dpid| {
                let dp = data_point::DataPointPin::open_pin(work_dir, dpid).unwrap();
                let open_dp = open(&dp).unwrap();
                (NoDLog, open_dp)
            })
            .collect();

        // Do the merge
        let destination = DataPointPin::create_pin(work_dir).unwrap();
        nucliadb_vectors::data_point::merge(
            &destination,
            &segment_ids.iter().map(|(a, b)| (a, b)).collect::<Vec<_>>(),
            &VectorConfig::default(),
            SystemTime::now(),
        )?;

        Ok(destination.id().to_string())
    }
}
