use std::fs::{File, OpenOptions};
use std::io::Seek;
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
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use nidx_types::Seq;
use nucliadb_core::protos::{Resource, VectorSearchRequest};
use nucliadb_core::vectors::VectorWriter;
use nucliadb_vectors::data_point::DataPointPin;
use nucliadb_vectors::data_point_provider;
use nucliadb_vectors::data_point_provider::reader::{Reader, TimeSensitiveDLog};
use nucliadb_vectors::data_point_provider::state::write_state;
use nucliadb_vectors::formula::Formula;
use nucliadb_vectors::{
    config::VectorConfig,
    data_point::{self, open},
    service::VectorWriterService,
    VectorR,
};
use tempfile::{tempdir, TempDir};

pub struct VectorIndexer;

impl VectorIndexer {
    pub fn index_resource(&self, output_dir: &Path, resource: &Resource) -> VectorR<(i64, Vec<String>)> {
        let tmp = tempfile::tempdir()?;

        // Index resource
        let mut writer =
            VectorWriterService::create(&tmp.path().join("index"), "Don't care".into(), VectorConfig::default())
                .unwrap();
        writer.set_resource(resource.into()).unwrap();

        // Copy just the segment to the output directory
        let segments = writer.get_segment_ids().unwrap();
        if segments.is_empty() {
            return Ok((0, resource.sentences_to_delete.clone()));
        }
        assert!(segments.len() <= 1, "Expected a single segment");
        std::fs::rename(tmp.path().join("index").join(&segments[0]), output_dir)?;

        Ok((writer.count().unwrap() as i64, resource.sentences_to_delete.clone()))
    }

    pub fn merge(
        &self,
        work_dir: &Path,
        segments: &[(PathBuf, Seq, i64)],
        deletions: &Vec<(Seq, &Vec<String>)>,
    ) -> VectorR<(String, usize)> {
        // TODO: Maybe segments should not get a DTrie of deletions and just a hashset of them, and we can handle building that here?
        // Wait and see how the Tantivy indexes turn out
        let mut delete_log = data_point_provider::DTrie::new();
        for d in deletions {
            let time = SystemTime::UNIX_EPOCH + Duration::from_secs(i64::from(&d.0) as u64);
            for k in d.1 {
                delete_log.insert(k.as_bytes(), time);
            }
        }

        // Rename (nucliadb_vectors wants uuid, we use i64 as segment ids) and open the segments
        let segment_ids: Vec<_> = segments
            .iter()
            .map(|(segment_path, seq, _)| {
                let uuid = uuid::Uuid::new_v4();
                std::fs::rename(segment_path, work_dir.join(uuid.to_string())).unwrap();
                (uuid, seq)
            })
            .map(|(dpid, seq)| {
                let dp = data_point::DataPointPin::open_pin(work_dir, dpid).unwrap();
                let open_dp = open(&dp).unwrap();
                (
                    TimeSensitiveDLog {
                        dlog: &delete_log,
                        time: SystemTime::UNIX_EPOCH + Duration::from_secs(i64::from(seq) as u64),
                    },
                    open_dp,
                )
            })
            .collect();

        // Do the merge
        let destination = DataPointPin::create_pin(work_dir).unwrap();
        let open_destination = nucliadb_vectors::data_point::merge(
            &destination,
            &segment_ids.iter().map(|(a, b)| (a, b)).collect::<Vec<_>>(),
            &VectorConfig::default(),
            SystemTime::now(),
        )?;

        Ok((destination.id().to_string(), open_destination.journal().no_nodes()))
    }
}

pub struct VectorSearcher {
    _index_dir: TempDir,
    reader: data_point_provider::reader::Reader,
}

impl VectorSearcher {
    pub fn new(
        segments_dir: &Path,
        index_id: i64,
        segments: Vec<(i64, i64)>,
        deletions: Vec<(i64, String)>,
    ) -> VectorR<Self> {
        let index_dir = tempdir()?;
        let mut index_state = data_point_provider::state::State::default();

        for (segment_id, seq) in segments {
            // Give it a uuid for a name
            let uuid = uuid::Uuid::new_v4();
            let segment_path = segments_dir.join(format!("{index_id}/{segment_id}"));
            std::os::unix::fs::symlink(&segment_path, index_dir.path().join(uuid.to_string()))?;
            index_state.data_point_list.push(uuid);

            // Tweak time in journal
            let mut journal_file =
                OpenOptions::new().read(true).write(true).open(&segment_path.join("journal.json"))?;
            let mut journal: serde_json::Value = serde_json::from_reader(&journal_file)?;
            journal["ctime"]["secs_since_epoch"] = serde_json::Value::from(seq);
            journal["ctime"]["nanos_since_epoch"] = serde_json::Value::from(0);
            journal_file.seek(std::io::SeekFrom::Start(0))?;
            serde_json::to_writer(&mut journal_file, &journal)?;
            let len = journal_file.stream_position()?;
            journal_file.set_len(len)?;
            drop(journal_file);
        }

        for (seq, key) in deletions {
            index_state
                .delete_log
                .insert(key.as_bytes(), SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(seq as u64));
        }

        // Write state
        let mut state_file = File::create(index_dir.path().join("state.bincode"))?;
        write_state(&mut state_file, &index_state)?;
        drop(state_file);

        // Open reader using the recently created data
        let reader = Reader::open(index_dir.path())?;

        Ok(VectorSearcher {
            _index_dir: index_dir,
            reader,
        })
    }

    pub fn dummy_search(&self) -> VectorR<usize> {
        Ok(self
            .reader
            .search(
                &(
                    20,
                    &VectorSearchRequest {
                        vector: Vec::from(&[0.1; 1024]),
                        min_score: -10.0,
                        with_duplicates: true,
                        ..Default::default()
                    },
                    Formula::new(),
                ),
                &None,
            )?
            .len())
    }
}
