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

use std::fs::OpenOptions;
use std::path::Path;

use filetime::FileTime;
use nucliadb_core::tracing::error;

use crate::settings::Settings;

pub struct ReplicationHealthManager {
    settings: Settings,
}

const REPLICATION_HEALTH_FILE: &str = "replication_marker";

impl ReplicationHealthManager {
    /// Simple health check right now to communicate between
    /// 2 processes that share the same disk.
    /// We simply touch the file and see when it was last
    /// modified to decide if we're healthy and running up-to-date.
    pub fn new(settings: Settings) -> Self {
        Self { settings }
    }

    fn health_filepath(&self) -> String {
        format!(
            "{}/{}",
            self.settings.data_path().to_string_lossy(),
            REPLICATION_HEALTH_FILE
        )
    }

    pub fn update_healthy(&self) {
        if OpenOptions::new()
            .write(true)
            .create(true)
            .open(self.health_filepath())
            .is_err()
        {
            error!("Error updating replication health status");
            return;
        }
        // Update the modified time of the file
        filetime::set_file_mtime(self.health_filepath(), FileTime::now()).unwrap();
    }

    pub fn healthy(&self) -> bool {
        if !Path::new(&self.health_filepath()).exists() {
            return false;
        }
        let metaddata = Path::new(&self.health_filepath()).metadata();
        if metaddata.is_err() {
            return false;
        }
        let metadata = metaddata.unwrap();
        metadata.modified().unwrap()
            > std::time::SystemTime::now()
                - std::time::Duration::from_secs(self.settings.replication_healthy_delay())
    }
}
