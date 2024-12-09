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

macro_rules! metric_definition {
    ($id:ident: $type:path{$new:expr} as $name:literal ($description:literal)) => {
        lazy_static! {
            pub static ref $id: $type = Family::new_with_constructor(|| Histogram::new($new));
        }
    };
    ($id:ident: $type:path[$new:expr] as $name:literal ($description:literal)) => {
        lazy_static! {
            pub static ref $id: $type = Histogram::new($new);
        }
    };
    ($id:ident: $type:ty as $name:literal ($description:literal)) => {
        lazy_static! {
            pub static ref $id: $type = Default::default();
        }
    };
}

macro_rules! metrics {
    {
        $(
            $id:ident: $type:ty$([$new:expr])?$({$new_family:expr})? as $name:literal ($description:literal)
        ),*$(,)?
    } => {
        use lazy_static::lazy_static;
        use prometheus_client::registry::Registry;


        $(metric_definition!($id: $type$([$new])?$({$new_family})? as $name ($description));)*

        pub fn register(metrics: &mut Registry) {
            $(metrics.register($name, $description, $id.clone());)*
        }
    };
}

use std::fmt::Write;

use crate::metadata::IndexKind;
use prometheus_client::encoding::EncodeLabelValue;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IndexKindLabel(IndexKind);
impl EncodeLabelValue for IndexKindLabel {
    fn encode(&self, encoder: &mut prometheus_client::encoding::LabelValueEncoder) -> Result<(), std::fmt::Error> {
        let txt = match self.0 {
            IndexKind::Text => "text",
            IndexKind::Paragraph => "paragraph",
            IndexKind::Vector => "vector",
            IndexKind::Relation => "relation",
        };
        encoder.write_str(txt)
    }
}

pub mod scheduler {
    use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
    use prometheus_client::metrics::{family::Family, gauge::Gauge};

    #[derive(Clone, Debug, EncodeLabelValue, PartialEq, Eq, Hash)]
    pub enum JobState {
        Queued,
        Running,
    }

    #[derive(Clone, Debug, EncodeLabelSet, PartialEq, Eq, Hash)]
    pub struct JobFamily {
        pub state: JobState,
    }

    metrics! {
        QUEUED_JOBS: Family<JobFamily, Gauge> as "merge_jobs" ("Number of merge jobs in different states"),
    }
}

pub mod searcher {
    use std::sync::atomic::AtomicU64;

    use prometheus_client::{
        encoding::EncodeLabelSet,
        metrics::{
            family::Family,
            gauge::Gauge,
            histogram::{exponential_buckets, Histogram},
        },
    };

    use crate::metadata::IndexKind;

    use super::IndexKindLabel;

    #[derive(Clone, Debug, EncodeLabelSet, PartialEq, Eq, Hash)]
    pub struct IndexKindLabels {
        kind: IndexKindLabel,
    }

    impl IndexKindLabels {
        pub fn new(kind: IndexKind) -> Self {
            Self {
                kind: IndexKindLabel(kind),
            }
        }
    }

    metrics! {
        SYNC_DELAY: Gauge::<f64, AtomicU64> as "searcher_sync_delay_seconds" ("Delay between the time the last index was updated and it was synced"),
        SYNC_FAILED_INDEXES: Gauge as "searcher_sync_failed_indexes" ("Number of indexes failing to sync"),
        REFRESH_QUEUE_LEN: Gauge as "searcher_indexes_pending_refresh" ("Number of indexes waiting to be refreshed"),
        INDEX_LOAD_TIME: Family<IndexKindLabels, Histogram>{exponential_buckets(0.001, 2.0, 12)} as "searcher_index_load_time_seconds" ("Time to load index searchers"),
    }
}
