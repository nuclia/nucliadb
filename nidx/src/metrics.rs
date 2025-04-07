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

        #[allow(unused_imports)]
        use std::sync::atomic::AtomicU64;

        #[allow(unused_imports)]
        use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};

        #[allow(unused_imports)]
        use prometheus_client::metrics::{
            counter::Counter,
            gauge::Gauge,
            family::Family,
            histogram::{exponential_buckets, Histogram},
        };

        #[allow(unused_imports)]
        use super::*;

        $(metric_definition!($id: $type$([$new])?$({$new_family})? as $name ($description));)*

        pub fn register(metrics: &mut Registry) {
            $(metrics.register($name, $description, $id.clone());)*
        }
    };
}

use std::fmt::Write;

use crate::metadata::IndexKind;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};

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

#[derive(Clone, Debug, PartialEq, Eq, Hash, EncodeLabelValue)]
enum OperationStatus {
    Success,
    Failure,
}

#[derive(Clone, Debug, EncodeLabelSet, PartialEq, Eq, Hash)]
pub struct OperationStatusLabels {
    status: OperationStatus,
}

impl OperationStatusLabels {
    pub fn failure() -> Self {
        Self {
            status: OperationStatus::Failure,
        }
    }

    pub fn success() -> Self {
        Self {
            status: OperationStatus::Success,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, EncodeLabelValue)]
enum Utilization {
    Free,
    Busy,
}

#[derive(Clone, Debug, EncodeLabelSet, PartialEq, Eq, Hash)]
pub struct UtilizationLabels {
    status: Utilization,
}

impl From<bool> for UtilizationLabels {
    fn from(busy: bool) -> Self {
        if busy {
            Self {
                status: Utilization::Busy,
            }
        } else {
            Self {
                status: Utilization::Free,
            }
        }
    }
}

pub mod common {
    metrics! {
        SPAN_DURATION: Family<Vec<(String, String)>, Histogram>{exponential_buckets(0.001, 2.0, 20)} as "span_duration_seconds" ("Duration of a tracing span"),
    }
}

pub mod scheduler {
    #[derive(Clone, Debug, EncodeLabelValue, PartialEq, Eq, Hash)]
    pub enum JobState {
        Queued,
        RecentlyQueued,
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
    metrics! {
        SYNC_DELAY: Gauge::<f64, AtomicU64> as "searcher_sync_delay_seconds" ("Delay between the time the last index was updated and it was synced"),
        SYNC_FAILED_INDEXES: Gauge as "searcher_sync_failed_indexes" ("Number of indexes failing to sync"),
        REFRESH_QUEUE_LEN: Gauge as "searcher_indexes_pending_refresh" ("Number of indexes waiting to be refreshed"),
        INDEX_LOAD_TIME: Family<IndexKindLabels, Histogram>{exponential_buckets(0.001, 2.0, 12)} as "searcher_index_load_time_seconds" ("Time to load index searchers"),

        SHARD_SELECTOR_TIME: Histogram[exponential_buckets(0.001, 2.0, 8)] as "searcher_shard_selector_time_seconds" ("Time to select shards to sync"),
        ACTIVE_SHARDS: Gauge as "searcher_shards_active" ("Number of active shards in this searcher"),
        EVICTED_SHARDS: Gauge as "searcher_shards_evicted" ("Number of evicted shards (pending deletion) in this searcher"),
        DESIRED_SHARDS: Gauge as "searcher_shards_desired" ("Number of shards desired by this searcher"),

        INDEX_CACHE_COUNT: Gauge as "searcher_index_cache_count" ("Number of indexes in the searcher cache"),
        INDEX_CACHE_BYTES: Gauge as "searcher_index_cache_size_bytes" ("Total size of open indexes in the searcher cache"),
    }
}

pub mod indexer {
    metrics! {
        INDEXING_COUNTER: Family<OperationStatusLabels, Counter> as "indexer_message_count" ("Number of indexing operations per status"),
        TOTAL_INDEXING_TIME: Histogram[exponential_buckets(0.01, 2.0, 12)] as "total_indexing_time_seconds" ("Time it took to process an entire index message"),
        PER_INDEX_INDEXING_TIME: Family<IndexKindLabels, Histogram>{exponential_buckets(0.01, 2.0, 12)} as "indexing_time_seconds" ("Time it took to index a resource to an index"),
        INDEXING_BUSY: Family<UtilizationLabels, Counter::<f64, AtomicU64>> as "indexer_utilization_seconds" ("Time spent in free/busy status"),
    }
}

pub mod worker {
    metrics! {
        MERGE_COUNTER: Family<OperationStatusLabels, Counter> as "merge_job_count" ("Number of merge jobs per status"),
        PER_INDEX_MERGE_TIME: Family<IndexKindLabels, Histogram>{exponential_buckets(1.0, 2.0, 12)} as "merge_time_seconds" ("Time it took to run a merge job"),
        WORKER_BUSY: Family<UtilizationLabels, Counter::<f64, AtomicU64>> as "merge_worker_utilization_seconds" ("Time spent in free/busy status"),
    }
}
