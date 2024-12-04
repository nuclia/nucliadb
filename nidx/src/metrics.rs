macro_rules! metrics {
    {
        // Start a repetition:
        $(
            // Each repeat must contain an expression...
            $id:ident: $type:ty as $name:literal ($description:literal)
        ),
        *
    } => {
        use lazy_static::lazy_static;
        use prometheus_client::registry::Registry;

        lazy_static! {
            $(pub static ref $id: $type = Default::default();)*
        }

        pub fn register(metrics: &mut Registry) {
            $(metrics.register($name, $description, $id.clone());)*
        }
    };
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
        QUEUED_JOBS: Family<JobFamily, Gauge> as "merge_jobs" ("Number of merge jobs in diffeerent states")
    }
}

pub mod searcher {
    use std::sync::atomic::AtomicU64;

    use prometheus_client::metrics::gauge::Gauge;

    metrics! {
        SYNC_DELAY: Gauge::<f64, AtomicU64> as "searcher_sync_delay_seconds" ("Seconds of delay in syncing indexes")
    }
}
