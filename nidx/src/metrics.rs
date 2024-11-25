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
        use prometheus_client::{
            metrics::{gauge::Gauge, family::Family},
            registry::Registry,
        };

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
