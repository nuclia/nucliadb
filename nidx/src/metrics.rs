use lazy_static::lazy_static;
use prometheus_client::{
    metrics::{counter::Counter, family::Family},
    registry::Registry,
};

macro_rules! metrics {
    {
        // Start a repetition:
        $(
            // Each repeat must contain an expression...
            $id:ident: $type:ty as $name:literal ($description:literal)
        ),
        *
    } => {
        lazy_static! {
            $(static ref $id: $type = Default::default();)*
        }

        pub fn register(metrics: &mut Registry) {
            $(metrics.register($name, $description, $id.clone());)*
        }
    };
}

metrics! {
    QUEUED_JOBS: Counter as "queued_jobs" ("Number of merge jobs in the queue")
}
