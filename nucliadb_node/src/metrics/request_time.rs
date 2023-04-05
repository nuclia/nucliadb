use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
enum RequestActor {
    Shard,
    Vectors,
    Paragraphs,
    Texts,
    Relations,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RequestTimeKey {
    actor: RequestActor,
    request: String,
}

pub type RequestTimeValue = f64;

impl RequestTimeKey {
    fn new(actor: RequestActor, request: String) -> RequestTimeKey {
        RequestTimeKey { actor, request }
    }
    pub fn shard(request: String) -> RequestTimeKey {
        Self::new(RequestActor::Shard, request)
    }
    pub fn vectors(request: String) -> RequestTimeKey {
        Self::new(RequestActor::Vectors, request)
    }
    pub fn paragraphs(request: String) -> RequestTimeKey {
        Self::new(RequestActor::Paragraphs, request)
    }
    pub fn texts(request: String) -> RequestTimeKey {
        Self::new(RequestActor::Texts, request)
    }
    pub fn relations(request: String) -> RequestTimeKey {
        Self::new(RequestActor::Relations, request)
    }
}

pub(super) type RequestTimeMetric = Family<RequestTimeKey, Histogram>;
const BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

pub(super) fn register_request_time(registry: &mut Registry) -> RequestTimeMetric {
    let constructor = || Histogram::new(BUCKETS.iter().copied());
    let metric = RequestTimeMetric::new_with_constructor(constructor);
    registry.register(
        "node_requests",
        "Time an actor took to process the given request",
        metric.clone(),
    );
    metric
}
