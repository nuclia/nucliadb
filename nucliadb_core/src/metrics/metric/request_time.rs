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

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

use super::prometheus_metric_observer::PrometheusMetricObserver;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RequestTimeKey {
    actor: RequestActor,
    request: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
enum RequestActor {
    Shard,
    Vectors,
    Paragraphs,
    Texts,
    Relations,
}

pub type RequestTimeValue = f64;

type RequestCount = Family<RequestTimeKey, Counter>;
type RequestDuration = Family<RequestTimeKey, Histogram>;

const BUCKETS: [f64; 12] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 2.5, 5.0, 10.0, 30.0, 60.0,
];

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

pub fn register_request_time(registry: &mut Registry) -> PrometheusMetricObserver<RequestTimeKey> {
    let count_metric = RequestCount::default();
    registry.register(
        "request_count",
        "Number of times an actor executed the given request",
        count_metric.clone(),
    );

    let constructor = || Histogram::new(BUCKETS.iter().copied());
    let duration_metric = RequestDuration::new_with_constructor(constructor);
    registry.register(
        "request_duration",
        "Time an actor took to process the given request",
        duration_metric.clone(),
    );

    let observer = PrometheusMetricObserver::new(count_metric, duration_metric);

    observer
}
