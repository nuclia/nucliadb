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

use prometheus_client::{
    encoding::{EncodeLabelSet, EncodeLabelValue},
    metrics::{
        family::Family,
        histogram::{exponential_buckets, Histogram},
    },
    registry::Registry,
};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum MergeSource {
    High,
    Low,
    Idle,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MergeLabels {
    source: MergeSource,
}

pub type MergeHistogram = Family<MergeLabels, Histogram>;

pub struct VectorsMetrics {
    merge_time: MergeHistogram,
    input_segment_size: MergeHistogram,
    output_segment_size: MergeHistogram,
}

impl VectorsMetrics {
    pub fn new(registry: &mut Registry) -> Self {
        let metrics = Self {
            merge_time: MergeHistogram::new_with_constructor(|| {
                // 0.1s to < 1000s
                Histogram::new(exponential_buckets(0.1, f64::powf(10.0, 1.0 / 3.0), 13))
            }),
            input_segment_size: MergeHistogram::new_with_constructor(|| {
                // 10 to 1.000.000 nodes
                Histogram::new(exponential_buckets(10.0, f64::powf(10.0, 1.0 / 2.0), 13))
            }),
            output_segment_size: MergeHistogram::new_with_constructor(|| {
                // 10 to 1.000.000 nodes
                Histogram::new(exponential_buckets(10.0, f64::powf(10.0, 1.0 / 2.0), 13))
            }),
        };

        registry.register("merge_time_seconds", "Vectors merge operation time", metrics.merge_time.clone());
        registry.register(
            "merge_input_segment_size",
            "Vectors per segment to merge",
            metrics.input_segment_size.clone(),
        );
        registry.register(
            "merge_output_segment_size",
            "Vectors per merged segment",
            metrics.output_segment_size.clone(),
        );

        metrics
    }

    pub fn record_time(&self, source: MergeSource, seconds: f64) {
        self.merge_time
            .get_or_create(&MergeLabels {
                source,
            })
            .observe(seconds);
    }

    pub fn record_input_segment(&self, source: MergeSource, vectors: usize) {
        self.input_segment_size
            .get_or_create(&MergeLabels {
                source,
            })
            .observe(vectors as f64);
    }

    pub fn record_output_segment(&self, source: MergeSource, vectors: usize) {
        self.output_segment_size
            .get_or_create(&MergeLabels {
                source,
            })
            .observe(vectors as f64);
    }
}
