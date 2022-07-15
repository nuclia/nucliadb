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

use nucliadb_node::config::Configuration;
use nucliadb_protos::node_writer_client::NodeWriterClient;
use nucliadb_protos::EmptyQuery;
use opentelemetry::global;
use opentelemetry::global::shutdown_tracer_provider;
use opentelemetry::propagation::Injector;
use tonic::Request;
use tracing::{event, info_span, instrument, span, Instrument, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{FilterFn, Targets};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

struct TestMetadataMap<'a>(&'a mut tonic::metadata::MetadataMap);

impl<'a> Injector for TestMetadataMap<'a> {
    /// Set a key and value in the MetadataMap.  Does nothing if the key or value are not valid
    /// inputs
    #[allow(deprecated)]
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(val) = tonic::metadata::MetadataValue::from_str(&value) {
                self.0.insert(key, val);
            }
        }
    }
}

// just for local testing jaeger tracing
#[ignore]
#[test]
pub fn create_shard() {
    println!("{}", dotenvy::dotenv().unwrap().display());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let id = rt.block_on(async {
        test_tracing_init();
        let span = info_span!("send request instr - app root span");
        let result = send_request().await;
        drop(span);
        shutdown_tracer_provider();
        result
    });
    println!("response id {id}");
}

fn test_tracing_init() {
    let agent_endpoint = Configuration::jaeger_agent_endp();
    global::set_text_map_propagator(opentelemetry_zipkin::Propagator::new());
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_agent_endpoint(agent_endpoint)
        .with_service_name("grpc-client")
        .install_simple()
        .unwrap();

    let filter = FilterFn::new(|metadata| {
        metadata
            .file()
            .filter(|file| file.contains("nucliadb_node"))
            .map(|_| metadata.is_event())
            .map(|state| state && metadata.fields().field("trace_marker").is_none())
            .map(|state| !state)
            .unwrap_or_default()
    });

    let log_levels = Configuration::log_level();
    tracing_subscriber::registry()
        .with(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(Targets::new().with_targets(log_levels))
                .with_filter(filter),
        )
        .try_init()
        .unwrap();
}

#[instrument(name = "send_reques")]
async fn send_request() -> String {
    event!(Level::INFO, trace_marker = true, "test event for jaeger",);
    let mut client = NodeWriterClient::connect("http://127.0.0.1:4446")
        .instrument(info_span!("client creation"))
        .await
        .expect("Error creating NodeWriter client");

    let span = tracing::Span::current();
    let req = tokio::task::spawn_blocking(move || {
        span!(parent: &span, Level::INFO, "internal inject").in_scope(|| {
            let mut req = Request::new(EmptyQuery {});
            global::get_text_map_propagator(|prop| {
                prop.inject_context(
                    &tracing::Span::current().context(),
                    &mut TestMetadataMap(req.metadata_mut()),
                )
            });
            req
        })
    })
    .await
    .unwrap();

    let response = client
        .new_shard(req)
        .instrument(info_span!("new shard request send"))
        .await
        .expect("Error in new_shard request");
    let id = response.get_ref().id.clone();
    id
}
