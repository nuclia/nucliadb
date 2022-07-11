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
use nucliadb_node::telemetry::init_telemetry;
use nucliadb_node::utils::measure_time;
use nucliadb_protos::node_writer_client::NodeWriterClient;
use nucliadb_protos::EmptyQuery;
use opentelemetry::global;
use opentelemetry::global::shutdown_tracer_provider;
use opentelemetry::propagation::Injector;
use tokio::task::spawn_blocking;
use tonic::Request;
use tracing::{error_span, info, info_span, instrument, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

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
        tracing_init();
        let app_root = info_span!("app root span");
        let result = send_request().instrument(app_root).await;
        shutdown_tracer_provider();
        result
    });
    println!("response id {id}");
}

fn tracing_init() {
    let agent_endpoint = Configuration::jaeger_agent_endp();
    global::set_text_map_propagator(opentelemetry_zipkin::Propagator::new());
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_agent_endpoint(agent_endpoint)
        .with_service_name("grpc-client")
        .install_simple()
        .unwrap();
    tracing_subscriber::registry()
        .with(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(EnvFilter::from_default_env()),
        )
        .try_init()
        .unwrap();
}

#[instrument]
async fn send_request() -> String {
    let mut client = NodeWriterClient::connect("http://127.0.0.1:4446")
        .instrument(info_span!("client creation"))
        .await
        .expect("Error creating NodeWriter client");

    let req = spawn_blocking(|| {
        measure_time(
            || {
                let mut req = Request::new(EmptyQuery {});
                global::get_text_map_propagator(|prop| {
                    prop.inject_context(
                        &tracing::Span::current().context(),
                        &mut TestMetadataMap(req.metadata_mut()),
                    )
                });
                req
            },
            "injecting context to request headers",
        )
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

//#[ignore]
//#[tokio::test]
// pub async fn set_and_search_vectors() {
//    let mut writer_client = NodeWriterClient::connect("http://127.0.0.1:4446")
//        .await
//        .expect("Error creating NodeWriter client");
//    writer_client.set_resource(Resource {});
//
//    let mut reader_client = NodeReaderClient::connect("http://127.0.0.1:4445")
//        .await
//        .expect("Error creating NodeWriter client");
//
//    reader_client.vector_search(Request::new(VectorSearchRequest {
//        id: todo!(),
//        vector: todo!(),
//        tags: todo!(),
//        reload: todo!(),
//    }))
//}
