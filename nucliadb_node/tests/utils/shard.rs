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
use tracing::info_span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

    let id = rt.block_on(send_request());
    println!("response id {id}");
}

fn tracing_init() {
    let agent_endpoint = Configuration::jaeger_agent_endp();
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_agent_endpoint(agent_endpoint)
        .with_service_name("grpc-client")
        .install_simple()
        .unwrap();
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()
        .unwrap();
}

async fn send_request() -> String {
    tracing_init();
    let span = info_span!("test parent link");
    let _guard = span.enter();

    let mut client = NodeWriterClient::connect("http://127.0.0.1:4446")
        .await
        .expect("Error creating NodeWriter client");

    let mut req = Request::new(EmptyQuery {});
    global::get_text_map_propagator(|prop| {
        prop.inject_context(
            &tracing::Span::current().context(),
            &mut TestMetadataMap(req.metadata_mut()),
        )
    });

    let response = client
        .new_shard(req)
        .await
        .expect("Error in new_shard request");
    let id = response.get_ref().id.clone();
    shutdown_tracer_provider();
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
