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
// This program is distributed in the hope th&mut at it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

use dotenvy;
use nucliadb_node::telemetry::init_telemetry;
use nucliadb_protos::node_writer_client::NodeWriterClient;
use nucliadb_protos::EmptyQuery;
use opentelemetry::propagation::Injector;
use opentelemetry::sdk::trace::Span;
use opentelemetry::trace::Tracer;
use opentelemetry::{global, Context};
use opentelemetry_jaeger::Propagator;
use tonic::metadata::{AsciiMetadataKey, MetadataMap};
use tonic::Request;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub struct TestMetadataMap<'a>(pub &'a mut MetadataMap);

impl<'a> Injector for TestMetadataMap<'a> {
    fn set(&mut self, key: &str, value: String) {
        let key = AsciiMetadataKey::from_bytes(key.to_ascii_lowercase().as_bytes()).unwrap();
        self.0.append(key, value.parse().unwrap());
    }
}

#[ignore] // just for local testing jaeger tracing
#[tokio::test]
pub async fn create_shard() {
    println!("{}", dotenvy::dotenv().unwrap().display());
    init_telemetry().unwrap();
    let span = ("creating client");

    let mut client = NodeWriterClient::connect("http://127.0.0.1:4446")
        .instrument(span)
        .await
        .expect("Error creating NodeWriter client");

    let mut req = Request::new(EmptyQuery {});
    let mut ctx_prop = TestMetadataMap(req.metadata_mut());
    println!("current context {:#?}", Context::current());

    global::get_text_map_propagator(|prop| prop.inject(&mut ctx_prop));

    let span = tracing::info_span!("sending request");
    let response = client
        .new_shard(req)
        .instrument(span)
        .await
        .expect("Error in new_shard request");

    global::shutdown_tracer_provider();
    println!("response id {}", response.get_ref().id);
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
