// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

pub mod grpc;
pub mod shards;

use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{Settings, grpc_server::GrpcServer};

pub async fn run(settings: Settings, shutdown: CancellationToken) -> anyhow::Result<()> {
    let service = grpc::ApiServer::new(&settings).into_router();
    let server = GrpcServer::new("0.0.0.0:10000").await?;
    debug!("Running API at port {}", server.port()?);
    server.serve(service, shutdown).await?;

    Ok(())
}
