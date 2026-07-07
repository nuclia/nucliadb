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

use nidx_protos::nidx_api_client::NidxApiClient;
use nidx_protos::{NewShardRequest, VectorIndexConfig};

use std::collections::HashMap;
use tonic::Request;
use tonic::transport::Channel;
use uuid::Uuid;

pub async fn create_shards(api_client: &mut NidxApiClient<Channel>, count: usize) -> anyhow::Result<Vec<Uuid>> {
    let mut shards = Vec::with_capacity(count);
    for _ in 0..count {
        let response = api_client
            .new_shard(Request::new(NewShardRequest {
                kbid: "aabbccddeeff11223344556677889900".to_string(),
                vectorsets_configs: HashMap::from([(
                    "english".to_string(),
                    VectorIndexConfig {
                        vector_dimension: Some(3),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }))
            .await?;
        shards.push(uuid::Uuid::parse_str(&response.into_inner().id).unwrap());
    }
    Ok(shards)
}
