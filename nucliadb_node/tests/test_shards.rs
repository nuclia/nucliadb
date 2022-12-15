use nucliadb_node;
use nucliadb_protos::{EmptyQuery, ShardId};

mod common;

use common::{node_writer_client, node_writer_server};
use tonic::Request;


#[tokio::test]
async fn test_create_shard() -> Result<(), Box<dyn std::error::Error>> {
    node_writer_server().await;
    let mut writer = node_writer_client().await;

    let new_shard_response = writer
        .new_shard(Request::new(EmptyQuery {}))
        .await?;
    let shard_id = &new_shard_response.get_ref().id;

    let response = writer
        .get_shard(Request::new(ShardId {
            id: shard_id.clone()
        }))
        .await?;

    assert_eq!(shard_id, &response.get_ref().id);

    Ok(())
}
