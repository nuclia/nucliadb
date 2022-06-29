use nucliadb_node::writer::NodeWriterService;
use nucliadb_protos::*;
use prost::Message;
use std::io::Cursor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let resources_dir = std::path::Path::new("/Users/ramon/tmp/testing_resources");
    let shard_id = ShardId {
        id: "d7f64176-6bd4-4a9a-a85d-1deb68ee9d9d".to_string(),
    };
    let mut writer = NodeWriterService::new();
    let _shard = writer.get_shard(&shard_id).await.unwrap();
    assert!(resources_dir.exists());
    for file_path in std::fs::read_dir(&resources_dir).unwrap() {
        let file_path = file_path.unwrap().path();
        println!("processing {file_path:?}");
        let content = std::fs::read(&file_path).unwrap();
        let resource = Resource::decode(&mut Cursor::new(content)).unwrap();
        println!("Adding resource {}", file_path.display());
        let res = writer.set_resource(&shard_id, &resource).await.unwrap();
        assert!(res.is_ok());
        println!("Resource added: {}", res.unwrap());
    }

    Ok(())
}
