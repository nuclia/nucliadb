use std::io::Cursor;

use nucliadb_node::reader::NodeReaderService;
use nucliadb_node::writer::NodeWriterService;
use nucliadb_protos::*;
use prost::Message;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut writer = NodeWriterService::new();
    let mut reader = NodeReaderService::new();

    let resources_dir = std::path::Path::new("/path/to/data");
    let new_shard = writer.new_shard().await;
    let shard_id = ShardId {
        id: new_shard.id.clone(),
    };
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
        let info = reader.get_shard(&shard_id).await.unwrap().get_info().await;
        println!("Sentences {}", info.sentences);
        println!("Paragraphs {}", info.paragraphs);
        println!("resources {}", info.resources);
    }

    Ok(())
}
