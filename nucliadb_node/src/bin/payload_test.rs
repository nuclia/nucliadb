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

use std::io::Cursor;

use nucliadb_node::reader::NodeReaderService;
use nucliadb_node::writer::NodeWriterService;
use nucliadb_protos::*;
<<<<<<< Updated upstream:nucliadb_node/src/bin/payload_test.rs
=======
use nucliadb_service_interface::prelude::*;
use nucliadb_service_interface::vectos_interface::VectorServiceConfiguration;
use nucliadb_vectors::service::{VectorWriterService, VectorReaderService};
>>>>>>> Stashed changes:nucliadb_vectors/src/sic/analysis.rs
use prost::Message;
#[tokio::main]
<<<<<<< Updated upstream:nucliadb_node/src/bin/payload_test.rs
async fn main() -> anyhow::Result<()> {
    let mut writer = NodeWriterService::new();
    let mut reader = NodeReaderService::new();

    let resources_dir = std::path::Path::new("/path/to/data");
    let new_shard = writer.new_shard();
    let shard_id = ShardId { id: new_shard.id };
    assert!(resources_dir.exists());
    for file_path in std::fs::read_dir(&resources_dir).unwrap() {
=======
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new("payload_dir").unwrap();
    let vsc = VectorServiceConfiguration {
        no_results: Some(10),
        path: dir.path().to_str().unwrap().to_string(),
    };
    let mut writer = VectorWriterService::start(&vsc).await.unwrap();
    let reader = VectorReaderService::start(&vsc).await.unwrap();
    let payload_dir = std::path::Path::new("/Users/hermegarcia/RustWorkspace/data");
    assert!(payload_dir.exists());
    for file_path in std::fs::read_dir(&payload_dir).unwrap() {
>>>>>>> Stashed changes:nucliadb_vectors/src/sic/analysis.rs
        let file_path = file_path.unwrap().path();
        println!("processing {file_path:?}");
        let content = std::fs::read(&file_path).unwrap();
        let resource = Resource::decode(&mut Cursor::new(content)).unwrap();
        println!("Adding resource {}", file_path.display());
        let res = writer.set_resource(&shard_id, &resource).unwrap();
        assert!(res.is_ok());
        println!("Resource added: {}", res.unwrap());
        let info = reader.get_shard(&shard_id).unwrap().get_info();
        println!("Sentences {}", info.sentences);
        println!("Paragraphs {}", info.paragraphs);
        println!("resources {}", info.resources);
    }
    reader.reload();
    println!("No vectors: {}", reader.count());
    Ok(())
}
