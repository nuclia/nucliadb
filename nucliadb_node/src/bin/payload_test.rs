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

use nucliadb_core::protos::*;
use nucliadb_core::NodeResult;
use nucliadb_node::env;
use nucliadb_node::shards::metadata::ShardMetadata;
use nucliadb_node::shards::providers::unbounded_cache::{
    UnboundedShardReaderCache, UnboundedShardWriterCache,
};
use nucliadb_node::shards::providers::{ShardReaderProvider, ShardWriterProvider};
use prost::Message;

fn main() -> NodeResult<()> {
    let writer = UnboundedShardWriterCache::new(env::shards_path());
    let reader = UnboundedShardReaderCache::new(env::shards_path());
    let resources_dir = std::path::Path::new("/path/to/data");

    let metadata = ShardMetadata::from(NewShardRequest::default());
    let new_shard = writer.create(metadata)?;
    let shard_id = ShardId { id: new_shard.id };
    assert!(resources_dir.exists());
    for file_path in std::fs::read_dir(resources_dir).unwrap() {
        let file_path = file_path.unwrap().path();
        println!("processing {file_path:?}");

        let content = std::fs::read(&file_path).unwrap();
        let resource = Resource::decode(&mut Cursor::new(content)).unwrap();
        println!("Adding resource {}", file_path.display());

        writer.load(shard_id.id.clone())?;
        let shard_writer = writer.get(shard_id.id.clone()).unwrap();
        let res = shard_writer.set_resource(&resource);
        assert!(res.is_ok());
        println!("Resource added: {:?}", res.unwrap());

        reader.load(shard_id.id.clone())?;
        let shard_reader = reader.get(shard_id.id.clone()).unwrap();
        let info = shard_reader.get_info(&GetShardRequest::default())?;
        println!("Sentences {}", info.sentences);
        println!("Paragraphs {}", info.paragraphs);
        println!("Fields {}", info.fields);
    }
    Ok(())
}
