use std::path::Path;
use std::sync::Arc;

use std::thread;
use std::time::{Duration, Instant, SystemTime};

use nucliadb_core::protos::prost::Message;
use nucliadb_core::protos::Resource;
use nucliadb_node::metadb::MetaDB;
use nucliadb_vectors::data_point::{self, merge, open, DataPointPin};
use nucliadb_vectors::data_point_provider::reader::SetDLog;
use object_store::local::LocalFileSystem;
use object_store::{path, ObjectStore};
use rand::Rng;
use tempfile::tempdir;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tokio_tar::Archive;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let meta = MetaDB::new().await?;

    loop {
        thread::sleep(Duration::from_millis(rand::thread_rng().gen_range(500..1500)));
        let working_path = tempdir()?;
        let (segments, deletions) = meta.list_segments_and_deletions().await?;

        // Merge time is the latest opstamp we read deletions or segments for
        let merge_time = segments.iter().chain(deletions.iter()).map(|(_, t)| t).max().unwrap();

        if segments.len() <= 1 {
            println!("Not enough to merge");
            tokio::time::sleep(Duration::from_millis(1000)).await;
            continue;
        }

        let t = Instant::now();
        println!("Merging {} segments", segments.len());
        let mut tasks = JoinSet::new();
        let storage = Arc::new(LocalFileSystem::new_with_prefix(Path::new("/tmp/shards/shard/objects"))?);
        for s in segments.clone() {
            let wp = working_path.path().to_owned();
            let storage = storage.clone();
            tasks.spawn(async move {
                let stream = storage.get(&object_store::path::Path::from(s.0.as_str())).await.unwrap().into_stream();
                let reader = tokio_util::io::StreamReader::new(stream);
                let mut unarchiver = Archive::new(reader);
                unarchiver.unpack(wp.join(&s.0)).await.unwrap();
            });
        }
        while tasks.join_next().await.is_some() {}
        println!("Downloaded");

        let inputs: Vec<_> = segments
            .iter()
            .map(|sid| {
                let pin = DataPointPin::open_pin(working_path.path(), uuid::Uuid::parse_str(&sid.0).unwrap()).unwrap();
                let delete_log = SetDLog {
                    deleted: deletions.iter().filter(|(_, t)| t > &sid.1).map(|(a, _)| a.as_bytes().to_vec()).collect(),
                };
                (delete_log, open(&pin).unwrap())
            })
            .collect();
        let ii: Vec<_> = inputs.iter().map(|(d, dp)| (d, dp)).collect();
        let output = DataPointPin::create_pin(working_path.path())?;
        let out = output.id();

        println!("Ready to merge");
        merge(&output, ii.as_slice(), nucliadb_vectors::data_point::Similarity::Dot, SystemTime::now())?;

        // Archive & upload
        let uploader = object_store::buffered::BufWriter::new(storage.clone(), out.to_string().into());
        let mut archive = tokio_tar::Builder::new(uploader);
        archive.append_dir_all(".", working_path.path().join(out.to_string())).await?;
        archive.finish().await?;
        let mut x = archive.into_inner().await?;
        x.shutdown().await?;
        println!("Uploaded");

        meta.replace_segments(segments.iter().map(|x| x.0.clone()).collect(), out.to_string(), *merge_time).await?;

        for s in segments {
            storage.delete(&path::Path::from(s.0)).await?;
        }
        println!("Took {:?}", t.elapsed());
    }

    Ok(())
}
