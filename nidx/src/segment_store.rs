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

use std::path::PathBuf;
use std::{path::Path, sync::Arc};

use futures::TryStreamExt;
use object_store::DynObjectStore;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::SyncIoBridge;
use tracing::*;

use crate::metadata::SegmentId;

/// Adapter that implements a sync Writer trait and writes to an AsyncWrite while counting bytes
struct WriteCounter<T> {
    writer: SyncIoBridge<T>,
    counter: usize,
}
impl<T> std::io::Write for WriteCounter<T>
where
    T: tokio::io::AsyncWrite + Unpin,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes = self.writer.write(buf)?;
        self.counter += bytes;
        Ok(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
impl<T> WriteCounter<T>
where
    T: tokio::io::AsyncWrite + Unpin,
{
    fn new(writer: T) -> Self {
        let writer = SyncIoBridge::new(writer);
        Self { writer, counter: 0 }
    }
    fn finish(&mut self) -> std::io::Result<usize> {
        self.writer.shutdown()?;
        Ok(self.counter)
    }
}

#[instrument(skip(storage))]
pub async fn pack_and_upload(
    storage: Arc<DynObjectStore>,
    local_path: &Path,
    store_path: object_store::path::Path,
) -> anyhow::Result<usize> {
    let mut upload = WriteCounter::new(object_store::buffered::BufWriter::new(storage, store_path));
    let local_path = local_path.to_path_buf();
    let size = tokio::task::spawn_blocking(move || -> anyhow::Result<usize> {
        let mut tar = tar::Builder::new(&mut upload);
        tar.mode(tar::HeaderMode::Deterministic);
        tar.append_dir_all(".", local_path)?;
        tar.finish()?;
        drop(tar);

        let bytes = upload.finish()?;
        Ok(bytes)
    })
    .await??;

    Ok(size)
}

const BUF_SIZE: usize = 1_000_000;

pub async fn download_segment(
    storage: Arc<DynObjectStore>,
    segment_id: SegmentId,
    output_dir: PathBuf,
) -> anyhow::Result<()> {
    // Create a temp directory to download this segment
    let temp_dir = PathBuf::from(&format!("{}.tmp", output_dir.to_str().unwrap()));
    if tokio::fs::try_exists(&temp_dir).await? {
        tokio::fs::remove_dir_all(&temp_dir).await?;
    }

    let response = storage.get(&segment_id.storage_key()).await?.into_stream();
    let reader = response.map_err(std::io::Error::from).into_async_read().compat();

    // Setting a buffer in the async and sync sides of the bridge, since it can be costly to switch contexts
    let bufreader = tokio::io::BufReader::with_capacity(BUF_SIZE, reader);
    let reader = std::io::BufReader::with_capacity(BUF_SIZE, SyncIoBridge::new(bufreader));

    let temp_dir2 = temp_dir.clone();
    let mut tar = tar::Archive::new(reader);
    let result = tokio::task::spawn_blocking(move || tar.unpack(temp_dir2)).await?;

    if let Err(e) = result {
        let _ = tokio::fs::remove_dir_all(temp_dir).await;
        return Err(e.into());
    } else {
        tokio::fs::rename(temp_dir, output_dir).await?;
    }

    Ok(())
}
