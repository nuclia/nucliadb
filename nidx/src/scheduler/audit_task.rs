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

use crate::NidxMetadata;
use nidx_protos::kb_usage::{self, KbSource, Service};
use nidx_protos::prost::*;

use sqlx::types::time::PrimitiveDateTime;
use std::{cmp::max, time::SystemTime};

/// Purge segments that have not been ready for a while:
/// - Uploads that failed
/// - Recent deletions
pub async fn audit_kb_storage(
    meta: &NidxMetadata,
    nats: &async_nats::Client,
    last_updated: PrimitiveDateTime,
) -> anyhow::Result<Option<PrimitiveDateTime>> {
    let updated_storage = sqlx::query!(
        r#"
        SELECT
            shards.kbid,
            SUM(records) FILTER (WHERE kind = 'text') AS "fields!: i64",
            SUM(records) FILTER (WHERE kind = 'paragraph') AS "paragraphs!: i64",
            SUM(size_bytes) AS "bytes!: i64",
            MAX(updated_at) AS "last_update!"
        FROM segments
        JOIN indexes ON segments.index_id = indexes.id
        JOIN shards ON indexes.shard_id = shards.id
        WHERE shards.id IN (SELECT shard_id FROM indexes WHERE updated_at > $1)
        GROUP BY shards.kbid;"#,
        last_updated
    )
    .fetch_all(&meta.pool)
    .await?;

    if updated_storage.is_empty() {
        return Ok(None);
    }

    let mut latest_update = updated_storage[0].last_update;

    for updated_kb in updated_storage {
        let report = kb_usage::KbUsage {
            service: Service::NucliaDb.into(),
            timestamp: Some(SystemTime::now().into()),
            account_id: None,
            kb_id: Some(updated_kb.kbid.into()),
            kb_source: KbSource::Hosted.into(),
            activity_log_match: None,
            storage: Some(kb_usage::Storage {
                paragraphs: Some(updated_kb.paragraphs as u64),
                fields: Some(updated_kb.fields as u64),
                resources: None,
                bytes: Some(updated_kb.bytes as u64),
            }),
            ..Default::default()
        };
        nats.publish("kb-usage.nuclia_db", report.encode_to_vec().into()).await?;
        latest_update = max(latest_update, updated_kb.last_update);
    }

    Ok(Some(latest_update))
}
