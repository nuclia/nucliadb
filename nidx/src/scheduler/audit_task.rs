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

pub trait SendReport {
    async fn send(&self, report: kb_usage::KbUsage) -> anyhow::Result<()>;
}

pub struct NatsSendReport<'a>(pub &'a async_nats::Client);

impl SendReport for NatsSendReport<'_> {
    async fn send(&self, report: kb_usage::KbUsage) -> anyhow::Result<()> {
        self.0
            .publish("kb-usage.nuclia_db", report.encode_to_vec().into())
            .await?;
        Ok(())
    }
}

/// Send storage metrics to audit, only for KB's that have been recently updated
pub async fn audit_kb_storage(
    meta: &NidxMetadata,
    nats: impl SendReport,
    last_updated: PrimitiveDateTime,
) -> anyhow::Result<Option<PrimitiveDateTime>> {
    let updated_storage = sqlx::query!(
        r#"
        SELECT
            shards.kbid,
            SUM(records) FILTER (WHERE kind = 'text')::bigint AS "fields!",
            SUM(records) FILTER (WHERE kind = 'paragraph')::bigint AS "paragraphs!",
            SUM(size_bytes)::bigint AS "bytes!",
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
        nats.send(report).await?;
        latest_update = max(latest_update, updated_kb.last_update);
    }

    Ok(Some(latest_update))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use nidx_protos::kb_usage;
    use sqlx::types::time::PrimitiveDateTime;

    use crate::{
        NidxMetadata,
        metadata::{Index, IndexConfig, Segment, Shard},
        scheduler::audit_task::audit_kb_storage,
    };

    use super::SendReport;

    struct TestSendReport(Mutex<Vec<kb_usage::KbUsage>>);

    impl SendReport for &TestSendReport {
        async fn send(&self, report: kb_usage::KbUsage) -> anyhow::Result<()> {
            self.0.lock().unwrap().push(report);
            Ok(())
        }
    }

    #[sqlx::test]
    async fn test_audit_report(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let meta = NidxMetadata::new_with_pool(pool).await.unwrap();

        // Create an index with some segments
        let kbid = uuid::Uuid::new_v4();
        let shard = Shard::create(&meta.pool, kbid).await?;
        let index_text = Index::create(&meta.pool, shard.id, "text", IndexConfig::new_text()).await?;
        let segment_text = Segment::create(&meta.pool, index_text.id, 1i64.into(), 12, serde_json::Value::Null).await?;
        segment_text.mark_ready(&meta.pool, 12345).await?;
        let index_para = Index::create(&meta.pool, shard.id, "para", IndexConfig::new_paragraph()).await?;
        let segment_para = Segment::create(&meta.pool, index_para.id, 1i64.into(), 99, serde_json::Value::Null).await?;
        segment_para.mark_ready(&meta.pool, 56789).await?;

        // Should send a report
        let reporter = TestSendReport(Mutex::new(Vec::new()));
        let since = PrimitiveDateTime::MIN.replace_year(2000)?;
        let since = audit_kb_storage(&meta, &reporter, since).await?.unwrap();

        let reports = reporter.0.lock().unwrap().clone();
        reporter.0.lock().unwrap().clear();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].kb_id, Some(kbid.to_string()));
        let storage = reports[0].storage.unwrap();
        assert_eq!(storage.fields, Some(12));
        assert_eq!(storage.paragraphs, Some(99));
        assert_eq!(storage.bytes, Some(12345 + 56789));

        // No changes, no report sent
        let new_date = audit_kb_storage(&meta, &reporter, since).await?;
        assert!(new_date.is_none());
        assert!(reporter.0.lock().unwrap().is_empty());

        // Update a segment, report is sent
        let segment_para = Segment::create(&meta.pool, index_para.id, 2i64.into(), 22, serde_json::Value::Null).await?;
        segment_para.mark_ready(&meta.pool, 4444).await?;
        Index::updated(&meta.pool, &index_para.id).await?;
        let since = audit_kb_storage(&meta, &reporter, since).await?.unwrap();

        let reports = reporter.0.lock().unwrap().clone();
        reporter.0.lock().unwrap().clear();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].kb_id, Some(kbid.to_string()));
        let storage = reports[0].storage.unwrap();
        assert_eq!(storage.fields, Some(12));
        assert_eq!(storage.paragraphs, Some(99 + 22));
        assert_eq!(storage.bytes, Some(12345 + 56789 + 4444));

        // No changes, no report sent
        let new_date = audit_kb_storage(&meta, &reporter, since).await?;
        assert!(new_date.is_none());
        assert!(reporter.0.lock().unwrap().is_empty());

        Ok(())
    }
}
