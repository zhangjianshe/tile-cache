use crate::model::{CleanupHistoryEntry, CleanupRun, CleanupSettings, NewCleanupHistory, Page};
use anyhow::{Context, Result};
use serde::Serialize;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};
use std::{
    collections::BTreeMap,
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const RETAIN_HOURS: u64 = 24 * 90;
const CLEANUP_HISTORY_RETENTION_SECONDS: u64 = 90 * 86_400;

#[derive(Clone)]
pub struct AccessStats {
    inner: Arc<StatsInner>,
}

struct StatsInner {
    state: Mutex<StatsState>,
    database: SqlitePool,
}

#[derive(Default)]
struct StatsState {
    buckets: BTreeMap<u64, HourBucket>,
    pending: BTreeMap<u64, HourBucket>,
}

#[derive(Clone, Default)]
struct HourBucket {
    get_requests: u64,
    put_requests: u64,
    hits: u64,
    misses: u64,
    bytes_read: u64,
    bytes_written: u64,
}

#[derive(Debug, Serialize)]
pub struct HourStats {
    pub hour_epoch: u64,
    pub get_requests: u64,
    pub put_requests: u64,
    pub hits: u64,
    pub misses: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

impl AccessStats {
    pub async fn open(config_dir: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(config_dir)
            .await
            .with_context(|| format!("create config directory {}", config_dir.display()))?;
        let path = config_dir.join("tile-cache-meta.db");
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let database = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS hourly_metrics(hour_epoch INTEGER PRIMARY KEY,get_requests INTEGER NOT NULL DEFAULT 0,put_requests INTEGER NOT NULL DEFAULT 0,hits INTEGER NOT NULL DEFAULT 0,misses INTEGER NOT NULL DEFAULT 0,bytes_read INTEGER NOT NULL DEFAULT 0,bytes_written INTEGER NOT NULL DEFAULT 0)")
            .execute(&database)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS service_settings(key TEXT PRIMARY KEY,value INTEGER NOT NULL)")
            .execute(&database)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS cleanup_runs(id INTEGER PRIMARY KEY AUTOINCREMENT,completed_at INTEGER NOT NULL,retention_days INTEGER NOT NULL,deleted_databases INTEGER NOT NULL,error TEXT)")
            .execute(&database)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS cleanup_history(id INTEGER PRIMARY KEY AUTOINCREMENT,completed_at INTEGER NOT NULL,operation TEXT NOT NULL,database_id TEXT NOT NULL,database_name TEXT NOT NULL DEFAULT '',item_id TEXT,item_name TEXT,layer_count INTEGER NOT NULL DEFAULT 0,tile_count INTEGER NOT NULL DEFAULT 0,bytes INTEGER NOT NULL DEFAULT 0,actor TEXT NOT NULL,success INTEGER NOT NULL,error TEXT)")
            .execute(&database).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_cleanup_history_completed ON cleanup_history(completed_at DESC,id DESC)")
            .execute(&database).await?;
        let oldest = current_hour().saturating_sub(RETAIN_HOURS);
        let rows = sqlx::query("SELECT hour_epoch,get_requests,put_requests,hits,misses,bytes_read,bytes_written FROM hourly_metrics WHERE hour_epoch>=? ORDER BY hour_epoch")
            .bind(oldest as i64)
            .fetch_all(&database)
            .await?;
        let mut buckets = BTreeMap::new();
        for row in rows {
            buckets.insert(
                row.get::<i64, _>(0) as u64,
                HourBucket {
                    get_requests: row.get::<i64, _>(1) as u64,
                    put_requests: row.get::<i64, _>(2) as u64,
                    hits: row.get::<i64, _>(3) as u64,
                    misses: row.get::<i64, _>(4) as u64,
                    bytes_read: row.get::<i64, _>(5) as u64,
                    bytes_written: row.get::<i64, _>(6) as u64,
                },
            );
        }
        Ok(Self {
            inner: Arc::new(StatsInner {
                state: Mutex::new(StatsState {
                    buckets,
                    pending: BTreeMap::new(),
                }),
                database,
            }),
        })
    }

    pub fn record_get(&self, hit: bool, bytes: usize) {
        self.update(|bucket| {
            bucket.get_requests += 1;
            if hit {
                bucket.hits += 1;
                bucket.bytes_read += bytes as u64;
            } else {
                bucket.misses += 1;
            }
        });
    }

    pub fn record_put(&self, bytes: usize) {
        self.update(|bucket| {
            bucket.put_requests += 1;
            bucket.bytes_written += bytes as u64;
        });
    }

    pub fn recent(&self, hours: u64) -> Vec<HourStats> {
        let current = current_hour();
        let start = current.saturating_sub(hours.saturating_sub(1));
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        (start..=current)
            .map(|hour| {
                let bucket = state.buckets.get(&hour).cloned().unwrap_or_default();
                HourStats {
                    hour_epoch: hour * 3_600,
                    get_requests: bucket.get_requests,
                    put_requests: bucket.put_requests,
                    hits: bucket.hits,
                    misses: bucket.misses,
                    bytes_read: bucket.bytes_read,
                    bytes_written: bucket.bytes_written,
                }
            })
            .collect()
    }

    pub async fn load_cleanup_settings(
        &self,
        defaults: CleanupSettings,
    ) -> Result<CleanupSettings> {
        let rows = sqlx::query("SELECT key,value FROM service_settings")
            .fetch_all(&self.inner.database)
            .await?;
        let mut settings = defaults;
        for row in rows {
            let key: String = row.get(0);
            let value = row.get::<i64, _>(1).max(0) as u64;
            match key.as_str() {
                "retention_days" => settings.retention_days = value,
                "cleanup_hour" => settings.cleanup_hour = value.min(23) as u8,
                "shard_idle_seconds" => settings.shard_idle_seconds = value,
                _ => {}
            }
        }
        Ok(settings)
    }

    pub async fn save_cleanup_settings(&self, settings: &CleanupSettings) -> Result<()> {
        let mut transaction = self.inner.database.begin().await?;
        for (key, value) in [
            ("retention_days", settings.retention_days),
            ("cleanup_hour", settings.cleanup_hour as u64),
            ("shard_idle_seconds", settings.shard_idle_seconds),
        ] {
            sqlx::query("INSERT INTO service_settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
                .bind(key)
                .bind(value as i64)
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn record_cleanup(&self, run: &CleanupRun) -> Result<()> {
        sqlx::query("INSERT INTO cleanup_runs(completed_at,retention_days,deleted_databases,error) VALUES(?,?,?,?)")
            .bind(run.completed_at.unwrap_or_default() as i64)
            .bind(run.retention_days as i64)
            .bind(run.deleted_databases as i64)
            .bind(&run.error)
            .execute(&self.inner.database)
            .await?;
        Ok(())
    }

    pub async fn last_cleanup(&self) -> Result<Option<CleanupRun>> {
        let row = sqlx::query("SELECT completed_at,retention_days,deleted_databases,error FROM cleanup_runs ORDER BY id DESC LIMIT 1")
            .fetch_optional(&self.inner.database)
            .await?;
        Ok(row.map(|row| CleanupRun {
            completed_at: Some(row.get::<i64, _>(0).max(0) as u64),
            retention_days: row.get::<i64, _>(1).max(0) as u64,
            deleted_databases: row.get::<i64, _>(2).max(0) as u64,
            error: row.get(3),
        }))
    }

    pub async fn record_cleanup_history(&self, entry: &NewCleanupHistory) -> Result<()> {
        let mut transaction = self.inner.database.begin().await?;
        sqlx::query("INSERT INTO cleanup_history(completed_at,operation,database_id,database_name,item_id,item_name,layer_count,tile_count,bytes,actor,success,error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(entry.completed_at as i64).bind(&entry.operation).bind(&entry.database_id)
            .bind(&entry.database_name).bind(&entry.item_id).bind(&entry.item_name)
            .bind(entry.layer_count as i64).bind(entry.tile_count as i64).bind(entry.bytes as i64)
            .bind(&entry.actor).bind(i64::from(entry.success)).bind(&entry.error)
            .execute(&mut *transaction).await?;
        let cutoff = entry
            .completed_at
            .saturating_sub(CLEANUP_HISTORY_RETENTION_SECONDS);
        sqlx::query("DELETE FROM cleanup_history WHERE completed_at<?")
            .bind(cutoff as i64)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn cleanup_history(
        &self,
        page: u64,
        page_size: u64,
    ) -> Result<Page<CleanupHistoryEntry>> {
        let total = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM cleanup_history")
            .fetch_one(&self.inner.database)
            .await?
            .max(0) as u64;
        let rows = sqlx::query("SELECT id,completed_at,operation,database_id,database_name,item_id,item_name,layer_count,tile_count,bytes,actor,success,error FROM cleanup_history ORDER BY completed_at DESC,id DESC LIMIT ? OFFSET ?")
            .bind(page_size as i64).bind(((page - 1) * page_size) as i64)
            .fetch_all(&self.inner.database).await?;
        Ok(Page {
            items: rows
                .into_iter()
                .map(|row| CleanupHistoryEntry {
                    id: row.get(0),
                    completed_at: row.get::<i64, _>(1).max(0) as u64,
                    operation: row.get(2),
                    database_id: row.get(3),
                    database_name: row.get(4),
                    item_id: row.get(5),
                    item_name: row.get(6),
                    layer_count: row.get::<i64, _>(7).max(0) as u64,
                    tile_count: row.get::<i64, _>(8).max(0) as u64,
                    bytes: row.get::<i64, _>(9).max(0) as u64,
                    actor: row.get(10),
                    success: row.get::<i64, _>(11) != 0,
                    error: row.get(12),
                })
                .collect(),
            page,
            page_size,
            total,
        })
    }

    pub async fn flush(&self) -> Result<()> {
        let pending = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            std::mem::take(&mut state.pending)
        };
        if pending.is_empty() {
            return Ok(());
        }
        let result = self.write_pending(&pending).await;
        if result.is_err() {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            for (hour, delta) in pending {
                merge(state.pending.entry(hour).or_default(), &delta);
            }
        }
        result
    }

    async fn write_pending(&self, pending: &BTreeMap<u64, HourBucket>) -> Result<()> {
        let mut transaction = self.inner.database.begin().await?;
        for (hour, bucket) in pending {
            sqlx::query("INSERT INTO hourly_metrics(hour_epoch,get_requests,put_requests,hits,misses,bytes_read,bytes_written) VALUES(?,?,?,?,?,?,?) ON CONFLICT(hour_epoch) DO UPDATE SET get_requests=get_requests+excluded.get_requests,put_requests=put_requests+excluded.put_requests,hits=hits+excluded.hits,misses=misses+excluded.misses,bytes_read=bytes_read+excluded.bytes_read,bytes_written=bytes_written+excluded.bytes_written")
                .bind(*hour as i64)
                .bind(bucket.get_requests as i64)
                .bind(bucket.put_requests as i64)
                .bind(bucket.hits as i64)
                .bind(bucket.misses as i64)
                .bind(bucket.bytes_read as i64)
                .bind(bucket.bytes_written as i64)
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        let oldest = current_hour().saturating_sub(RETAIN_HOURS);
        sqlx::query("DELETE FROM hourly_metrics WHERE hour_epoch < ?")
            .bind(oldest as i64)
            .execute(&self.inner.database)
            .await?;
        Ok(())
    }

    fn update(&self, update: impl Fn(&mut HourBucket)) {
        let current = current_hour();
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        update(state.buckets.entry(current).or_default());
        update(state.pending.entry(current).or_default());
        let oldest = current.saturating_sub(RETAIN_HOURS);
        state.buckets.retain(|hour, _| *hour >= oldest);
    }
}

fn merge(target: &mut HourBucket, source: &HourBucket) {
    target.get_requests += source.get_requests;
    target.put_requests += source.put_requests;
    target.hits += source.hits;
    target.misses += source.misses;
    target.bytes_read += source.bytes_read;
    target.bytes_written += source.bytes_written;
}

fn current_hour() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 3_600
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn persists_and_restores_current_hour() {
        let directory = tempfile::tempdir().unwrap();
        let stats = AccessStats::open(directory.path()).await.unwrap();
        stats.record_get(true, 12);
        stats.record_get(false, 0);
        stats.record_put(34);
        stats.flush().await.unwrap();
        drop(stats);

        let restored = AccessStats::open(directory.path()).await.unwrap();
        let current = restored.recent(1).pop().unwrap();
        assert_eq!(current.get_requests, 2);
        assert_eq!(current.hits, 1);
        assert_eq!(current.misses, 1);
        assert_eq!(current.put_requests, 1);
        assert_eq!(current.bytes_read, 12);
        assert_eq!(current.bytes_written, 34);
    }

    #[tokio::test]
    async fn persists_cleanup_settings_and_run() {
        let directory = tempfile::tempdir().unwrap();
        let stats = AccessStats::open(directory.path()).await.unwrap();
        let settings = CleanupSettings {
            retention_days: 14,
            cleanup_hour: 3,
            shard_idle_seconds: 180,
        };
        stats.save_cleanup_settings(&settings).await.unwrap();
        stats
            .record_cleanup(&CleanupRun {
                completed_at: Some(1234),
                retention_days: 14,
                deleted_databases: 2,
                error: None,
            })
            .await
            .unwrap();

        let restored = stats
            .load_cleanup_settings(CleanupSettings {
                retention_days: 7,
                cleanup_hour: 4,
                shard_idle_seconds: 300,
            })
            .await
            .unwrap();
        assert_eq!(restored.retention_days, 14);
        assert_eq!(restored.cleanup_hour, 3);
        assert_eq!(restored.shard_idle_seconds, 180);
        let run = stats.last_cleanup().await.unwrap().unwrap();
        assert_eq!(run.completed_at, Some(1234));
        assert_eq!(run.deleted_databases, 2);
    }

    #[tokio::test]
    async fn persists_and_pages_cleanup_history() {
        let directory = tempfile::tempdir().unwrap();
        let stats = AccessStats::open(directory.path()).await.unwrap();
        stats
            .record_cleanup_history(&NewCleanupHistory {
                completed_at: 2_000_000_000,
                operation: "delete_layer".into(),
                database_id: "database-id".into(),
                database_name: "测试数据库".into(),
                item_id: Some("layer-id".into()),
                item_name: Some("测试图层".into()),
                layer_count: 1,
                tile_count: 42,
                bytes: 4096,
                actor: "administrator".into(),
                success: true,
                error: None,
            })
            .await
            .unwrap();
        drop(stats);

        let restored = AccessStats::open(directory.path()).await.unwrap();
        let page = restored.cleanup_history(1, 20).await.unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.items[0].database_name, "测试数据库");
        assert_eq!(page.items[0].item_name.as_deref(), Some("测试图层"));
        assert_eq!(page.items[0].tile_count, 42);
        assert_eq!(page.items[0].bytes, 4096);
    }
}
