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
}
