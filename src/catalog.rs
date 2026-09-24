use crate::{
    error::ApiError,
    model::{DatabaseInfo, Page, TilesetInfo},
    store::{PutOutcome, TileStore},
};
use anyhow::{Context, Result};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};
use std::{
    collections::HashMap,
    path::Path,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot, Mutex};

#[derive(Clone)]
pub struct Catalog {
    database: SqlitePool,
    write_lock: Arc<Mutex<()>>,
    access_touches: Arc<Mutex<HashMap<String, Instant>>>,
    put_sender: mpsc::Sender<CatalogCommand>,
}

enum CatalogCommand {
    Put(CatalogPut),
    Flush(oneshot::Sender<Result<()>>),
}

struct CatalogPut {
    database: String,
    item: String,
    database_name: Option<String>,
    item_name: Option<String>,
    path: String,
    format: String,
    z: i64,
    x: i64,
    y: i64,
    bytes: usize,
    outcome: PutOutcome,
    reply: Option<oneshot::Sender<Result<()>>>,
}

impl Catalog {
    pub async fn open(config_dir: &Path, store: TileStore) -> Result<Self> {
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
            .max_connections(2)
            .connect_with(options)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS cache_databases(database_id TEXT PRIMARY KEY,display_name TEXT NOT NULL DEFAULT '',path TEXT NOT NULL DEFAULT '',bytes INTEGER NOT NULL DEFAULT 0,tile_count INTEGER NOT NULL DEFAULT 0,layer_count INTEGER NOT NULL DEFAULT 0,last_access_at INTEGER,updated_at INTEGER NOT NULL)")
            .execute(&database).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS cache_layers(database_id TEXT NOT NULL,item_id TEXT NOT NULL,display_name TEXT NOT NULL DEFAULT '',format TEXT NOT NULL,min_zoom INTEGER,max_zoom INTEGER,min_x INTEGER,max_x INTEGER,min_y INTEGER,max_y INTEGER,tile_count INTEGER NOT NULL DEFAULT 0,bytes INTEGER NOT NULL DEFAULT 0,updated_at INTEGER NOT NULL,PRIMARY KEY(database_id,item_id))")
            .execute(&database).await?;
        ensure_column(
            &database,
            "cache_databases",
            "revocable",
            "INTEGER NOT NULL DEFAULT 1",
        )
        .await?;
        ensure_column(
            &database,
            "cache_layers",
            "revocable",
            "INTEGER NOT NULL DEFAULT 1",
        )
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_cache_layers_database ON cache_layers(database_id)",
        )
        .execute(&database)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS catalog_state(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
        )
        .execute(&database)
        .await?;
        let write_lock = Arc::new(Mutex::new(()));
        let (put_sender, put_receiver) = mpsc::channel(65_536);
        tokio::spawn(catalog_writer_loop(
            database.clone(),
            write_lock.clone(),
            put_receiver,
            store,
        ));
        Ok(Self {
            database,
            write_lock,
            access_touches: Arc::new(Mutex::new(HashMap::new())),
            put_sender,
        })
    }

    pub async fn initialized(&self) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM catalog_state WHERE key='initialized'",
        )
        .fetch_one(&self.database)
        .await?
            > 0)
    }

    pub async fn rebuild(&self, store: &TileStore) -> Result<(usize, usize), ApiError> {
        self.rebuild_with_progress(store, |_, _, _, _| {}).await
    }

    pub async fn rebuild_with_progress<F>(
        &self,
        store: &TileStore,
        mut progress: F,
    ) -> Result<(usize, usize), ApiError>
    where
        F: FnMut(usize, usize, usize, usize),
    {
        self.flush_pending().await.map_err(ApiError::Internal)?;
        let _guard = self.write_lock.lock().await;
        let databases = store.list_databases().await?;
        let total_databases = databases.len();
        let mut total_layers = 0;
        for database in &databases {
            total_layers += store.tileset_count(&database.database).await?;
        }
        progress(0, total_databases, 0, total_layers);
        let mut scanned = Vec::with_capacity(databases.len());
        let mut processed_layers = 0;
        for database in databases {
            let layers = store.tilesets(&database.database).await?;
            processed_layers += layers.len();
            scanned.push((database, layers));
            progress(
                scanned.len(),
                total_databases,
                processed_layers,
                total_layers,
            );
        }
        let old_databases =
            sqlx::query("SELECT database_id,display_name,revocable FROM cache_databases")
                .fetch_all(&self.database)
                .await?
                .into_iter()
                .map(|row| {
                    (
                        row.get::<String, _>(0),
                        (row.get::<String, _>(1), row.get::<i64, _>(2) != 0),
                    )
                })
                .collect::<HashMap<_, _>>();
        let old_layers =
            sqlx::query("SELECT database_id,item_id,display_name,revocable FROM cache_layers")
                .fetch_all(&self.database)
                .await?
                .into_iter()
                .map(|row| {
                    (
                        (row.get::<String, _>(0), row.get::<String, _>(1)),
                        (row.get::<String, _>(2), row.get::<i64, _>(3) != 0),
                    )
                })
                .collect::<HashMap<_, _>>();
        let now = epoch_seconds();
        let mut transaction = self.database.begin().await?;
        sqlx::query("DELETE FROM cache_layers")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM cache_databases")
            .execute(&mut *transaction)
            .await?;
        let mut layer_count = 0;
        for (database, layers) in &scanned {
            let tiles = layers.iter().map(|layer| layer.tile_count).sum::<i64>();
            let old = old_databases.get(&database.database);
            sqlx::query("INSERT INTO cache_databases(database_id,display_name,path,bytes,tile_count,layer_count,last_access_at,updated_at,revocable) VALUES(?,?,?,?,?,?,?,?,?)")
                .bind(&database.database)
                .bind(old.map(|value| value.0.clone()).unwrap_or_default())
                .bind(&database.path)
                .bind(database.bytes as i64)
                .bind(tiles)
                .bind(layers.len() as i64)
                .bind(database.modified_at.map(|value| value as i64))
                .bind(now)
                .bind(i64::from(old.is_none_or(|value| value.1)))
                .execute(&mut *transaction).await?;
            for layer in layers {
                let old = old_layers.get(&(database.database.clone(), layer.item.clone()));
                sqlx::query("INSERT INTO cache_layers(database_id,item_id,display_name,format,min_zoom,max_zoom,min_x,max_x,min_y,max_y,tile_count,bytes,updated_at,revocable) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                    .bind(&database.database).bind(&layer.item)
                    .bind(old.map(|value| value.0.clone()).unwrap_or_default())
                    .bind(&layer.format).bind(layer.min_zoom).bind(layer.max_zoom)
                    .bind(layer.min_x).bind(layer.max_x).bind(layer.min_y).bind(layer.max_y)
                    .bind(layer.tile_count).bind(layer.total_bytes).bind(now)
                    .bind(i64::from(old.is_none_or(|value| value.1)))
                    .execute(&mut *transaction).await?;
                layer_count += 1;
            }
        }
        sqlx::query("INSERT INTO catalog_state(key,value) VALUES('initialized',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
            .bind(now.to_string()).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok((scanned.len(), layer_count))
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub async fn record_put(
        &self,
        database: &str,
        item: &str,
        database_name: Option<&str>,
        item_name: Option<&str>,
        path: &str,
        format: &str,
        z: i64,
        x: i64,
        y: i64,
        bytes: usize,
        outcome: PutOutcome,
    ) -> Result<()> {
        let (reply, response) = oneshot::channel();
        self.put_sender
            .send(CatalogCommand::Put(CatalogPut {
                database: database.to_owned(),
                item: item.to_owned(),
                database_name: database_name.map(str::to_owned),
                item_name: item_name.map(str::to_owned),
                path: path.to_owned(),
                format: format.to_owned(),
                z,
                x,
                y,
                bytes,
                outcome,
                reply: Some(reply),
            }))
            .await
            .map_err(|_| anyhow::anyhow!("catalog writer stopped"))?;
        response
            .await
            .map_err(|_| anyhow::anyhow!("catalog writer stopped"))?
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_put(
        &self,
        database: &str,
        item: &str,
        database_name: Option<&str>,
        item_name: Option<&str>,
        path: &str,
        format: &str,
        z: i64,
        x: i64,
        y: i64,
        bytes: usize,
        outcome: PutOutcome,
    ) -> Result<()> {
        self.put_sender
            .send(CatalogCommand::Put(CatalogPut {
                database: database.to_owned(),
                item: item.to_owned(),
                database_name: database_name.map(str::to_owned),
                item_name: item_name.map(str::to_owned),
                path: path.to_owned(),
                format: format.to_owned(),
                z,
                x,
                y,
                bytes,
                outcome,
                reply: None,
            }))
            .await
            .map_err(|_| anyhow::anyhow!("catalog writer stopped"))
    }

    pub async fn flush(&self) -> Result<()> {
        let (reply, response) = oneshot::channel();
        self.put_sender
            .send(CatalogCommand::Flush(reply))
            .await
            .map_err(|_| anyhow::anyhow!("catalog writer stopped"))?;
        response
            .await
            .map_err(|_| anyhow::anyhow!("catalog writer stopped"))?
    }

    async fn flush_pending(&self) -> Result<()> {
        self.flush().await
    }

    pub async fn database_page(
        &self,
        page: u64,
        page_size: u64,
        query: &str,
    ) -> Result<Page<DatabaseInfo>, ApiError> {
        let pattern = search_pattern(query);
        let total = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM cache_databases WHERE ?='' OR display_name LIKE ? ESCAPE '\\' OR database_id LIKE ? ESCAPE '\\'")
            .bind(query).bind(&pattern).bind(&pattern).fetch_one(&self.database).await?.max(0) as u64;
        let rows = sqlx::query("SELECT database_id,display_name,path,bytes,last_access_at,tile_count,layer_count,revocable FROM cache_databases WHERE ?='' OR display_name LIKE ? ESCAPE '\\' OR database_id LIKE ? ESCAPE '\\' ORDER BY COALESCE(NULLIF(display_name,''),database_id) LIMIT ? OFFSET ?")
            .bind(query).bind(&pattern).bind(&pattern).bind(page_size as i64)
            .bind(((page - 1) * page_size) as i64).fetch_all(&self.database).await?;
        Ok(Page {
            items: rows.into_iter().map(database_from_row).collect(),
            page,
            page_size,
            total,
        })
    }

    pub async fn layer_page(
        &self,
        database: &str,
        page: u64,
        page_size: u64,
        query: &str,
        revocable: Option<bool>,
    ) -> Result<Page<TilesetInfo>, ApiError> {
        let pattern = search_pattern(query);
        let revocable = revocable.map(i64::from);
        let total = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM cache_layers WHERE database_id=? AND (?='' OR display_name LIKE ? ESCAPE '\\' OR item_id LIKE ? ESCAPE '\\') AND (? IS NULL OR revocable=?)")
            .bind(database).bind(query).bind(&pattern).bind(&pattern)
            .bind(revocable).bind(revocable)
            .fetch_one(&self.database).await?.max(0) as u64;
        let rows = sqlx::query("SELECT item_id,display_name,tile_count,bytes,format,min_zoom,max_zoom,min_x,max_x,min_y,max_y,revocable FROM cache_layers WHERE database_id=? AND (?='' OR display_name LIKE ? ESCAPE '\\' OR item_id LIKE ? ESCAPE '\\') AND (? IS NULL OR revocable=?) ORDER BY COALESCE(NULLIF(display_name,''),item_id) LIMIT ? OFFSET ?")
            .bind(database).bind(query).bind(&pattern).bind(&pattern).bind(revocable).bind(revocable).bind(page_size as i64)
            .bind(((page - 1) * page_size) as i64).fetch_all(&self.database).await?;
        Ok(Page {
            items: rows.into_iter().map(layer_from_row).collect(),
            page,
            page_size,
            total,
        })
    }

    pub async fn summary(&self) -> Result<(usize, u64), ApiError> {
        let row = sqlx::query("SELECT COUNT(*),COALESCE(SUM(bytes),0) FROM cache_databases")
            .fetch_one(&self.database)
            .await?;
        Ok((
            row.get::<i64, _>(0).max(0) as usize,
            row.get::<i64, _>(1).max(0) as u64,
        ))
    }

    pub async fn database(&self, database: &str) -> Result<Option<DatabaseInfo>, ApiError> {
        let row = sqlx::query("SELECT database_id,display_name,path,bytes,last_access_at,tile_count,layer_count,revocable FROM cache_databases WHERE database_id=?")
            .bind(database).fetch_optional(&self.database).await?;
        Ok(row.map(database_from_row))
    }

    pub async fn layer(&self, database: &str, item: &str) -> Result<Option<TilesetInfo>, ApiError> {
        let row = sqlx::query("SELECT item_id,display_name,tile_count,bytes,format,min_zoom,max_zoom,min_x,max_x,min_y,max_y,revocable FROM cache_layers WHERE database_id=? AND item_id=?")
            .bind(database).bind(item).fetch_optional(&self.database).await?;
        Ok(row.map(layer_from_row))
    }

    pub async fn all_layers(&self, database: &str) -> Result<Vec<TilesetInfo>, ApiError> {
        let rows = sqlx::query("SELECT item_id,display_name,tile_count,bytes,format,min_zoom,max_zoom,min_x,max_x,min_y,max_y,revocable FROM cache_layers WHERE database_id=? ORDER BY item_id")
            .bind(database).fetch_all(&self.database).await?;
        Ok(rows.into_iter().map(layer_from_row).collect())
    }

    pub async fn touch_database(&self, database: &str) -> Result<(), ApiError> {
        let now = Instant::now();
        let mut touches = self.access_touches.lock().await;
        if touches
            .get(database)
            .is_some_and(|last| now.duration_since(*last) < Duration::from_secs(60))
        {
            return Ok(());
        }
        touches.insert(database.to_owned(), now);
        drop(touches);
        sqlx::query("UPDATE cache_databases SET last_access_at=? WHERE database_id=?")
            .bind(epoch_seconds())
            .bind(database)
            .execute(&self.database)
            .await?;
        Ok(())
    }

    pub async fn rename_database(&self, database: &str, name: &str) -> Result<bool, ApiError> {
        self.flush_pending().await.map_err(ApiError::Internal)?;
        Ok(sqlx::query(
            "UPDATE cache_databases SET display_name=?,updated_at=? WHERE database_id=?",
        )
        .bind(name)
        .bind(epoch_seconds())
        .bind(database)
        .execute(&self.database)
        .await?
        .rows_affected()
            > 0)
    }

    pub async fn rename_layer(
        &self,
        database: &str,
        item: &str,
        name: &str,
    ) -> Result<bool, ApiError> {
        self.flush_pending().await.map_err(ApiError::Internal)?;
        Ok(sqlx::query(
            "UPDATE cache_layers SET display_name=?,updated_at=? WHERE database_id=? AND item_id=?",
        )
        .bind(name)
        .bind(epoch_seconds())
        .bind(database)
        .bind(item)
        .execute(&self.database)
        .await?
        .rows_affected()
            > 0)
    }

    pub async fn set_database_revocable(
        &self,
        database: &str,
        revocable: bool,
    ) -> Result<bool, ApiError> {
        self.flush_pending().await.map_err(ApiError::Internal)?;
        Ok(
            sqlx::query("UPDATE cache_databases SET revocable=?,updated_at=? WHERE database_id=?")
                .bind(i64::from(revocable))
                .bind(epoch_seconds())
                .bind(database)
                .execute(&self.database)
                .await?
                .rows_affected()
                > 0,
        )
    }

    pub async fn set_layer_revocable(
        &self,
        database: &str,
        item: &str,
        revocable: bool,
    ) -> Result<bool, ApiError> {
        self.flush_pending().await.map_err(ApiError::Internal)?;
        Ok(sqlx::query(
            "UPDATE cache_layers SET revocable=?,updated_at=? WHERE database_id=? AND item_id=?",
        )
        .bind(i64::from(revocable))
        .bind(epoch_seconds())
        .bind(database)
        .bind(item)
        .execute(&self.database)
        .await?
        .rows_affected()
            > 0)
    }

    pub async fn remove_layer(&self, database: &str, item: &str) -> Result<(), ApiError> {
        self.flush_pending().await.map_err(ApiError::Internal)?;
        let _guard = self.write_lock.lock().await;
        let mut transaction = self.database.begin().await?;
        let row = sqlx::query(
            "SELECT tile_count,bytes FROM cache_layers WHERE database_id=? AND item_id=?",
        )
        .bind(database)
        .bind(item)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = row {
            sqlx::query("DELETE FROM cache_layers WHERE database_id=? AND item_id=?")
                .bind(database)
                .bind(item)
                .execute(&mut *transaction)
                .await?;
            sqlx::query("UPDATE cache_databases SET tile_count=MAX(0,tile_count-?),bytes=MAX(0,bytes-?),layer_count=MAX(0,layer_count-1),updated_at=? WHERE database_id=?")
                .bind(row.get::<i64, _>(0)).bind(row.get::<i64, _>(1)).bind(epoch_seconds()).bind(database)
                .execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn remove_database(&self, database: &str) -> Result<(), ApiError> {
        self.flush_pending().await.map_err(ApiError::Internal)?;
        let _guard = self.write_lock.lock().await;
        let mut transaction = self.database.begin().await?;
        sqlx::query("DELETE FROM cache_layers WHERE database_id=?")
            .bind(database)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM cache_databases WHERE database_id=?")
            .bind(database)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }
}

async fn catalog_writer_loop(
    database: SqlitePool,
    write_lock: Arc<Mutex<()>>,
    mut receiver: mpsc::Receiver<CatalogCommand>,
    store: TileStore,
) {
    const FLUSH_INTERVAL: Duration = Duration::from_secs(60);
    const MAX_BATCH: usize = 65_536;
    let mut pending = Vec::new();
    let mut interval = tokio::time::interval(FLUSH_INTERVAL);
    interval.tick().await;
    loop {
        tokio::select! {
            command = receiver.recv() => match command {
                Some(CatalogCommand::Put(command)) => {
                    let requires_flush = command.reply.is_some();
                    pending.push(command);
                    if requires_flush || pending.len() >= MAX_BATCH {
                        let _ = flush_catalog_batch(&database, &write_lock, &store, &mut pending).await;
                    }
                }
                Some(CatalogCommand::Flush(reply)) => {
                    let result = flush_catalog_batch(&database, &write_lock, &store, &mut pending).await;
                    let _ = reply.send(result);
                }
                None => {
                    flush_catalog_batch(&database, &write_lock, &store, &mut pending).await.ok();
                    break;
                }
            },
            _ = interval.tick() => {
                flush_catalog_batch(&database, &write_lock, &store, &mut pending).await.ok();
            }
        }
    }
}

async fn flush_catalog_batch(
    database: &SqlitePool,
    write_lock: &Mutex<()>,
    store: &TileStore,
    pending: &mut Vec<CatalogPut>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let batch = std::mem::take(pending);
    let mut recalculated = HashMap::new();
    let dirty_databases = batch
        .iter()
        .filter(|command| command.outcome.requires_recalculation)
        .map(|command| command.database.as_str())
        .collect::<std::collections::HashSet<_>>();
    for database_id in dirty_databases {
        for layer in store
            .tilesets(database_id)
            .await
            .map_err(|error| anyhow::anyhow!("tileset recalculation failed: {error:?}"))?
        {
            recalculated.insert(
                (database_id.to_owned(), layer.item),
                (layer.tile_count, layer.total_bytes),
            );
        }
    }
    let mut attempts = 0;
    let result = loop {
        attempts += 1;
        let guard = write_lock.lock().await;
        let result = write_catalog_batch(database, &batch, &recalculated).await;
        drop(guard);
        if result.is_ok()
            || attempts >= 4
            || !result
                .as_ref()
                .is_err_and(|error| error.to_string().contains("database is locked"))
        {
            break result;
        }
        tokio::time::sleep(Duration::from_millis(2 * attempts)).await;
    };
    let message = result.as_ref().err().map(ToString::to_string);
    for command in batch {
        if let Some(reply) = command.reply {
            let reply_result = message
                .as_ref()
                .map_or_else(|| Ok(()), |value| Err(anyhow::anyhow!(value.clone())));
            let _ = reply.send(reply_result);
        }
    }
    if let Some(message) = message {
        tracing::error!(error = %message, "catalog PUT batch failed");
        return Err(anyhow::anyhow!(message));
    }
    Ok(())
}

async fn write_catalog_batch(
    database: &SqlitePool,
    batch: &[CatalogPut],
    recalculated: &HashMap<(String, String), (i64, i64)>,
) -> Result<()> {
    struct DatabaseDelta<'a> {
        name: Option<&'a str>,
        path: &'a str,
        bytes: i64,
        tiles: i64,
        layers: i64,
        updated_at: i64,
    }

    let mut groups: HashMap<(&str, &str), Vec<&CatalogPut>> = HashMap::new();
    for command in batch {
        groups
            .entry((&command.database, &command.item))
            .or_default()
            .push(command);
    }
    let mut transaction = database.begin().await?;
    let mut databases: HashMap<&str, DatabaseDelta<'_>> = HashMap::new();
    for ((database_id, item_id), commands) in groups {
        let first = commands[0];
        let now = epoch_seconds();
        let existing = sqlx::query("SELECT min_zoom,max_zoom,min_x,max_x,min_y,max_y,tile_count,bytes FROM cache_layers WHERE database_id=? AND item_id=?")
            .bind(database_id).bind(item_id)
            .fetch_optional(&mut *transaction).await?;
        let new_layer = existing.is_none();
        let (
            mut min_zoom,
            mut max_zoom,
            mut min_x,
            mut max_x,
            mut min_y,
            mut max_y,
            mut tile_count,
            mut total_bytes,
        ) = if let Some(row) = existing {
            (
                row.get(0),
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
                row.get(5),
                row.get::<i64, _>(6),
                row.get::<i64, _>(7),
            )
        } else {
            (None, None, None, None, None, None, 0, 0)
        };
        let old_tile_count = tile_count;
        let old_total_bytes = total_bytes;
        let mut item_name = None;
        let mut database_name = None;
        let mut format = first.format.as_str();
        for command in commands {
            item_name = item_name.or(command.item_name.as_deref());
            database_name = database_name.or(command.database_name.as_deref());
            format = &command.format;
            if max_zoom.is_none_or(|value| command.z > value) {
                min_x = Some(command.x);
                max_x = Some(command.x);
                min_y = Some(command.y);
                max_y = Some(command.y);
            } else if max_zoom == Some(command.z) {
                min_x = merge_min(min_x, Some(command.x));
                max_x = merge_max(max_x, Some(command.x));
                min_y = merge_min(min_y, Some(command.y));
                max_y = merge_max(max_y, Some(command.y));
            }
            min_zoom = merge_min(min_zoom, Some(command.z));
            max_zoom = merge_max(max_zoom, Some(command.z));
            if command.outcome.created {
                tile_count += 1;
                total_bytes += command.bytes as i64;
            }
        }
        if let Some((count, bytes)) =
            recalculated.get(&(database_id.to_owned(), item_id.to_owned()))
        {
            tile_count = *count;
            total_bytes = *bytes;
        }
        let database_tiles = tile_count - old_tile_count;
        let database_bytes = total_bytes - old_total_bytes;
        sqlx::query("INSERT INTO cache_layers(database_id,item_id,display_name,format,min_zoom,max_zoom,min_x,max_x,min_y,max_y,tile_count,bytes,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(database_id,item_id) DO UPDATE SET display_name=CASE WHEN cache_layers.display_name='' THEN excluded.display_name ELSE cache_layers.display_name END,format=excluded.format,min_zoom=excluded.min_zoom,max_zoom=excluded.max_zoom,min_x=excluded.min_x,max_x=excluded.max_x,min_y=excluded.min_y,max_y=excluded.max_y,tile_count=excluded.tile_count,bytes=excluded.bytes,updated_at=excluded.updated_at")
            .bind(database_id).bind(item_id).bind(item_name.unwrap_or_default()).bind(format)
            .bind(min_zoom).bind(max_zoom).bind(min_x).bind(max_x).bind(min_y).bind(max_y)
            .bind(tile_count).bind(total_bytes).bind(now)
            .execute(&mut *transaction).await?;
        let delta = databases.entry(database_id).or_insert(DatabaseDelta {
            name: database_name,
            path: &first.path,
            bytes: 0,
            tiles: 0,
            layers: 0,
            updated_at: now,
        });
        delta.name = delta.name.or(database_name);
        delta.path = &first.path;
        delta.bytes += database_bytes;
        delta.tiles += database_tiles;
        delta.layers += i64::from(new_layer);
        delta.updated_at = now;
    }
    for (database_id, delta) in databases {
        sqlx::query("INSERT INTO cache_databases(database_id,display_name,path,bytes,tile_count,layer_count,last_access_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(database_id) DO UPDATE SET display_name=CASE WHEN cache_databases.display_name='' THEN excluded.display_name ELSE cache_databases.display_name END,path=excluded.path,bytes=MAX(0,cache_databases.bytes+?),tile_count=cache_databases.tile_count+?,layer_count=cache_databases.layer_count+?,last_access_at=excluded.last_access_at,updated_at=excluded.updated_at")
            .bind(database_id).bind(delta.name.unwrap_or_default()).bind(delta.path)
            .bind(delta.bytes.max(0)).bind(delta.tiles).bind(delta.layers)
            .bind(delta.updated_at).bind(delta.updated_at).bind(delta.bytes)
            .bind(delta.tiles).bind(delta.layers)
            .execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    Ok(())
}

fn merge_min(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn merge_max(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

fn database_from_row(row: sqlx::sqlite::SqliteRow) -> DatabaseInfo {
    DatabaseInfo {
        database: row.get(0),
        name: row.get(1),
        path: row.get(2),
        bytes: row.get::<i64, _>(3).max(0) as u64,
        modified_at: row
            .get::<Option<i64>, _>(4)
            .map(|value| value.max(0) as u64),
        tile_count: row.get(5),
        layer_count: row.get(6),
        revocable: row.get::<i64, _>(7) != 0,
    }
}

fn layer_from_row(row: sqlx::sqlite::SqliteRow) -> TilesetInfo {
    TilesetInfo {
        item: row.get(0),
        name: row.get(1),
        tile_count: row.get(2),
        total_bytes: row.get(3),
        format: row.get(4),
        min_zoom: row.get(5),
        max_zoom: row.get(6),
        min_x: row.get(7),
        max_x: row.get(8),
        min_y: row.get(9),
        max_y: row.get(10),
        revocable: row.get::<i64, _>(11) != 0,
    }
}

async fn ensure_column(
    database: &SqlitePool,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<()> {
    let rows = sqlx::query(&format!("PRAGMA table_info({table})"))
        .fetch_all(database)
        .await?;
    if !rows.iter().any(|row| row.get::<String, _>(1) == column) {
        sqlx::query(&format!(
            "ALTER TABLE {table} ADD COLUMN {column} {definition}"
        ))
        .execute(database)
        .await?;
    }
    Ok(())
}

fn search_pattern(query: &str) -> String {
    let escaped = query
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    format!("%{escaped}%")
}

fn epoch_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::TileKey;

    #[tokio::test]
    async fn incrementally_tracks_overwrites_and_preserves_names_on_rebuild() {
        let temp = tempfile::tempdir().unwrap();
        let store = TileStore::open(&temp.path().join("tiles"), 1, 8)
            .await
            .unwrap();
        let catalog = Catalog::open(&temp.path().join("config"), store.clone())
            .await
            .unwrap();
        let database = "database";
        let item = "image";
        let path = store.database_path(database).unwrap();
        let first = b"\x89PNG\r\n\x1a\nfirst".to_vec();
        let outcome = store
            .put(
                TileKey {
                    database: database.into(),
                    item: item.into(),
                    z: 10,
                    x: 805,
                    y: 418,
                    tile_type: None,
                },
                first.clone(),
            )
            .await
            .unwrap();
        catalog
            .record_put(
                database,
                item,
                Some("初始数据库"),
                Some("初始影像"),
                &path,
                "png",
                10,
                805,
                418,
                first.len(),
                outcome,
            )
            .await
            .unwrap();

        let second = b"\x89PNG\r\n\x1a\nnew".to_vec();
        let outcome = store
            .put(
                TileKey {
                    database: database.into(),
                    item: item.into(),
                    z: 10,
                    x: 805,
                    y: 418,
                    tile_type: None,
                },
                second.clone(),
            )
            .await
            .unwrap();
        catalog
            .record_put(
                database,
                item,
                Some("不应覆盖数据库"),
                Some("不应覆盖影像"),
                &path,
                "png",
                10,
                805,
                418,
                second.len(),
                outcome,
            )
            .await
            .unwrap();
        assert_eq!(
            catalog.database(database).await.unwrap().unwrap().name,
            "初始数据库"
        );
        assert_eq!(
            catalog.layer(database, item).await.unwrap().unwrap().name,
            "初始影像"
        );
        catalog
            .rename_database(database, "测试数据库")
            .await
            .unwrap();
        catalog
            .rename_layer(database, item, "测试影像")
            .await
            .unwrap();
        catalog
            .set_database_revocable(database, false)
            .await
            .unwrap();
        catalog
            .set_layer_revocable(database, item, false)
            .await
            .unwrap();

        let databases = catalog.database_page(1, 50, "").await.unwrap();
        let layers = catalog
            .layer_page(database, 1, 100, "", None)
            .await
            .unwrap();
        assert_eq!(databases.items[0].tile_count, 1);
        assert_eq!(databases.items[0].layer_count, 1);
        assert_eq!(layers.items[0].tile_count, 1);
        assert_eq!(layers.items[0].total_bytes, second.len() as i64);
        assert_eq!(layers.items[0].name, "测试影像");
        assert!(!databases.items[0].revocable);
        assert!(!layers.items[0].revocable);
        let database_page = catalog.database_page(1, 50, "测试").await.unwrap();
        assert_eq!(database_page.total, 1);
        assert_eq!(database_page.items[0].database, database);
        let layer_page = catalog
            .layer_page(database, 1, 100, "影像", None)
            .await
            .unwrap();
        assert_eq!(layer_page.total, 1);
        assert_eq!(layer_page.items[0].item, item);
        assert_eq!(
            catalog
                .layer_page(database, 1, 100, "不存在", None)
                .await
                .unwrap()
                .total,
            0
        );

        catalog.rebuild(&store).await.unwrap();
        assert_eq!(
            catalog.database_page(1, 50, "").await.unwrap().items[0].name,
            "测试数据库"
        );
        assert_eq!(
            catalog
                .layer_page(database, 1, 100, "", None)
                .await
                .unwrap()
                .items[0]
                .name,
            "测试影像"
        );
        assert!(!catalog.database(database).await.unwrap().unwrap().revocable);
        assert!(
            !catalog
                .layer(database, item)
                .await
                .unwrap()
                .unwrap()
                .revocable
        );
    }
}
