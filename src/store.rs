use crate::{
    error::ApiError,
    model::{DatabaseInfo, TilesetInfo},
};
use anyhow::{Context, Result};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Connection, Row, SqliteConnection, SqlitePool,
};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};

const ACCESS_FILE: &str = ".tile-cache-access";
const ACCESS_TOUCH_INTERVAL: Duration = Duration::from_secs(60 * 60);

#[derive(Clone)]
pub struct TileStore {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    root: PathBuf,
    shards: RwLock<HashMap<PathBuf, Arc<Shard>>>,
    access_touches: Mutex<HashMap<String, SystemTime>>,
    read_connections: u32,
    queue_capacity: usize,
}

struct Shard {
    reads: SqlitePool,
    writes: mpsc::Sender<WriteCommand>,
    last_used: std::sync::Mutex<Instant>,
}

#[derive(Clone, Debug)]
pub struct TileKey {
    pub database: String,
    pub item: String,
    pub z: i64,
    pub x: i64,
    pub y: i64,
    pub tile_type: Option<i64>,
}

struct TileAddress {
    database_dir: PathBuf,
    item_dir: PathBuf,
    shard_file: PathBuf,
    table: String,
    id: i64,
}

#[derive(Clone, Copy, Debug)]
pub struct PutOutcome {
    pub created: bool,
    pub requires_recalculation: bool,
}

enum WriteCommand {
    Put {
        table: String,
        id: i64,
        x: i64,
        y: i64,
        data: Vec<u8>,
        reply: oneshot::Sender<Result<PutOutcome>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

impl TileStore {
    pub async fn open(root: &Path, read_connections: u32, queue_capacity: usize) -> Result<Self> {
        tokio::fs::create_dir_all(root)
            .await
            .with_context(|| format!("create tile database root {}", root.display()))?;
        Ok(Self {
            inner: Arc::new(StoreInner {
                root: root.to_path_buf(),
                shards: RwLock::new(HashMap::new()),
                access_touches: Mutex::new(HashMap::new()),
                read_connections: read_connections.max(1),
                queue_capacity: queue_capacity.max(1),
            }),
        })
    }

    pub fn database_path(&self, database: &str) -> Result<String, ApiError> {
        Ok(database_directory(&self.inner.root, database)?
            .to_string_lossy()
            .into_owned())
    }

    pub async fn get(&self, key: TileKey) -> Result<Option<Vec<u8>>, ApiError> {
        let address = tile_address(&self.inner.root, &key)?;
        let Some(shard) = self.shard(&address.shard_file, false).await? else {
            return Ok(None);
        };
        shard.touch();
        if !table_exists(&shard.reads, &address.table).await? {
            return Ok(None);
        }
        let table = quote_identifier(&address.table)?;
        let data =
            sqlx::query_scalar::<_, Vec<u8>>(&format!("SELECT Data FROM {table} WHERE ID=?"))
                .bind(address.id)
                .fetch_optional(&shard.reads)
                .await?;
        let data = data.filter(|value| !value.is_empty());
        if data.is_some() {
            self.touch_database(&key.database, &address.database_dir)
                .await;
        }
        Ok(data)
    }

    pub async fn put(&self, key: TileKey, data: Vec<u8>) -> Result<PutOutcome, ApiError> {
        if data.is_empty() {
            return Err(ApiError::Invalid(
                "empty tile data is not allowed".to_owned(),
            ));
        }
        let address = tile_address(&self.inner.root, &key)?;
        tokio::fs::create_dir_all(&address.item_dir).await?;
        let shard = self
            .shard(&address.shard_file, true)
            .await?
            .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("tile shard was not created")))?;
        shard.touch();
        let (reply, response) = oneshot::channel();
        shard
            .writes
            .try_send(WriteCommand::Put {
                table: address.table,
                id: address.id,
                x: key.x,
                y: key.y,
                data,
                reply,
            })
            .map_err(|_| ApiError::Overloaded)?;
        let outcome = response
            .await
            .map_err(|_| writer_stopped())?
            .map_err(ApiError::Internal)?;
        self.touch_database(&key.database, &address.database_dir)
            .await;
        Ok(outcome)
    }

    pub async fn put_batch(
        &self,
        writes: Vec<(TileKey, Vec<u8>)>,
    ) -> Result<Vec<(TileKey, usize, PutOutcome)>, ApiError> {
        let mut tasks = tokio::task::JoinSet::new();
        for (key, data) in writes {
            let store = self.clone();
            tasks.spawn(async move {
                let bytes = data.len();
                let outcome = store.put(key.clone(), data).await?;
                Result::<_, ApiError>::Ok((key, bytes, outcome))
            });
        }
        let mut results = Vec::new();
        while let Some(result) = tasks.join_next().await {
            results.push(result.map_err(|error| ApiError::Internal(error.into()))??);
        }
        Ok(results)
    }

    pub async fn list_databases(&self) -> Result<Vec<DatabaseInfo>, ApiError> {
        let mut result = Vec::new();
        let mut first = tokio::fs::read_dir(&self.inner.root).await?;
        while let Some(a) = first.next_entry().await? {
            if !a.file_type().await?.is_dir() {
                continue;
            }
            add_database_info(&mut result, &a.path(), component(&a.path())).await?;
            let mut second = tokio::fs::read_dir(a.path()).await?;
            while let Some(b) = second.next_entry().await? {
                if !b.file_type().await?.is_dir() {
                    continue;
                }
                add_database_info(
                    &mut result,
                    &b.path(),
                    format!("{}{}", component(&a.path()), component(&b.path())),
                )
                .await?;
            }
        }
        result.sort_by(|left, right| left.database.cmp(&right.database));
        Ok(result)
    }

    pub async fn tilesets(&self, database_id: &str) -> Result<Vec<TilesetInfo>, ApiError> {
        let database_dir = database_directory(&self.inner.root, database_id)?;
        if tokio::fs::metadata(&database_dir).await.is_err() {
            return Ok(Vec::new());
        }
        let mut result = Vec::new();
        let mut prefixes = tokio::fs::read_dir(&database_dir).await?;
        while let Some(prefix) = prefixes.next_entry().await? {
            if !prefix.file_type().await?.is_dir() {
                continue;
            }
            let prefix_name = component(&prefix.path());
            let mut items = tokio::fs::read_dir(prefix.path()).await?;
            while let Some(item) = items.next_entry().await? {
                if !item.file_type().await?.is_dir() {
                    continue;
                }
                let item_id = format!("{}{}", prefix_name, component(&item.path()));
                if validate_id("item", &item_id).is_err() {
                    continue;
                }
                let shard_files = list_shard_files(&item.path()).await?;
                let mut tile_count = 0;
                let mut total_bytes = 0;
                let mut format = None;
                let mut min_zoom = None;
                let mut max_zoom = None;
                let mut min_x = None;
                let mut max_x = None;
                let mut min_y = None;
                let mut max_y = None;
                for shard_file in shard_files {
                    let Some(shard) = self.shard(&shard_file, false).await? else {
                        continue;
                    };
                    let tables = sqlx::query_scalar::<_, String>(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name GLOB '[A-Z]_*_*'",
                    )
                    .fetch_all(&shard.reads)
                    .await?;
                    for raw_table in tables {
                        let Some(zoom) = table_zoom(&raw_table) else {
                            continue;
                        };
                        let table = quote_identifier(&raw_table)?;
                        let row = sqlx::query(&format!(
                            "SELECT COUNT(*),COALESCE(SUM(F),0),MIN(X),MAX(X),MIN(Y),MAX(Y) FROM {table}"
                        ))
                        .fetch_one(&shard.reads)
                        .await?;
                        let count = row.get::<i64, _>(0);
                        tile_count += count;
                        total_bytes += row.get::<i64, _>(1);
                        if count == 0 {
                            continue;
                        }
                        min_zoom = Some(min_zoom.map_or(zoom, |value: i64| value.min(zoom)));
                        if max_zoom.is_none_or(|value| zoom > value) {
                            max_zoom = Some(zoom);
                            min_x = row.get::<Option<i64>, _>(2);
                            max_x = row.get::<Option<i64>, _>(3);
                            min_y = row.get::<Option<i64>, _>(4);
                            max_y = row.get::<Option<i64>, _>(5);
                        } else if max_zoom == Some(zoom) {
                            min_x = merge_min(min_x, row.get::<Option<i64>, _>(2));
                            max_x = merge_max(max_x, row.get::<Option<i64>, _>(3));
                            min_y = merge_min(min_y, row.get::<Option<i64>, _>(4));
                            max_y = merge_max(max_y, row.get::<Option<i64>, _>(5));
                        }
                        if format.is_none() {
                            let sample = sqlx::query_scalar::<_, Vec<u8>>(&format!(
                                "SELECT Data FROM {table} WHERE length(Data)>0 LIMIT 1"
                            ))
                            .fetch_optional(&shard.reads)
                            .await?;
                            format = sample.as_deref().map(tile_format);
                        }
                    }
                }
                result.push(TilesetInfo {
                    item: item_id,
                    name: String::new(),
                    tile_count,
                    total_bytes,
                    format: format.unwrap_or("pbf").to_owned(),
                    min_zoom,
                    max_zoom,
                    min_x,
                    max_x,
                    min_y,
                    max_y,
                    revocable: true,
                });
            }
        }
        result.sort_by(|left, right| left.item.cmp(&right.item));
        Ok(result)
    }

    pub async fn drop_tileset(&self, database_id: &str, item: &str) -> Result<u64, ApiError> {
        validate_id("database", database_id)?;
        validate_id("item", item)?;
        let item_dir = item_directory(&database_directory(&self.inner.root, database_id)?, item)?;
        if tokio::fs::metadata(&item_dir).await.is_err() {
            return Ok(0);
        }
        self.close_shards_under(&item_dir).await;
        tokio::fs::remove_dir_all(&item_dir).await?;
        Ok(1)
    }

    pub async fn drop_database(&self, database_id: &str) -> Result<u64, ApiError> {
        let database_dir = database_directory(&self.inner.root, database_id)?;
        if tokio::fs::metadata(&database_dir).await.is_err() {
            return Ok(0);
        }
        self.close_shards_under(&database_dir).await;
        tokio::fs::remove_dir_all(&database_dir).await?;
        remove_empty_parents(database_dir.parent(), &self.inner.root).await;
        self.inner.access_touches.lock().await.remove(database_id);
        Ok(1)
    }

    pub async fn cleanup_candidates(
        &self,
        cutoff: SystemTime,
    ) -> Result<Vec<DatabaseInfo>, ApiError> {
        let cutoff_epoch = epoch_seconds(cutoff).unwrap_or(0);
        Ok(self
            .list_databases()
            .await?
            .into_iter()
            .filter(|database| {
                database
                    .modified_at
                    .is_some_and(|modified| modified < cutoff_epoch)
            })
            .collect())
    }

    pub async fn close_idle_shards(&self, idle_timeout: Duration) -> usize {
        let removed = {
            let mut shards = self.inner.shards.write().await;
            let paths = shards
                .iter()
                .filter(|(_, shard)| {
                    Arc::strong_count(shard) == 1 && shard.idle_for() >= idle_timeout
                })
                .map(|(path, _)| path.clone())
                .collect::<Vec<_>>();
            paths
                .into_iter()
                .filter_map(|path| shards.remove(&path))
                .collect::<Vec<_>>()
        };
        let count = removed.len();
        for shard in removed {
            close_shard(shard).await;
        }
        count
    }

    async fn shard(&self, path: &Path, create: bool) -> Result<Option<Arc<Shard>>, ApiError> {
        if let Some(shard) = self.inner.shards.read().await.get(path).cloned() {
            return Ok(Some(shard));
        }
        // Serialize first-open for a path. Without the second check under the write lock,
        // concurrent first PUTs can create multiple SQLite writers for the same shard.
        let mut shards = self.inner.shards.write().await;
        if let Some(shard) = shards.get(path).cloned() {
            return Ok(Some(shard));
        }
        if !create && tokio::fs::metadata(path).await.is_err() {
            return Ok(None);
        }
        if create {
            tokio::fs::create_dir_all(path.parent().expect("shard has parent")).await?;
        }
        let mut attempts = 0;
        let shard = loop {
            attempts += 1;
            match open_shard(
                path,
                create,
                self.inner.read_connections,
                self.inner.queue_capacity,
            )
            .await
            {
                Ok(shard) => break Arc::new(shard),
                Err(error)
                    if attempts < 4 && format!("{error:?}").contains("database is locked") =>
                {
                    tokio::time::sleep(Duration::from_millis(2 * attempts)).await;
                }
                Err(error) => return Err(error),
            }
        };
        shards.insert(path.to_path_buf(), shard.clone());
        Ok(Some(shard))
    }

    async fn close_shards_under(&self, directory: &Path) {
        let removed = {
            let mut shards = self.inner.shards.write().await;
            let paths: Vec<_> = shards
                .keys()
                .filter(|path| path.starts_with(directory))
                .cloned()
                .collect();
            paths
                .into_iter()
                .filter_map(|path| shards.remove(&path))
                .collect::<Vec<_>>()
        };
        for shard in removed {
            close_shard(shard).await;
        }
    }

    async fn touch_database(&self, id: &str, directory: &Path) {
        let now = SystemTime::now();
        let mut touches = self.inner.access_touches.lock().await;
        if touches
            .get(id)
            .and_then(|last| now.duration_since(*last).ok())
            .is_some_and(|age| age < ACCESS_TOUCH_INTERVAL)
        {
            return;
        }
        touches.insert(id.to_owned(), now);
        drop(touches);
        let marker = directory.join(ACCESS_FILE);
        let _ = tokio::fs::create_dir_all(directory).await;
        let _ = tokio::fs::write(marker, []).await;
    }
}

async fn open_shard(
    path: &Path,
    create: bool,
    read_connections: u32,
    queue_capacity: usize,
) -> Result<Shard, ApiError> {
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
        .create_if_missing(create)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(5));
    let writer = SqliteConnection::connect_with(&options).await?;
    let reads = SqlitePoolOptions::new()
        .max_connections(read_connections)
        .connect_with(options)
        .await?;
    let (writes, receiver) = mpsc::channel(queue_capacity);
    tokio::spawn(writer_loop(writer, receiver));
    Ok(Shard {
        reads,
        writes,
        last_used: std::sync::Mutex::new(Instant::now()),
    })
}

impl Shard {
    fn touch(&self) {
        *self
            .last_used
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_used
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .elapsed()
    }
}

async fn close_shard(shard: Arc<Shard>) {
    let (reply, response) = oneshot::channel();
    let _ = shard.writes.send(WriteCommand::Shutdown { reply }).await;
    let _ = response.await;
    shard.reads.close().await;
}

async fn writer_loop(mut connection: SqliteConnection, mut receiver: mpsc::Receiver<WriteCommand>) {
    const MAX_BATCH: usize = 256;
    let mut initialized_tables = HashSet::new();
    while let Some(first) = receiver.recv().await {
        if let WriteCommand::Shutdown { reply } = first {
            let _ = reply.send(());
            break;
        }
        let mut batch = vec![first];
        let mut shutdown_reply = None;
        while batch.len() < MAX_BATCH {
            match receiver.try_recv() {
                Ok(command @ WriteCommand::Put { .. }) => batch.push(command),
                Ok(WriteCommand::Shutdown { reply }) => {
                    shutdown_reply = Some(reply);
                    break;
                }
                Err(_) => break,
            }
        }

        let mut attempts = 0;
        let result = loop {
            attempts += 1;
            let result = write_tile_batch(&mut connection, &batch, &initialized_tables).await;
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
        let (outcomes, batch_tables) = match result {
            Ok(result) => result,
            Err(error) => {
                fail_write_batch(batch, error.to_string());
                if let Some(reply) = shutdown_reply {
                    let _ = reply.send(());
                    break;
                }
                continue;
            }
        };
        initialized_tables.extend(batch_tables);
        if outcomes.len() != batch.len() {
            fail_write_batch(batch, "incomplete tile write batch".to_owned());
            if let Some(reply) = shutdown_reply {
                let _ = reply.send(());
                break;
            }
            continue;
        }
        for (command, outcome) in batch.into_iter().zip(outcomes) {
            if let WriteCommand::Put { reply, .. } = command {
                let _ = reply.send(Ok(outcome));
            }
        }
        if let Some(reply) = shutdown_reply {
            let _ = reply.send(());
            break;
        }
    }
    let _ = connection.close().await;
}

async fn write_tile_batch(
    connection: &mut SqliteConnection,
    batch: &[WriteCommand],
    initialized_tables: &HashSet<String>,
) -> Result<(Vec<PutOutcome>, Vec<String>)> {
    let mut transaction = connection.begin().await?;
    let result = async {
        let mut outcomes = Vec::with_capacity(batch.len());
        let mut batch_tables = Vec::new();
        for command in batch {
            let WriteCommand::Put {
                table,
                id,
                x,
                y,
                data,
                ..
            } = command
            else {
                continue;
            };
            let quoted = quote_identifier(table).map_err(|error| anyhow::anyhow!("{error:?}"))?;
            if !initialized_tables.contains(table) && !batch_tables.contains(table) {
                sqlx::query(&format!("CREATE TABLE IF NOT EXISTS {quoted}(ID INTEGER NOT NULL PRIMARY KEY,Data BLOB,X INTEGER,Y INTEGER,F INTEGER)"))
                    .execute(&mut *transaction).await?;
                sqlx::query(&format!("CREATE UNIQUE INDEX IF NOT EXISTS IDX_{} ON {quoted}(ID ASC)", quoted.trim_matches('"')))
                    .execute(&mut *transaction).await?;
                batch_tables.push(table.clone());
            }
            let inserted = sqlx::query(&format!("INSERT OR IGNORE INTO {quoted}(ID,Data,X,Y,F) VALUES(?,?,?,?,?)"))
                .bind(id).bind(data).bind(x).bind(y).bind(data.len() as i64)
                .execute(&mut *transaction).await?.rows_affected() > 0;
            if !inserted {
                sqlx::query(&format!("UPDATE {quoted} SET Data=?,X=?,Y=?,F=? WHERE ID=?"))
                    .bind(data).bind(x).bind(y).bind(data.len() as i64).bind(id)
                    .execute(&mut *transaction).await?;
            }
            outcomes.push(PutOutcome {
                created: inserted,
                requires_recalculation: !inserted,
            });
        }
        Result::<_>::Ok((outcomes, batch_tables))
    }
    .await;
    match result {
        Ok(result) => {
            transaction.commit().await?;
            Ok(result)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

fn fail_write_batch(batch: Vec<WriteCommand>, message: String) {
    for command in batch {
        if let WriteCommand::Put { reply, .. } = command {
            let _ = reply.send(Err(anyhow::anyhow!(message.clone())));
        }
    }
}

fn tile_address(root: &Path, key: &TileKey) -> Result<TileAddress, ApiError> {
    validate_key(key)?;
    let database_dir = database_directory(root, &key.database)?;
    let item_dir = item_directory(&database_dir, &key.item)?;
    let virtual_zoom = key.z.max(9);
    let directory_letter = zoom_letter(virtual_zoom)?;
    let zoom_letter = zoom_letter(key.z)?;
    let shard_file = item_dir.join(directory_letter.to_string()).join(format!(
        "{directory_letter}_{}_{}.s",
        key.x / 256,
        key.y / 256
    ));
    let table = format!("{zoom_letter}_{}_{}", key.x / 64, key.y / 64);
    let id = key.x % 64 + 64 * (key.y % 64);
    Ok(TileAddress {
        database_dir,
        item_dir,
        shard_file,
        table,
        id,
    })
}

fn zoom_letter(zoom: i64) -> Result<char, ApiError> {
    if !(0..=25).contains(&zoom) {
        return Err(ApiError::Invalid(
            "TileTools supports zoom levels 0 through 25".to_owned(),
        ));
    }
    Ok((b'A' + zoom as u8) as char)
}

fn table_zoom(table: &str) -> Option<i64> {
    let first = table.as_bytes().first().copied()?;
    (first.is_ascii_uppercase()).then_some((first - b'A') as i64)
}

fn merge_min(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

fn merge_max(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

fn tile_format(data: &[u8]) -> &'static str {
    if data.starts_with(b"\x89PNG\r\n\x1a\n") {
        "png"
    } else if data.starts_with(b"\xff\xd8\xff") {
        "jpg"
    } else if data.len() >= 12 && &data[..4] == b"RIFF" && &data[8..12] == b"WEBP" {
        "webp"
    } else {
        "pbf"
    }
}

async fn table_exists(pool: &SqlitePool, table: &str) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
    )
    .bind(table)
    .fetch_one(pool)
    .await?
        > 0)
}

async fn list_shard_files(item_dir: &Path) -> Result<Vec<PathBuf>, ApiError> {
    let mut result = Vec::new();
    let mut levels = tokio::fs::read_dir(item_dir).await?;
    while let Some(level) = levels.next_entry().await? {
        if !level.file_type().await?.is_dir() {
            continue;
        }
        let mut files = tokio::fs::read_dir(level.path()).await?;
        while let Some(file) = files.next_entry().await? {
            if file.file_type().await?.is_file()
                && file
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "s")
            {
                result.push(file.path());
            }
        }
    }
    Ok(result)
}

async fn add_database_info(
    result: &mut Vec<DatabaseInfo>,
    directory: &Path,
    database: String,
) -> Result<(), ApiError> {
    let marker = directory.join(ACCESS_FILE);
    let Ok(metadata) = tokio::fs::metadata(&marker).await else {
        return Ok(());
    };
    let bytes = directory_size(directory).await?;
    result.push(DatabaseInfo {
        database,
        name: String::new(),
        path: directory.to_string_lossy().into_owned(),
        bytes,
        modified_at: metadata.modified().ok().and_then(epoch_seconds),
        tile_count: 0,
        layer_count: 0,
        revocable: true,
    });
    Ok(())
}

async fn directory_size(directory: &Path) -> Result<u64, ApiError> {
    let directory = directory.to_path_buf();
    tokio::task::spawn_blocking(move || {
        fn size(path: &Path) -> std::io::Result<u64> {
            let mut total = 0;
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let metadata = entry.metadata()?;
                total += if metadata.is_dir() {
                    size(&entry.path())?
                } else {
                    metadata.len()
                };
            }
            Ok(total)
        }
        size(&directory)
    })
    .await
    .map_err(|error| ApiError::Internal(error.into()))?
    .map_err(ApiError::from)
}

fn database_directory(root: &Path, id: &str) -> Result<PathBuf, ApiError> {
    validate_id("database", id)?;
    Ok(if id.len() <= 4 {
        root.join(id)
    } else {
        root.join(&id[..4]).join(&id[4..])
    })
}

fn item_directory(database_dir: &Path, id: &str) -> Result<PathBuf, ApiError> {
    validate_id("item", id)?;
    Ok(if id.len() <= 4 {
        database_dir.join(id)
    } else {
        database_dir.join(&id[..4]).join(&id[4..])
    })
}

fn quote_identifier(value: &str) -> Result<String, ApiError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(ApiError::Invalid("invalid SQLite identifier".to_owned()));
    }
    Ok(format!("\"{value}\""))
}

fn validate_key(key: &TileKey) -> Result<(), ApiError> {
    validate_id("database", &key.database)?;
    validate_id("item", &key.item)?;
    if key.x < 0 || key.y < 0 {
        return Err(ApiError::Invalid("invalid XYZ coordinate".to_owned()));
    }
    if key.tile_type.is_some() {
        return Err(ApiError::Invalid(
            "type-specific tile keys are not supported by TileTools storage".to_owned(),
        ));
    }
    zoom_letter(key.z)?;
    Ok(())
}

fn validate_id(name: &str, value: &str) -> Result<(), ApiError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(ApiError::Invalid(format!("invalid {name}")));
    }
    Ok(())
}

fn component(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

fn epoch_seconds(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|value| value.as_secs())
}

fn writer_stopped() -> ApiError {
    ApiError::Internal(anyhow::anyhow!("tile shard writer stopped"))
}

async fn remove_empty_parents(directory: Option<&Path>, root: &Path) {
    let mut current = directory.map(Path::to_path_buf);
    while let Some(path) = current {
        if path == root || !path.starts_with(root) {
            break;
        }
        if tokio::fs::remove_dir(&path).await.is_err() {
            break;
        }
        current = path.parent().map(Path::to_path_buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(database: &str, item: &str, z: i64, x: i64, y: i64) -> TileKey {
        TileKey {
            database: database.into(),
            item: item.into(),
            z,
            x,
            y,
            tile_type: None,
        }
    }

    #[tokio::test]
    async fn uses_tile_tools_shards_tables_and_ids() {
        let dir = tempfile::tempdir().unwrap();
        let store = TileStore::open(dir.path(), 2, 16).await.unwrap();
        let database = "1234567890abcdef";
        let item = "abcdef1234567890";
        store
            .put(key(database, item, 10, 805, 418), vec![1, 2, 3])
            .await
            .unwrap();
        assert_eq!(
            store.get(key(database, item, 10, 805, 418)).await.unwrap(),
            Some(vec![1, 2, 3])
        );
        let shard = dir
            .path()
            .join("1234/567890abcdef/abcd/ef1234567890/K/K_3_1.s");
        assert!(shard.is_file());
        let mut connection = SqliteConnection::connect(&format!("sqlite://{}", shard.display()))
            .await
            .unwrap();
        let row = sqlx::query("SELECT ID,X,Y,F,Data FROM K_12_6")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>(0), 2_213);
        assert_eq!(row.get::<i64, _>(1), 805);
        assert_eq!(row.get::<i64, _>(2), 418);
        assert_eq!(row.get::<i64, _>(3), 3);
        assert_eq!(row.get::<Vec<u8>, _>(4), vec![1, 2, 3]);
        connection.close().await.unwrap();
        let databases = store.list_databases().await.unwrap();
        assert_eq!(databases[0].database, database);
        let tilesets = store.tilesets(database).await.unwrap();
        assert_eq!(tilesets[0].item, item);
        assert_eq!(tilesets[0].tile_count, 1);
        assert_eq!(tilesets[0].format, "pbf");
        assert_eq!(tilesets[0].min_zoom, Some(10));
        assert_eq!(tilesets[0].max_zoom, Some(10));
        assert_eq!(tilesets[0].min_x, Some(805));
        assert_eq!(tilesets[0].max_x, Some(805));
        assert_eq!(tilesets[0].min_y, Some(418));
        assert_eq!(tilesets[0].max_y, Some(418));
    }

    #[test]
    fn detects_preview_tile_formats() {
        assert_eq!(tile_format(b"\x89PNG\r\n\x1a\nrest"), "png");
        assert_eq!(tile_format(b"\xff\xd8\xffrest"), "jpg");
        assert_eq!(tile_format(b"RIFF1234WEBPrest"), "webp");
        assert_eq!(tile_format(b"\x1a\x03mvt"), "pbf");
    }

    #[tokio::test]
    async fn closes_idle_shard_connections() {
        let dir = tempfile::tempdir().unwrap();
        let store = TileStore::open(dir.path(), 2, 16).await.unwrap();
        store
            .put(key("database", "item", 10, 805, 418), vec![1])
            .await
            .unwrap();
        assert_eq!(store.inner.shards.read().await.len(), 1);

        assert_eq!(store.close_idle_shards(Duration::ZERO).await, 1);
        assert!(store.inner.shards.read().await.is_empty());
        assert_eq!(
            store
                .get(key("database", "item", 10, 805, 418))
                .await
                .unwrap(),
            Some(vec![1])
        );
    }

    #[tokio::test]
    async fn separates_zoom_and_coordinate_regions() {
        let dir = tempfile::tempdir().unwrap();
        let store = TileStore::open(dir.path(), 2, 16).await.unwrap();
        for (z, x, y) in [(8, 10, 11), (9, 10, 11), (10, 300, 11)] {
            store
                .put(key("database", "image", z, x, y), vec![z as u8])
                .await
                .unwrap();
        }
        let base = dir.path().join("data/base/imag/e");
        assert!(base.join("J/J_0_0.s").is_file());
        assert!(base.join("K/K_1_0.s").is_file());
    }
}
