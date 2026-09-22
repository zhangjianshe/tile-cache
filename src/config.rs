use clap::Parser;
use std::{net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug, Parser)]
#[command(version, about)]
pub struct Config {
    /// HTTP listen address.
    #[arg(long, env = "TILE_CACHE_ADDR", default_value = "0.0.0.0:7601")]
    pub addr: SocketAddr,

    /// Tokio asynchronous runtime worker threads.
    #[arg(long, env = "TILE_CACHE_WORKER_THREADS", default_value_t = 4)]
    pub worker_threads: usize,

    /// Root directory containing all tile database directories.
    #[arg(long, env = "TILE_CACHE_ROOT", default_value = "./tiledata")]
    pub root_dir: PathBuf,

    /// Instance-local directory containing the management database.
    #[arg(long, env = "TILE_CACHE_CONFIG_DIR", default_value = "./config")]
    pub config_dir: PathBuf,

    /// Optional bearer token. Empty means authentication is disabled.
    #[arg(long, env = "TILE_CACHE_AUTH_TOKEN", default_value = "")]
    pub auth_token: String,

    /// Maximum accepted tile body in bytes.
    #[arg(long, env = "TILE_CACHE_MAX_TILE_BYTES", default_value_t = 33_554_432)]
    pub max_tile_bytes: usize,

    /// Bounded write queue capacity; overload is rejected with HTTP 503.
    #[arg(long, env = "TILE_CACHE_WRITE_QUEUE", default_value_t = 4096)]
    pub write_queue: usize,

    /// SQLite read connection count.
    #[arg(long, env = "TILE_CACHE_READ_CONNECTIONS", default_value_t = 16)]
    pub read_connections: u32,

    /// Delete entries not accessed for this many days. Zero disables cleanup.
    #[arg(long, env = "TILE_CACHE_RETENTION_DAYS", default_value_t = 7)]
    pub retention_days: u64,
}
