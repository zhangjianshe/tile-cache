mod api;
mod catalog;
mod config;
mod error;
mod model;
mod stats;
mod store;
mod system;

use anyhow::{bail, Result};
use api::AppState;
use catalog::Catalog;
use clap::Parser;
use config::{Command, Config};
use model::{CleanupRun, CleanupSettings};
use stats::AccessStats;
use std::{
    io::{Read, Write},
    net::{IpAddr, SocketAddr, TcpStream},
    sync::Arc,
    time::{Duration, SystemTime},
};
use store::TileStore;
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let config = Config::parse();
    if matches!(&config.command, Some(Command::Healthcheck)) {
        return healthcheck(config.addr);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.worker_threads.max(1))
        .enable_all()
        .build()?;
    if let Some(Command::ResetPassword { password }) = &config.command {
        return runtime.block_on(auth::reset_password(&config.config_dir, password.clone()));
    }
    runtime.block_on(run(config))
}

fn healthcheck(address: SocketAddr) -> Result<()> {
    let target = SocketAddr::new(
        if address.ip().is_unspecified() {
            IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        } else {
            address.ip()
        },
        address.port(),
    );
    let mut stream = TcpStream::connect_timeout(&target, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
    let mut response = [0_u8; 64];
    let length = stream.read(&mut response)?;
    let status = std::str::from_utf8(&response[..length]).unwrap_or_default();
    if !status.starts_with("HTTP/1.1 200") && !status.starts_with("HTTP/1.0 200") {
        bail!("health endpoint returned an unhealthy response")
    }
    Ok(())
}

async fn run(config: Config) -> Result<()> {
    if config.auth_token.trim().is_empty() && !config.allow_unauthenticated_writes {
        bail!(
            "写接口未配置鉴权：请设置 TILE_CACHE_AUTH_TOKEN；仅隔离开发环境可显式设置 TILE_CACHE_ALLOW_UNAUTHENTICATED_WRITES=true"
        );
    }
    let log_filter = std::env::var("RUST_LOG")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "tile_cache=info,tower_http=info".to_owned());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(log_filter))
        .init();
    tracing::info!(
        "tile-cache effective configuration\n\
         ├─ listen address   : {}\n\
         ├─ tile data root   : {}\n\
         ├─ config directory : {}\n\
         ├─ authentication   : {}\n\
         ├─ secure cookies   : {}\n\
         ├─ worker threads   : {}\n\
         ├─ read connections : {} per shard\n\
         ├─ shard idle close : {} seconds\n\
         ├─ cleanup hour    : {:02}:00 Asia/Shanghai\n\
         ├─ write queue      : {}\n\
         ├─ max tile size    : {} bytes ({:.2} MiB)\n\
         ├─ memory tile LRU  : {} bytes ({:.2} MiB)\n\
         ├─ LRU max tile     : {} bytes ({:.2} MiB)\n\
         └─ retention days   : {}",
        config.addr,
        config.root_dir.display(),
        config.config_dir.display(),
        if config.auth_token.is_empty() {
            "disabled (explicit development override)"
        } else {
            "enabled"
        },
        config.secure_cookies,
        config.worker_threads.max(1),
        config.read_connections.max(1),
        config.shard_idle_seconds,
        config.cleanup_hour.min(23),
        config.write_queue.max(1),
        config.max_tile_bytes,
        config.max_tile_bytes as f64 / 1_048_576.0,
        config.memory_cache_bytes,
        config.memory_cache_bytes as f64 / 1_048_576.0,
        config.memory_cache_max_tile_bytes,
        config.memory_cache_max_tile_bytes as f64 / 1_048_576.0,
        config.retention_days,
    );
    let store = TileStore::open_with_cache(
        &config.root_dir,
        config.read_connections,
        config.write_queue,
        config.memory_cache_bytes,
        config.memory_cache_max_tile_bytes,
    )
    .await?;
    let stats = AccessStats::open(&config.config_dir).await?;
    let catalog = Catalog::open(&config.config_dir, store.clone()).await?;
    if !catalog.initialized().await? {
        tracing::info!("building initial tile catalog");
        let (databases, layers) = catalog
            .rebuild(&store)
            .await
            .map_err(|error| anyhow::anyhow!("initial tile catalog failed: {error:?}"))?;
        tracing::info!(databases, layers, "initial tile catalog built");
    }
    let auth_service = auth::AuthService::open(&config.config_dir).await?;
    if !auth_service.configured().await? {
        if config.admin_password.is_some() {
            eprintln!("尚未配置 Dashboard 管理员，使用 TILE_CACHE_ADMIN_PASSWORD 初始化。");
        } else {
            eprintln!("尚未配置 Dashboard 管理员，正在生成初始密码。");
        }
        auth::reset_password(&config.config_dir, config.admin_password.clone()).await?;
    }
    let auth = Arc::new(auth_service);
    let cleanup_settings = Arc::new(RwLock::new(
        stats
            .load_cleanup_settings(CleanupSettings {
                retention_days: config.retention_days,
                cleanup_hour: config.cleanup_hour.min(23),
                shard_idle_seconds: config.shard_idle_seconds,
            })
            .await?,
    ));
    {
        let settings = cleanup_settings.read().await;
        tracing::info!(
            retention_days = settings.retention_days,
            cleanup_hour = settings.cleanup_hour,
            shard_idle_seconds = settings.shard_idle_seconds,
            "runtime cleanup settings loaded"
        );
    }

    let periodic_stats = stats.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(error) = periodic_stats.flush().await {
                tracing::error!(?error, "hourly metrics flush failed");
            }
        }
    });

    {
        let cleanup_store = store.clone();
        let cleanup_catalog = catalog.clone();
        let cleanup_stats = stats.clone();
        let runtime_settings = cleanup_settings.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await;
            loop {
                interval.tick().await;
                let settings = runtime_settings.read().await.clone();
                if settings.shard_idle_seconds > 0 {
                    let closed = cleanup_store
                        .close_idle_shards(Duration::from_secs(settings.shard_idle_seconds))
                        .await;
                    if closed > 0 {
                        tracing::info!(closed, "idle tile shards closed");
                    }
                }
                if settings.retention_days == 0 {
                    continue;
                }
                let now = api::epoch_seconds();
                let local = now.saturating_add(8 * 3_600);
                let local_day = local / 86_400;
                let local_hour = (local % 86_400) / 3_600;
                let last_day = cleanup_stats
                    .last_cleanup()
                    .await
                    .ok()
                    .flatten()
                    .and_then(|run| run.completed_at)
                    .map(|completed| completed.saturating_add(8 * 3_600) / 86_400);
                if local_hour != settings.cleanup_hour as u64 || last_day == Some(local_day) {
                    continue;
                }
                let cutoff = SystemTime::now()
                    .checked_sub(Duration::from_secs(settings.retention_days * 86_400))
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                let run = match api::cleanup_expired(
                    &cleanup_store,
                    &cleanup_catalog,
                    &cleanup_stats,
                    cutoff,
                    "scheduler",
                )
                .await
                {
                    Ok(deleted) => {
                        tracing::info!(
                            deleted,
                            retention_days = settings.retention_days,
                            "expired tile cleanup completed"
                        );
                        CleanupRun {
                            completed_at: Some(now),
                            retention_days: settings.retention_days,
                            deleted_databases: deleted,
                            error: None,
                        }
                    }
                    Err(error) => {
                        tracing::error!(?error, "expired tile cleanup failed");
                        CleanupRun {
                            completed_at: Some(now),
                            retention_days: settings.retention_days,
                            deleted_databases: 0,
                            error: Some(format!("{error:?}")),
                        }
                    }
                };
                if let Err(error) = cleanup_stats.record_cleanup(&run).await {
                    tracing::error!(?error, "cleanup result persistence failed");
                }
            }
        });
    }

    let state = AppState {
        store,
        catalog: catalog.clone(),
        auth_token: Arc::from(config.auth_token),
        stats: stats.clone(),
        cleanup_settings,
        auth,
        secure_cookies: config.secure_cookies,
    };
    let app = api::router(state, config.max_tile_bytes);
    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    tracing::info!(address = %config.addr, version = env!("CARGO_PKG_VERSION"), "tile-cache listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await?;
    catalog.flush().await?;
    stats.flush().await?;
    Ok(())
}

async fn shutdown() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl+C handler")
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}
mod auth;
