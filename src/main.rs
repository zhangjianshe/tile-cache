mod api;
mod config;
mod error;
mod model;
mod stats;
mod store;
mod system;

use anyhow::Result;
use api::AppState;
use clap::Parser;
use config::Config;
use stats::AccessStats;
use std::{sync::Arc, time::Duration};
use store::TileStore;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let config = Config::parse();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.worker_threads.max(1))
        .enable_all()
        .build()?
        .block_on(run(config))
}

async fn run(config: Config) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("tile_cache=info,tower_http=info")),
        )
        .init();
    let store = TileStore::open(
        &config.root_dir,
        config.read_connections,
        config.write_queue,
    )
    .await?;
    let stats = AccessStats::open(&config.config_dir).await?;

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

    if config.retention_days > 0 {
        let cleanup_store = store.clone();
        let retention_days = config.retention_days;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(24 * 60 * 60));
            interval.tick().await;
            loop {
                interval.tick().await;
                let cutoff = std::time::SystemTime::now()
                    .checked_sub(Duration::from_secs(retention_days * 86_400))
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                match cleanup_store.cleanup(cutoff).await {
                    Ok(deleted) => {
                        tracing::info!(deleted, retention_days, "expired tile cleanup completed")
                    }
                    Err(error) => tracing::error!(?error, "expired tile cleanup failed"),
                }
            }
        });
    }

    let state = AppState {
        store,
        auth_token: Arc::from(config.auth_token),
        stats: stats.clone(),
    };
    let app = api::router(state, config.max_tile_bytes);
    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    tracing::info!(address = %config.addr, version = env!("CARGO_PKG_VERSION"), "tile-cache listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await?;
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
