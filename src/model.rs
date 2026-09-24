use crate::stats::HourStats;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub page: u64,
    pub page_size: u64,
    pub total: u64,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse<'a> {
    pub status: &'a str,
    pub version: &'a str,
    pub git: &'a str,
    pub built: &'a str,
}

#[derive(Debug, Serialize)]
pub struct DatabaseInfo {
    pub database: String,
    pub name: String,
    pub path: String,
    pub bytes: u64,
    pub modified_at: Option<u64>,
    pub tile_count: i64,
    pub layer_count: i64,
    pub revocable: bool,
}

#[derive(Debug, Serialize)]
pub struct TilesetInfo {
    pub item: String,
    pub name: String,
    pub tile_count: i64,
    pub total_bytes: i64,
    pub format: String,
    pub min_zoom: Option<i64>,
    pub max_zoom: Option<i64>,
    pub min_x: Option<i64>,
    pub max_x: Option<i64>,
    pub min_y: Option<i64>,
    pub max_y: Option<i64>,
    pub revocable: bool,
}

#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    pub deleted: u64,
}

#[derive(Debug, Serialize)]
pub struct CleanupResponse {
    pub deleted_databases: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CleanupSettings {
    pub retention_days: u64,
    pub cleanup_hour: u8,
    pub shard_idle_seconds: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct CleanupRun {
    pub completed_at: Option<u64>,
    pub retention_days: u64,
    pub deleted_databases: u64,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CleanupSettingsResponse {
    pub settings: CleanupSettings,
    pub last_run: Option<CleanupRun>,
    pub next_run_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CleanupHistoryEntry {
    pub id: i64,
    pub completed_at: u64,
    pub operation: String,
    pub database_id: String,
    pub database_name: String,
    pub item_id: Option<String>,
    pub item_name: Option<String>,
    pub layer_count: u64,
    pub tile_count: u64,
    pub bytes: u64,
    pub actor: String,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NewCleanupHistory {
    pub completed_at: u64,
    pub operation: String,
    pub database_id: String,
    pub database_name: String,
    pub item_id: Option<String>,
    pub item_name: Option<String>,
    pub layer_count: u64,
    pub tile_count: u64,
    pub bytes: u64,
    pub actor: String,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DashboardData {
    pub database_count: usize,
    pub disk_bytes: u64,
    pub memory_bytes: u64,
    pub thread_count: u64,
    pub hourly: Vec<HourStats>,
}
