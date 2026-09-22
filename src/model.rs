use crate::stats::HourStats;
use serde::Serialize;

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
    pub path: String,
    pub bytes: u64,
    pub modified_at: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct TilesetInfo {
    pub item: String,
    pub tile_count: i64,
    pub total_bytes: i64,
}

#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    pub deleted: u64,
}

#[derive(Debug, Serialize)]
pub struct CleanupResponse {
    pub deleted_databases: u64,
}

#[derive(Debug, Serialize)]
pub struct DashboardData {
    pub database_count: usize,
    pub disk_bytes: u64,
    pub memory_bytes: u64,
    pub thread_count: u64,
    pub hourly: Vec<HourStats>,
}
