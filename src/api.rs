use crate::{
    error::ApiError,
    model::{CleanupResponse, DashboardData, DeleteResponse, HealthResponse},
    stats::AccessStats,
    store::{TileKey, TileStore},
    system::current_process_metrics,
};
use axum::{
    body::Bytes,
    extract::{Path, Query, Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::Next,
    response::{Html, IntoResponse, Response},
    routing::{delete, get},
    Json, Router,
};
use serde::Deserialize;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tower_http::{
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};

#[derive(Clone)]
pub struct AppState {
    pub store: TileStore,
    pub auth_token: Arc<str>,
    pub stats: AccessStats,
}

#[derive(Deserialize)]
struct TileQuery {
    #[serde(rename = "type")]
    tile_type: Option<i64>,
}

#[derive(Deserialize)]
struct CleanupQuery {
    days: Option<u64>,
}

pub fn router(state: AppState, max_tile_bytes: usize) -> Router {
    let public = Router::new()
        .route("/", get(dashboard))
        .route("/api/v1/dashboard", get(dashboard_data))
        .route("/tiles/{database}/{item}/{z}/{x}/{y_ext}", get(get_tile))
        .route("/api/v1/databases", get(list_databases))
        .route("/api/v1/databases/{database}", get(database_status));

    let protected = Router::new()
        .route(
            "/tiles/{database}/{item}/{z}/{x}/{y_ext}",
            axum::routing::put(put_tile),
        )
        .route("/api/v1/databases/{database}", delete(delete_database))
        .route(
            "/api/v1/databases/{database}/tilesets/{item}",
            delete(delete_tileset),
        )
        .route("/api/v1/admin/cleanup", delete(cleanup))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            authorize,
        ));

    Router::new()
        .route("/health", get(health))
        .merge(public)
        .merge(protected)
        .layer(RequestBodyLimitLayer::new(max_tile_bytes))
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::new(
            header::HeaderName::from_static("x-request-id"),
            MakeRequestUuid,
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health() -> Json<HealthResponse<'static>> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        git: env!("TILE_CACHE_GIT_HASH"),
        built: env!("TILE_CACHE_BUILD_TIME"),
    })
}

async fn authorize(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if state.auth_token.is_empty() {
        return Ok(next.run(request).await);
    }
    let expected = format!("Bearer {}", state.auth_token);
    let bearer_valid = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected);
    if !bearer_valid {
        return Err(ApiError::Unauthorized);
    }
    Ok(next.run(request).await)
}

async fn get_tile(
    State(state): State<AppState>,
    Path((database, item, z, x, y_ext)): Path<(String, String, i64, i64, String)>,
    Query(query): Query<TileQuery>,
) -> Result<Response, ApiError> {
    let (y, format) = parse_y_extension(&y_ext)?;
    let data = state
        .store
        .get(TileKey {
            database,
            item,
            z,
            x,
            y,
            tile_type: query.tile_type,
        })
        .await?;
    let Some(data) = data else {
        state.stats.record_get(false, 0);
        return Err(ApiError::NotFound);
    };
    state.stats.record_get(true, data.len());
    let content_type = match format {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "pbf" | "mvt" => "application/vnd.mapbox-vector-tile",
        _ => "application/octet-stream",
    };
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, HeaderValue::from_static(content_type)),
            (
                header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=604800"),
            ),
        ],
        data,
    )
        .into_response())
}

async fn put_tile(
    State(state): State<AppState>,
    Path((database, item, z, x, y_ext)): Path<(String, String, i64, i64, String)>,
    Query(query): Query<TileQuery>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let (y, _) = parse_y_extension(&y_ext)?;
    let bytes = body.len();
    state
        .store
        .put(
            TileKey {
                database,
                item,
                z,
                x,
                y,
                tile_type: query.tile_type,
            },
            body.to_vec(),
        )
        .await?;
    state.stats.record_put(bytes);
    Ok(StatusCode::NO_CONTENT)
}

async fn dashboard() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

async fn dashboard_data(State(state): State<AppState>) -> Result<Json<DashboardData>, ApiError> {
    let databases = state.store.list_databases().await?;
    let process = current_process_metrics();
    Ok(Json(DashboardData {
        database_count: databases.len(),
        disk_bytes: databases.iter().map(|database| database.bytes).sum(),
        memory_bytes: process.memory_bytes,
        thread_count: process.thread_count,
        hourly: state.stats.recent(24),
    }))
}

async fn list_databases(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.store.list_databases().await?))
}

async fn database_status(
    State(state): State<AppState>,
    Path(database): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.store.tilesets(&database).await?))
}

async fn delete_tileset(
    State(state): State<AppState>,
    Path((database, item)): Path<(String, String)>,
) -> Result<Json<DeleteResponse>, ApiError> {
    Ok(Json(DeleteResponse {
        deleted: state.store.drop_tileset(&database, &item).await?,
    }))
}

async fn delete_database(
    State(state): State<AppState>,
    Path(database): Path<String>,
) -> Result<Json<DeleteResponse>, ApiError> {
    Ok(Json(DeleteResponse {
        deleted: state.store.drop_database(&database).await?,
    }))
}

async fn cleanup(
    State(state): State<AppState>,
    Query(query): Query<CleanupQuery>,
) -> Result<Json<CleanupResponse>, ApiError> {
    let days = query.days.unwrap_or(7).clamp(1, 3650);
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(days * 86_400))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    Ok(Json(CleanupResponse {
        deleted_databases: state.store.cleanup(cutoff).await?,
    }))
}

fn parse_y_extension(value: &str) -> Result<(i64, &str), ApiError> {
    let (y, format) = value
        .rsplit_once('.')
        .ok_or_else(|| ApiError::Invalid("XYZ URL must end with a tile extension".to_owned()))?;
    let y = y
        .parse::<i64>()
        .map_err(|_| ApiError::Invalid("invalid XYZ y coordinate".to_owned()))?;
    if format.is_empty()
        || format.len() > 8
        || !format.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(ApiError::Invalid("invalid tile extension".to_owned()));
    }
    Ok((y, format))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    #[test]
    fn parses_xyz_file_name() {
        assert_eq!(parse_y_extension("420.png").unwrap(), (420, "png"));
        assert!(parse_y_extension("420").is_err());
        assert!(parse_y_extension("x.png").is_err());
    }

    #[tokio::test]
    async fn get_is_public_but_put_requires_token() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState {
            store: TileStore::open(&temp.path().join("tiles"), 1, 8)
                .await
                .unwrap(),
            auth_token: Arc::from("secret"),
            stats: AccessStats::open(&temp.path().join("config"))
                .await
                .unwrap(),
        };
        let app = router(state, 1024);

        let get_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/dashboard")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);

        let put_response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/tiles/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/1/1/1.png")
                    .body(Body::from("tile"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put_response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejects_empty_tile_data() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState {
            store: TileStore::open(&temp.path().join("tiles"), 1, 8)
                .await
                .unwrap(),
            auth_token: Arc::from(""),
            stats: AccessStats::open(&temp.path().join("config"))
                .await
                .unwrap(),
        };
        let response = router(state, 1024)
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/tiles/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/1/1/1.png")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
