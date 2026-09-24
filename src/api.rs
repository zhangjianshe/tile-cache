use crate::{
    auth::{cookie_token, SharedAuth},
    catalog::Catalog,
    error::ApiError,
    model::{
        CatalogRebuildStatus, CleanupResponse, CleanupRun, CleanupSettings,
        CleanupSettingsResponse, DashboardData, DeleteResponse, HealthResponse, NewCleanupHistory,
    },
    stats::AccessStats,
    store::{TileKey, TileStore},
    system::current_process_metrics,
};
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, RwLock as StdRwLock},
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tower_http::{
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};

#[derive(Clone)]
pub struct AppState {
    pub store: TileStore,
    pub catalog: Catalog,
    pub auth_token: Arc<str>,
    pub stats: AccessStats,
    pub cleanup_settings: Arc<RwLock<CleanupSettings>>,
    pub auth: SharedAuth,
    pub secure_cookies: bool,
    pub catalog_rebuild: Arc<StdRwLock<CatalogRebuildStatus>>,
}

#[derive(Deserialize)]
struct TileQuery {
    #[serde(rename = "type")]
    tile_type: Option<i64>,
    preview: Option<String>,
}

#[derive(Deserialize)]
struct CleanupQuery {
    days: Option<u64>,
}

#[derive(Deserialize)]
struct PageQuery {
    page: Option<u64>,
    page_size: Option<u64>,
    q: Option<String>,
    revocable: Option<bool>,
}

#[derive(Deserialize)]
struct LoginRequest {
    password: String,
}

#[derive(Deserialize)]
struct RenameRequest {
    name: String,
}

#[derive(Deserialize)]
struct RevocableRequest {
    revocable: bool,
}

#[derive(Deserialize)]
struct BatchPutRequest {
    database: String,
    item: String,
    format: String,
    database_name: Option<String>,
    layer_name: Option<String>,
    tiles: Vec<BatchTile>,
}

#[derive(Deserialize)]
struct BatchTile {
    z: i64,
    x: i64,
    y: i64,
    #[serde(rename = "type")]
    tile_type: Option<i64>,
    data: String,
}

#[derive(Serialize)]
struct AuthStatus {
    authenticated: bool,
    configured: bool,
}

pub fn router(state: AppState, max_tile_bytes: usize) -> Router {
    let public = Router::new()
        .route("/", get(dashboard))
        .route("/assets/openlayers/ol.js", get(openlayers_js))
        .route("/assets/openlayers/ol.css", get(openlayers_css))
        .route("/api/v1/dashboard", get(dashboard_data))
        .route("/api/v1/admin/settings", get(cleanup_settings))
        .route("/api/v1/admin/cleanup/history", get(cleanup_history))
        .route("/api/v1/admin/catalog/rebuild", get(catalog_rebuild_status))
        .route("/api/v1/auth/status", get(auth_status))
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/auth/logout", post(logout))
        .route("/tiles/{database}/{item}/{z}/{x}/{y_ext}", get(get_tile))
        .route("/api/v1/databases", get(list_databases))
        .route("/api/v1/databases/{database}", get(database_status));

    let protected = Router::new()
        .route("/api/v1/tiles/batch", post(put_tile_batch))
        .route(
            "/tiles/{database}/{item}/{z}/{x}/{y_ext}",
            axum::routing::put(put_tile),
        )
        .route(
            "/api/v1/databases/{database}",
            delete(delete_database).patch(rename_database),
        )
        .route(
            "/api/v1/databases/{database}/tilesets/{item}",
            delete(delete_tileset).patch(rename_tileset),
        )
        .route(
            "/api/v1/databases/{database}/revocable",
            put(set_database_revocable),
        )
        .route(
            "/api/v1/databases/{database}/tilesets/{item}/revocable",
            put(set_tileset_revocable),
        )
        .route("/api/v1/admin/catalog/rebuild", post(rebuild_catalog))
        .route("/api/v1/admin/cleanup", delete(cleanup))
        .route("/api/v1/admin/settings", put(update_cleanup_settings))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            authorize,
        ));

    Router::new()
        .route("/health", get(health))
        .merge(public)
        .merge(protected)
        .layer(RequestBodyLimitLayer::new(max_tile_bytes))
        .layer(DefaultBodyLimit::disable())
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
    let session_valid = if let Some(token) = cookie_token(request.headers()) {
        state.auth.valid_session(token).await.unwrap_or(false)
    } else {
        false
    };
    if !bearer_valid && !session_valid {
        return Err(ApiError::Unauthorized);
    }
    Ok(next.run(request).await)
}

async fn auth_status(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<AuthStatus>, ApiError> {
    let authenticated = if let Some(token) = cookie_token(&headers) {
        state
            .auth
            .valid_session(token)
            .await
            .map_err(ApiError::Internal)?
    } else {
        false
    };
    Ok(Json(AuthStatus {
        authenticated,
        configured: state.auth.configured().await.map_err(ApiError::Internal)?,
    }))
}

async fn login(
    State(state): State<AppState>,
    Json(body): Json<LoginRequest>,
) -> Result<Response, ApiError> {
    let token = state
        .auth
        .login(body.password)
        .await
        .map_err(ApiError::Internal)?
        .ok_or(ApiError::Unauthorized)?;
    let secure = if state.secure_cookies { "; Secure" } else { "" };
    Ok((
        [(
            header::SET_COOKIE,
            format!("tile_cache_session={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=7200{secure}"),
        )],
        Json(serde_json::json!({"authenticated":true})),
    )
        .into_response())
}

async fn logout(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    if let Some(token) = cookie_token(&headers) {
        state.auth.logout(token).await.map_err(ApiError::Internal)?;
    }
    let secure = if state.secure_cookies { "; Secure" } else { "" };
    Ok((
        [(
            header::SET_COOKIE,
            format!("tile_cache_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0{secure}"),
        )],
        Json(serde_json::json!({"authenticated":false})),
    )
        .into_response())
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
            database: database.clone(),
            item,
            z,
            x,
            y,
            tile_type: query.tile_type,
        })
        .await?;
    let Some(data) = data else {
        state.stats.record_get(false, 0);
        if query
            .preview
            .as_deref()
            .is_some_and(|value| matches!(value, "1" | "true"))
            && matches!(format, "png" | "jpg" | "jpeg" | "webp")
        {
            return Ok((
                StatusCode::OK,
                [
                    (
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("image/svg+xml; charset=utf-8"),
                    ),
                    (header::CACHE_CONTROL, HeaderValue::from_static("no-store")),
                    (
                        header::HeaderName::from_static("x-tile-cache-miss"),
                        HeaderValue::from_static("true"),
                    ),
                ],
                br##"<svg xmlns="http://www.w3.org/2000/svg" width="256" height="256" viewBox="0 0 256 256"><rect width="256" height="256" fill="#eef1f5"/><g transform="rotate(-28 128 128)" fill="#94a3b8" font-family="Arial,sans-serif" font-size="18" font-weight="600" letter-spacing="2" text-anchor="middle" opacity=".72"><text x="128" y="76">NOT CACHED</text><text x="128" y="132">NOT CACHED</text><text x="128" y="188">NOT CACHED</text></g></svg>"##
                    .as_slice(),
            )
                .into_response());
        }
        return Err(ApiError::NotFound);
    };
    if let Err(error) = state.catalog.touch_database(&database).await {
        tracing::warn!(?error, database, "tile catalog access time update failed");
    }
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
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let (y, format) = parse_y_extension(&y_ext)?;
    let database_name = display_name_header(&headers, "x-tile-database-name")?;
    let item_name = display_name_header(&headers, "x-tile-layer-name")?;
    let bytes = body.len();
    let path = state.store.database_path(&database)?;
    let outcome = state
        .store
        .put(
            TileKey {
                database: database.clone(),
                item: item.clone(),
                z,
                x,
                y,
                tile_type: query.tile_type,
            },
            body.to_vec(),
        )
        .await?;
    state
        .catalog
        .enqueue_put(
            &database,
            &item,
            database_name.as_deref(),
            item_name.as_deref(),
            &path,
            format,
            z,
            x,
            y,
            bytes,
            outcome,
        )
        .await
        .map_err(ApiError::Internal)?;
    state.stats.record_put(bytes);
    Ok(StatusCode::NO_CONTENT)
}

async fn put_tile_batch(
    State(state): State<AppState>,
    Json(request): Json<BatchPutRequest>,
) -> Result<StatusCode, ApiError> {
    if request.tiles.is_empty() || request.tiles.len() > 256 {
        return Err(ApiError::Invalid(
            "batch must contain 1 to 256 tiles".to_owned(),
        ));
    }
    let format = request.format.trim().to_ascii_lowercase();
    if format.is_empty()
        || format.len() > 16
        || !format.bytes().all(|value| value.is_ascii_alphanumeric())
    {
        return Err(ApiError::Invalid("invalid tile format".to_owned()));
    }
    let database_name = request.database_name.map(valid_display_name).transpose()?;
    let layer_name = request.layer_name.map(valid_display_name).transpose()?;
    let path = state.store.database_path(&request.database)?;
    let mut writes = Vec::with_capacity(request.tiles.len());
    for tile in request.tiles {
        let data = BASE64
            .decode(tile.data)
            .map_err(|_| ApiError::Invalid("invalid base64 tile data".to_owned()))?;
        if data.is_empty() {
            return Err(ApiError::Invalid(
                "empty tile data is not allowed".to_owned(),
            ));
        }
        writes.push((
            TileKey {
                database: request.database.clone(),
                item: request.item.clone(),
                z: tile.z,
                x: tile.x,
                y: tile.y,
                tile_type: tile.tile_type,
            },
            data,
        ));
    }
    for (key, bytes, outcome) in state.store.put_batch(writes).await? {
        state
            .catalog
            .enqueue_put(
                &request.database,
                &request.item,
                database_name.as_deref(),
                layer_name.as_deref(),
                &path,
                &format,
                key.z,
                key.x,
                key.y,
                bytes,
                outcome,
            )
            .await
            .map_err(ApiError::Internal)?;
        state.stats.record_put(bytes);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn dashboard() -> Html<String> {
    Html(
        include_str!("dashboard.html").replace("__TILE_CACHE_VERSION__", env!("CARGO_PKG_VERSION")),
    )
}

async fn openlayers_js() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        include_bytes!("vendor/openlayers/ol.js").as_slice(),
    )
}

async fn openlayers_css() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        include_bytes!("vendor/openlayers/ol.css").as_slice(),
    )
}

async fn dashboard_data(State(state): State<AppState>) -> Result<Json<DashboardData>, ApiError> {
    let (database_count, disk_bytes) = state.catalog.summary().await?;
    let process = current_process_metrics();
    Ok(Json(DashboardData {
        database_count,
        disk_bytes,
        memory_bytes: process.memory_bytes,
        thread_count: process.thread_count,
        tile_memory_cache: state.store.memory_cache_metrics(),
        hourly: state.stats.recent(24),
    }))
}

async fn list_databases(
    State(state): State<AppState>,
    Query(query): Query<PageQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let (page, page_size, search) = pagination(query, 50)?;
    Ok(Json(
        state
            .catalog
            .database_page(page, page_size, &search)
            .await?,
    ))
}

async fn database_status(
    State(state): State<AppState>,
    Path(database): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let revocable = query.revocable;
    let (page, page_size, search) = pagination(query, 100)?;
    Ok(Json(
        state
            .catalog
            .layer_page(&database, page, page_size, &search, revocable)
            .await?,
    ))
}

async fn delete_tileset(
    State(state): State<AppState>,
    Path((database, item)): Path<(String, String)>,
) -> Result<Json<DeleteResponse>, ApiError> {
    let snapshot = state.catalog.layer(&database, &item).await?;
    let database_name = state
        .catalog
        .database(&database)
        .await?
        .map(|value| value.name)
        .unwrap_or_default();
    let deleted = match state.store.drop_tileset(&database, &item).await {
        Ok(deleted) => deleted,
        Err(error) => {
            record_history(
                &state,
                NewCleanupHistory {
                    completed_at: epoch_seconds(),
                    operation: "delete_layer".into(),
                    database_id: database.clone(),
                    database_name,
                    item_id: Some(item.clone()),
                    item_name: snapshot.as_ref().map(|value| value.name.clone()),
                    layer_count: 1,
                    tile_count: snapshot
                        .as_ref()
                        .map_or(0, |value| value.tile_count.max(0) as u64),
                    bytes: snapshot
                        .as_ref()
                        .map_or(0, |value| value.total_bytes.max(0) as u64),
                    actor: "administrator".into(),
                    success: false,
                    error: Some(format!("{error:?}")),
                },
            )
            .await;
            return Err(error);
        }
    };
    if deleted > 0 {
        record_history(
            &state,
            NewCleanupHistory {
                completed_at: epoch_seconds(),
                operation: "delete_layer".into(),
                database_id: database.clone(),
                database_name,
                item_id: Some(item.clone()),
                item_name: snapshot.as_ref().map(|value| value.name.clone()),
                layer_count: 1,
                tile_count: snapshot
                    .as_ref()
                    .map_or(0, |value| value.tile_count.max(0) as u64),
                bytes: snapshot
                    .as_ref()
                    .map_or(0, |value| value.total_bytes.max(0) as u64),
                actor: "administrator".into(),
                success: true,
                error: None,
            },
        )
        .await;
        state.catalog.remove_layer(&database, &item).await?;
    }
    Ok(Json(DeleteResponse { deleted }))
}

async fn delete_database(
    State(state): State<AppState>,
    Path(database): Path<String>,
) -> Result<Json<DeleteResponse>, ApiError> {
    let snapshot = state.catalog.database(&database).await?;
    let deleted = match state.store.drop_database(&database).await {
        Ok(deleted) => deleted,
        Err(error) => {
            record_history(
                &state,
                database_history(
                    &database,
                    snapshot.as_ref(),
                    "delete_database",
                    "administrator",
                    false,
                    Some(format!("{error:?}")),
                ),
            )
            .await;
            return Err(error);
        }
    };
    if deleted > 0 {
        record_history(
            &state,
            database_history(
                &database,
                snapshot.as_ref(),
                "delete_database",
                "administrator",
                true,
                None,
            ),
        )
        .await;
        state.catalog.remove_database(&database).await?;
    }
    Ok(Json(DeleteResponse { deleted }))
}

async fn rename_database(
    State(state): State<AppState>,
    Path(database): Path<String>,
    Json(body): Json<RenameRequest>,
) -> Result<StatusCode, ApiError> {
    let name = valid_display_name(body.name)?;
    if !state.catalog.rename_database(&database, &name).await? {
        return Err(ApiError::NotFound);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn rename_tileset(
    State(state): State<AppState>,
    Path((database, item)): Path<(String, String)>,
    Json(body): Json<RenameRequest>,
) -> Result<StatusCode, ApiError> {
    let name = valid_display_name(body.name)?;
    if !state.catalog.rename_layer(&database, &item, &name).await? {
        return Err(ApiError::NotFound);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn set_database_revocable(
    State(state): State<AppState>,
    Path(database): Path<String>,
    Json(body): Json<RevocableRequest>,
) -> Result<StatusCode, ApiError> {
    if !state
        .catalog
        .set_database_revocable(&database, body.revocable)
        .await?
    {
        return Err(ApiError::NotFound);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn set_tileset_revocable(
    State(state): State<AppState>,
    Path((database, item)): Path<(String, String)>,
    Json(body): Json<RevocableRequest>,
) -> Result<StatusCode, ApiError> {
    if !state
        .catalog
        .set_layer_revocable(&database, &item, body.revocable)
        .await?
    {
        return Err(ApiError::NotFound);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn rebuild_catalog(
    State(state): State<AppState>,
) -> Result<(StatusCode, Json<CatalogRebuildStatus>), ApiError> {
    let initial = {
        let mut status = state
            .catalog_rebuild
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if status.running {
            return Ok((StatusCode::OK, Json(status.clone())));
        }
        *status = CatalogRebuildStatus {
            running: true,
            phase: "discovering".to_owned(),
            started_at: Some(epoch_seconds()),
            ..CatalogRebuildStatus::default()
        };
        status.clone()
    };
    let catalog = state.catalog.clone();
    let store = state.store.clone();
    let rebuild_status = state.catalog_rebuild.clone();
    tokio::spawn(async move {
        let progress_status = rebuild_status.clone();
        let result = catalog
            .rebuild_with_progress(
                &store,
                move |databases, total_databases, layers, total_layers| {
                    let mut status = progress_status
                        .write()
                        .unwrap_or_else(|error| error.into_inner());
                    status.phase = "scanning".to_owned();
                    status.processed_databases = databases;
                    status.total_databases = total_databases;
                    status.processed_layers = layers;
                    status.total_layers = total_layers;
                },
            )
            .await;
        let mut status = rebuild_status
            .write()
            .unwrap_or_else(|error| error.into_inner());
        status.running = false;
        status.completed_at = Some(epoch_seconds());
        match result {
            Ok((databases, layers)) => {
                status.phase = "completed".to_owned();
                status.processed_databases = databases;
                status.total_databases = databases;
                status.processed_layers = layers;
                status.total_layers = layers;
            }
            Err(error) => {
                status.phase = "failed".to_owned();
                status.error = Some(format!("{error:?}"));
            }
        }
    });
    Ok((StatusCode::ACCEPTED, Json(initial)))
}

async fn catalog_rebuild_status(State(state): State<AppState>) -> Json<CatalogRebuildStatus> {
    Json(
        state
            .catalog_rebuild
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone(),
    )
}

fn valid_display_name(name: String) -> Result<String, ApiError> {
    let name = name.trim().to_owned();
    if name.is_empty() || name.chars().count() > 128 || name.chars().any(char::is_control) {
        return Err(ApiError::Invalid(
            "name must contain 1 to 128 printable characters".to_owned(),
        ));
    }
    Ok(name)
}

fn display_name_header(headers: &HeaderMap, header_name: &str) -> Result<Option<String>, ApiError> {
    let Some(value) = headers.get(header_name) else {
        return Ok(None);
    };
    let encoded = value.to_str().map_err(|_| {
        ApiError::Invalid(format!("{header_name} must be ASCII percent-encoded UTF-8"))
    })?;
    let decoded = percent_decode(encoded)
        .map_err(|message| ApiError::Invalid(format!("invalid {header_name}: {message}")))?;
    valid_display_name(decoded).map(Some)
}

fn percent_decode(value: &str) -> Result<String, &'static str> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err("incomplete percent escape");
            }
            let high = hex_digit(bytes[index + 1]).ok_or("invalid percent escape")?;
            let low = hex_digit(bytes[index + 2]).ok_or("invalid percent escape")?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| "value is not valid UTF-8")
}

fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn pagination(query: PageQuery, default_size: u64) -> Result<(u64, u64, String), ApiError> {
    let search = query.q.unwrap_or_default().trim().to_owned();
    if search.chars().count() > 128 {
        return Err(ApiError::Invalid(
            "q must not exceed 128 characters".to_owned(),
        ));
    }
    Ok((
        query.page.unwrap_or(1).max(1),
        query.page_size.unwrap_or(default_size).clamp(1, 200),
        search,
    ))
}

async fn cleanup_history(
    State(state): State<AppState>,
    Query(query): Query<PageQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let (page, page_size, _) = pagination(query, 20)?;
    Ok(Json(
        state
            .stats
            .cleanup_history(page, page_size)
            .await
            .map_err(ApiError::Internal)?,
    ))
}

pub(crate) fn database_history(
    database: &str,
    snapshot: Option<&crate::model::DatabaseInfo>,
    operation: &str,
    actor: &str,
    success: bool,
    error: Option<String>,
) -> NewCleanupHistory {
    NewCleanupHistory {
        completed_at: epoch_seconds(),
        operation: operation.into(),
        database_id: database.into(),
        database_name: snapshot.map_or_else(String::new, |value| value.name.clone()),
        item_id: None,
        item_name: None,
        layer_count: snapshot.map_or(0, |value| value.layer_count.max(0) as u64),
        tile_count: snapshot.map_or(0, |value| value.tile_count.max(0) as u64),
        bytes: snapshot.map_or(0, |value| value.bytes),
        actor: actor.into(),
        success,
        error,
    }
}

pub(crate) async fn record_history(state: &AppState, entry: NewCleanupHistory) {
    if let Err(error) = state.stats.record_cleanup_history(&entry).await {
        tracing::error!(?error, "cleanup history persistence failed");
    }
}

async fn cleanup(
    State(state): State<AppState>,
    Query(query): Query<CleanupQuery>,
) -> Result<Json<CleanupResponse>, ApiError> {
    let configured_days = state.cleanup_settings.read().await.retention_days;
    let days = query.days.unwrap_or(configured_days).clamp(1, 3650);
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(days * 86_400))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let deleted_databases = cleanup_expired(
        &state.store,
        &state.catalog,
        &state.stats,
        cutoff,
        "administrator",
    )
    .await?;
    state
        .stats
        .record_cleanup(&CleanupRun {
            completed_at: Some(epoch_seconds()),
            retention_days: days,
            deleted_databases,
            error: None,
        })
        .await
        .map_err(ApiError::Internal)?;
    Ok(Json(CleanupResponse { deleted_databases }))
}

pub(crate) async fn cleanup_expired(
    store: &TileStore,
    catalog: &Catalog,
    stats: &AccessStats,
    cutoff: SystemTime,
    actor: &str,
) -> Result<u64, ApiError> {
    let candidates = store.cleanup_candidates(cutoff).await?;
    let mut deleted = 0;
    for candidate in candidates {
        let database = candidate.database.clone();
        let snapshot = catalog.database(&database).await?.unwrap_or(candidate);
        let layers = catalog.all_layers(&database).await?;
        if !snapshot.revocable || layers.iter().any(|layer| !layer.revocable) {
            for layer in layers.into_iter().filter(|layer| layer.revocable) {
                let count = store.drop_tileset(&database, &layer.item).await?;
                if count > 0 {
                    if let Err(error) = stats
                        .record_cleanup_history(&NewCleanupHistory {
                            completed_at: epoch_seconds(),
                            operation: "expired_layer".into(),
                            database_id: database.clone(),
                            database_name: snapshot.name.clone(),
                            item_id: Some(layer.item.clone()),
                            item_name: Some(layer.name.clone()),
                            layer_count: 1,
                            tile_count: layer.tile_count.max(0) as u64,
                            bytes: layer.total_bytes.max(0) as u64,
                            actor: actor.into(),
                            success: true,
                            error: None,
                        })
                        .await
                    {
                        tracing::error!(?error, "cleanup history persistence failed");
                    }
                    catalog.remove_layer(&database, &layer.item).await?;
                }
            }
            continue;
        }
        match store.drop_database(&database).await {
            Ok(count) => {
                if count > 0 {
                    deleted += count;
                    if let Err(error) = stats
                        .record_cleanup_history(&database_history(
                            &database,
                            Some(&snapshot),
                            "expired_database",
                            actor,
                            true,
                            None,
                        ))
                        .await
                    {
                        tracing::error!(?error, "cleanup history persistence failed");
                    }
                    catalog.remove_database(&database).await?;
                }
            }
            Err(error) => {
                let detail = format!("{error:?}");
                if let Err(history_error) = stats
                    .record_cleanup_history(&database_history(
                        &database,
                        Some(&snapshot),
                        "expired_database",
                        actor,
                        false,
                        Some(detail),
                    ))
                    .await
                {
                    tracing::error!(?history_error, "cleanup history persistence failed");
                }
                return Err(error);
            }
        }
    }
    Ok(deleted)
}

async fn cleanup_settings(
    State(state): State<AppState>,
) -> Result<Json<CleanupSettingsResponse>, ApiError> {
    let settings = state.cleanup_settings.read().await.clone();
    let last_run = state
        .stats
        .last_cleanup()
        .await
        .map_err(ApiError::Internal)?;
    Ok(Json(CleanupSettingsResponse {
        next_run_at: if settings.retention_days == 0 {
            None
        } else {
            next_cleanup_epoch(epoch_seconds(), settings.cleanup_hour)
        },
        settings,
        last_run,
    }))
}

async fn update_cleanup_settings(
    State(state): State<AppState>,
    Json(settings): Json<CleanupSettings>,
) -> Result<Json<CleanupSettingsResponse>, ApiError> {
    if settings.retention_days > 3650 {
        return Err(ApiError::Invalid(
            "retention_days must be between 0 and 3650".to_owned(),
        ));
    }
    if settings.cleanup_hour > 23 {
        return Err(ApiError::Invalid(
            "cleanup_hour must be between 0 and 23".to_owned(),
        ));
    }
    if settings.shard_idle_seconds > 86_400 {
        return Err(ApiError::Invalid(
            "shard_idle_seconds must be between 0 and 86400".to_owned(),
        ));
    }
    state
        .stats
        .save_cleanup_settings(&settings)
        .await
        .map_err(ApiError::Internal)?;
    *state.cleanup_settings.write().await = settings.clone();
    let last_run = state
        .stats
        .last_cleanup()
        .await
        .map_err(ApiError::Internal)?;
    Ok(Json(CleanupSettingsResponse {
        next_run_at: if settings.retention_days == 0 {
            None
        } else {
            next_cleanup_epoch(epoch_seconds(), settings.cleanup_hour)
        },
        settings,
        last_run,
    }))
}

pub fn epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn next_cleanup_epoch(now: u64, cleanup_hour: u8) -> Option<u64> {
    let local = now.saturating_add(8 * 3_600);
    let day_start = local / 86_400 * 86_400;
    let today = day_start + cleanup_hour as u64 * 3_600;
    Some((if local < today { today } else { today + 86_400 }).saturating_sub(8 * 3_600))
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
    async fn dashboard_shows_product_name_and_current_version() {
        let html = dashboard().await.0;
        assert!(html.contains("地图缓存服务"));
        assert!(html.contains(&format!("v{}", env!("CARGO_PKG_VERSION"))));
        assert!(!html.contains("__TILE_CACHE_VERSION__"));
        assert!(!html.contains("Storage service"));
        assert!(html.contains("id=\"rebuildCatalog\""));
    }

    #[tokio::test]
    async fn catalog_rebuild_runs_in_background_and_rejects_duplicates() {
        let temp = tempfile::tempdir().unwrap();
        let store = TileStore::open(&temp.path().join("tiles"), 1, 8)
            .await
            .unwrap();
        store
            .put(
                TileKey {
                    database: "database".to_owned(),
                    item: "layer".to_owned(),
                    z: 1,
                    x: 1,
                    y: 1,
                    tile_type: None,
                },
                vec![1, 2, 3],
            )
            .await
            .unwrap();
        let catalog = Catalog::open(&temp.path().join("config"), store.clone())
            .await
            .unwrap();
        let state = AppState {
            store,
            catalog,
            auth_token: Arc::from(""),
            stats: AccessStats::open(&temp.path().join("config"))
                .await
                .unwrap(),
            cleanup_settings: Arc::new(RwLock::new(CleanupSettings {
                retention_days: 7,
                cleanup_hour: 4,
                shard_idle_seconds: 300,
            })),
            auth: Arc::new(
                crate::auth::AuthService::open(&temp.path().join("config"))
                    .await
                    .unwrap(),
            ),
            secure_cookies: false,
            catalog_rebuild: Arc::new(StdRwLock::new(CatalogRebuildStatus::default())),
        };

        let (status, first) = rebuild_catalog(State(state.clone())).await.unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(first.running);
        let (status, second) = rebuild_catalog(State(state.clone())).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert!(second.running);

        for _ in 0..100 {
            let current = catalog_rebuild_status(State(state.clone())).await.0;
            if !current.running {
                assert_eq!(current.phase, "completed");
                assert_eq!(current.total_databases, 1);
                assert_eq!(current.total_layers, 1);
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("catalog rebuild did not complete");
    }

    #[tokio::test]
    async fn get_is_public_but_put_requires_token() {
        let temp = tempfile::tempdir().unwrap();
        let store = TileStore::open(&temp.path().join("tiles"), 1, 8)
            .await
            .unwrap();
        let state = AppState {
            store: store.clone(),
            catalog: Catalog::open(&temp.path().join("config"), store)
                .await
                .unwrap(),
            auth_token: Arc::from("secret"),
            stats: AccessStats::open(&temp.path().join("config"))
                .await
                .unwrap(),
            cleanup_settings: Arc::new(RwLock::new(CleanupSettings {
                retention_days: 7,
                cleanup_hour: 4,
                shard_idle_seconds: 300,
            })),
            auth: Arc::new(
                crate::auth::AuthService::open(&temp.path().join("config"))
                    .await
                    .unwrap(),
            ),
            secure_cookies: false,
            catalog_rebuild: Arc::new(StdRwLock::new(CatalogRebuildStatus::default())),
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

        let asset_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/assets/openlayers/ol.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(asset_response.status(), StatusCode::OK);
        assert_eq!(
            asset_response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/javascript; charset=utf-8"
        );

        let missing_uri = "/tiles/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/1/1/1.png";
        let missing_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(missing_uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_response.status(), StatusCode::NOT_FOUND);
        let preview_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("{missing_uri}?preview=1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(preview_response.status(), StatusCode::OK);
        assert_eq!(
            preview_response.headers().get("x-tile-cache-miss").unwrap(),
            "true"
        );

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
        let store = TileStore::open(&temp.path().join("tiles"), 1, 8)
            .await
            .unwrap();
        let state = AppState {
            store: store.clone(),
            catalog: Catalog::open(&temp.path().join("config"), store)
                .await
                .unwrap(),
            auth_token: Arc::from(""),
            stats: AccessStats::open(&temp.path().join("config"))
                .await
                .unwrap(),
            cleanup_settings: Arc::new(RwLock::new(CleanupSettings {
                retention_days: 7,
                cleanup_hour: 4,
                shard_idle_seconds: 300,
            })),
            auth: Arc::new(
                crate::auth::AuthService::open(&temp.path().join("config"))
                    .await
                    .unwrap(),
            ),
            secure_cookies: false,
            catalog_rebuild: Arc::new(StdRwLock::new(CatalogRebuildStatus::default())),
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

    #[tokio::test]
    async fn batch_put_writes_each_tile() {
        let temp = tempfile::tempdir().unwrap();
        let store = TileStore::open(&temp.path().join("tiles"), 1, 8)
            .await
            .unwrap();
        let state = AppState {
            store: store.clone(),
            catalog: Catalog::open(&temp.path().join("config"), store)
                .await
                .unwrap(),
            auth_token: Arc::from(""),
            stats: AccessStats::open(&temp.path().join("config"))
                .await
                .unwrap(),
            cleanup_settings: Arc::new(RwLock::new(CleanupSettings {
                retention_days: 7,
                cleanup_hour: 4,
                shard_idle_seconds: 300,
            })),
            auth: Arc::new(
                crate::auth::AuthService::open(&temp.path().join("config"))
                    .await
                    .unwrap(),
            ),
            secure_cookies: false,
            catalog_rebuild: Arc::new(StdRwLock::new(CatalogRebuildStatus::default())),
        };
        let app = router(state, 4 * 1024 * 1024);
        let database = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let item = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let large_tile = BASE64.encode(vec![7_u8; 1600 * 1024]);
        let body = format!(
            r#"{{"database":"{database}","item":"{item}","format":"png","tiles":[{{"z":1,"x":1,"y":1,"data":"{large_tile}"}},{{"z":1,"x":1,"y":2,"data":"dHdv"}}]}}"#
        );
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/tiles/batch")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        for y in [1, 2] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/tiles/{database}/{item}/1/1/{y}.png"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
    }
}
