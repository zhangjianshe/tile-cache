use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

#[derive(Debug)]
pub enum ApiError {
    NotFound,
    Unauthorized,
    Invalid(String),
    Overloaded,
    Internal(anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "tile not found".to_owned()),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".to_owned()),
            Self::Invalid(message) => (StatusCode::BAD_REQUEST, message),
            Self::Overloaded => (
                StatusCode::SERVICE_UNAVAILABLE,
                "write queue is full".to_owned(),
            ),
            Self::Internal(error) => {
                tracing::error!(error = ?error, "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_owned(),
                )
            }
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(value: sqlx::Error) -> Self {
        Self::Internal(value.into())
    }
}

impl From<std::io::Error> for ApiError {
    fn from(value: std::io::Error) -> Self {
        Self::Internal(value.into())
    }
}
