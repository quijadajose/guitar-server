use crate::state::AppState;
use axum::{extract::State, response::IntoResponse, Json};
use serde_json::json;

pub async fn public_rooms(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({ "rooms": state.list_public_rooms().await }))
}

pub async fn health_check() -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "service": "guitar-server",
        "version": "0.1.0",
        "arch": "rust-axum-tokio",
        "memory_target": "128MiB"
    }))
}
