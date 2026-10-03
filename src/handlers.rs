use crate::state::AppState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;

pub async fn public_rooms(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({ "rooms": state.list_public_rooms().await }))
}

pub async fn health_check() -> StatusCode {
    StatusCode::NO_CONTENT
}
