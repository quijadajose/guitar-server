use crate::state::AppState;
use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::{net::SocketAddr, time::Duration};

use crate::catalog::SONGS;

/// Máximo 30 consultas por IP y minuto a la lista pública.
pub async fn public_rooms(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let ip = crate::auth::client_ip(&state.auth.config, &headers, &addr);
    if !state.rooms_limiter.allow_n(&ip, 30, Duration::from_secs(60)) {
        return (StatusCode::TOO_MANY_REQUESTS, Json(json!({ "error": "Demasiadas consultas" }))).into_response();
    }
    Json(json!({ "rooms": state.list_public_rooms().await })).into_response()
}

/// Catálogo de canciones de Versus: el cliente valida su partitura contra estos números.
pub async fn songs() -> impl IntoResponse {
    let items: Vec<_> = SONGS
        .iter()
        .map(|song| json!({ "id": song.id, "events": song.events, "points_per_hit": song.points_per_hit }))
        .collect();
    Json(json!({ "songs": items }))
}

pub async fn health_check() -> StatusCode {
    StatusCode::NO_CONTENT
}
