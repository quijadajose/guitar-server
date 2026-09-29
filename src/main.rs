mod auth;
mod handlers;
mod mail;
mod models;
mod protocol;
mod state;
mod ws;

use auth::{AuthConfig, AuthState, RateLimiter};
use axum::{middleware, routing::{get, post}, Router};
use mail::Mailer;
use state::AppState;
use std::sync::Arc;
use std::{net::SocketAddr, time::Duration};
use tower_http::cors::CorsLayer;
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt::init();

    let supabase_url = std::env::var("SUPABASE_URL").expect("SUPABASE_URL");
    let service_key = std::env::var("SUPABASE_SECRET_KEY").expect("SUPABASE_SECRET_KEY");
    let resend_key = std::env::var("RESEND_KEY").expect("RESEND_KEY");
    let resend_from = std::env::var("RESEND_FROM").unwrap_or_else(|_| "onboarding@resend.dev".into());
    let app_url = std::env::var("APP_URL").unwrap_or_else(|_| "https://quijadajose.github.io/guitar-app/".into());
    let mut allowed_origins = vec![
        app_url.clone(),
        "https://quijadajose.github.io".into(),
    ];
    if let Ok(extra) = std::env::var("CORS_ORIGINS") {
        allowed_origins.extend(extra.split(',').map(|item| item.trim().to_string()).filter(|item| !item.is_empty()));
    }

    let auth = AuthState {
        config: AuthConfig {
            supabase_url,
            service_key,
            app_url,
            allowed_origins: allowed_origins.clone(),
            http: reqwest::Client::new(),
        },
        mailer: Arc::new(Mailer::new(resend_key, resend_from)),
        limiter: RateLimiter::default(),
    };

    let state = AppState::new(auth);

    // Tarea en segundo plano para purgar salas inactivas y mantener la memoria < 128 MiB
    let state_cleanup = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            state_cleanup.clean_inactive_rooms().await;
        }
    });

    let state_purge = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        loop {
            interval.tick().await;
            auth::purge_scheduled_deletions(&state_purge.auth).await;
        }
    });

    let app = router(state);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3000);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Guitar multiplayer Server running on http://{} and ws://{}/ws", addr, addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .unwrap();
}

pub fn router(state: AppState) -> Router {
    let allowed_origins = state.auth.config.allowed_origins.clone();
    let cors = cors_layer(&allowed_origins);
    let auth_router = Router::new()
        .route("/auth/magic-link", post(auth::magic_link))
        .route(
            "/auth/account/deletion",
            post(auth::schedule_deletion).delete(auth::cancel_deletion),
        )
        .layer(middleware::from_fn_with_state(state.auth.clone(), auth::limit_auth));

    Router::new()
        .route("/health", get(handlers::health_check))
        .route("/rooms", get(handlers::public_rooms))
        .route("/ws", get(ws::ws_handler))
        .merge(auth_router)
        .layer(cors)
        .with_state(state)
}

fn cors_layer(origins: &[String]) -> CorsLayer {
    use axum::http::{header, HeaderValue, Method};
    let values: Vec<HeaderValue> = origins.iter().filter_map(|origin| origin.parse().ok()).collect();
    CorsLayer::new()
        .allow_origin(tower_http::cors::AllowOrigin::list(values))
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE, header::ACCEPT])
}
