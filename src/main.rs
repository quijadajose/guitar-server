mod auth;
mod catalog;
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
    // Para validar el token del usuario alcanza la publicable; si falta, se usa la secreta como antes.
    let publishable_key = std::env::var("SUPABASE_PUBLISHABLE_KEY").unwrap_or_else(|_| {
        tracing::warn!("SUPABASE_PUBLISHABLE_KEY no definida: se usa la secreta para validar sesiones");
        service_key.clone()
    });
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
    // Solo confiar en X-Forwarded-For si el servidor corre detrás de un proxy propio.
    let trust_proxy = std::env::var("TRUST_PROXY").is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));

    // Sin timeout, una llamada colgada a Supabase/Resend retiene la tarea para siempre.
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .expect("reqwest client");

    let auth = AuthState {
        config: AuthConfig {
            supabase_url,
            service_key,
            publishable_key,
            app_url,
            allowed_origins: allowed_origins.clone(),
            http: http.clone(),
            trust_proxy,
        },
        mailer: Arc::new(Mailer::new(resend_key, resend_from, http)),
        limiter: RateLimiter::default(),
        email_limiter: RateLimiter::default(),
    };

    let state = AppState::new(auth);
    let state_shutdown = state.clone();

    // Tarea en segundo plano para purgar salas inactivas y mantener la memoria < 128 MiB
    let state_cleanup = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            state_cleanup.clean_inactive_rooms().await;
            // Los limitadores guardan una entrada por IP/email: sin purga crecen sin límite.
            state_cleanup.auth.limiter.purge();
            state_cleanup.auth.email_limiter.purge();
            state_cleanup.rooms_limiter.purge();
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
        .with_graceful_shutdown(shutdown_signal(state_shutdown))
        .await
        .unwrap();
}

/// Ctrl+C o SIGTERM (lo que manda Docker/Deplexo al redeployar): avisa a las salas y espera un momento
/// para que el mensaje salga antes de cortar.
async fn shutdown_signal(state: AppState) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    info!("Apagando: avisando a las salas");
    state.announce_restart().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
}

pub fn router(state: AppState) -> Router {
    let allowed_origins = state.auth.config.allowed_origins.clone();
    let cors = cors_layer(&allowed_origins);
    let auth_router = Router::new()
        .route("/auth/magic-link", post(auth::magic_link))
        .route(
            "/auth/account/deletion",
            get(auth::deletion_status).post(auth::schedule_deletion).delete(auth::cancel_deletion),
        )
        .layer(middleware::from_fn_with_state(state.auth.clone(), auth::limit_auth));

    Router::new()
        .route("/health", get(handlers::health_check))
        .route("/rooms", get(handlers::public_rooms))
        .route("/songs", get(handlers::songs))
        .route("/ws", get(ws::ws_handler))
        .merge(auth_router)
        .layer(cors)
        .with_state(state)
}

fn cors_layer(origins: &[String]) -> CorsLayer {
    use axum::http::{header, HeaderValue, Method};
    let values: Vec<HeaderValue> = origins
        .iter()
        .filter_map(|origin| auth::origin_of(origin))
        .filter_map(|origin| origin.parse().ok())
        .collect();
    CorsLayer::new()
        .allow_origin(tower_http::cors::AllowOrigin::list(values))
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE, header::ACCEPT])
}
