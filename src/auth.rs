use crate::mail::Mailer;
use axum::{
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use dashmap::DashMap;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use uuid::Uuid;

#[derive(Clone)]
pub struct AuthConfig {
    pub supabase_url: String,
    pub service_key: String,
    pub app_url: String,
    pub allowed_origins: Vec<String>,
    pub http: reqwest::Client,
    pub trust_proxy: bool,
}

#[derive(Clone)]
pub struct AuthState {
    pub config: AuthConfig,
    pub mailer: Arc<Mailer>,
    /// Límite por IP para todas las rutas /auth.
    pub limiter: RateLimiter,
    /// Límite por destinatario: evita usar el servidor para inundar el buzón de un tercero.
    pub email_limiter: RateLimiter,
}

const LIMIT_WINDOW: Duration = Duration::from_secs(60);
const MAX_KEYS: usize = 10_000;

#[derive(Clone, Default)]
pub struct RateLimiter {
    hits: Arc<DashMap<String, Vec<Instant>>>,
}

impl RateLimiter {
    pub fn allow(&self, key: &str) -> bool {
        self.allow_n(key, 8, LIMIT_WINDOW)
    }

    pub fn allow_n(&self, key: &str, max: usize, window: Duration) -> bool {
        // Tope duro de memoria: si alguien rota miles de IPs, se rechaza en vez de crecer.
        if self.hits.len() >= MAX_KEYS && !self.hits.contains_key(key) {
            return false;
        }
        let now = Instant::now();
        let mut hits = self.hits.entry(key.to_string()).or_default();
        hits.retain(|at| now.duration_since(*at) < window);
        if hits.len() >= max {
            return false;
        }
        hits.push(now);
        true
    }

    /// Borra las claves sin actividad reciente.
    pub fn purge(&self) {
        let now = Instant::now();
        self.hits.retain(|_, hits| {
            hits.retain(|at| now.duration_since(*at) < Duration::from_secs(3600));
            !hits.is_empty()
        });
    }
}

/// IP real del cliente. Solo usa X-Forwarded-For si TRUST_PROXY está activo,
/// porque de lo contrario cualquiera podría falsificarla y saltarse el límite.
pub fn client_ip(config: &AuthConfig, headers: &HeaderMap, peer: &SocketAddr) -> String {
    if config.trust_proxy {
        if let Some(ip) = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .and_then(|v| v.trim().parse::<IpAddr>().ok())
        {
            return ip.to_string();
        }
    }
    peer.ip().to_string()
}

pub async fn limit_auth(
    State(auth): State<AuthState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let ip = client_ip(&auth.config, request.headers(), &addr);
    if !auth.limiter.allow(&ip) {
        return err(StatusCode::TOO_MANY_REQUESTS, "Demasiados intentos. Probá de nuevo en un minuto.");
    }
    next.run(request).await
}

#[derive(Deserialize)]
pub struct EmailBody {
    pub email: String,
    pub redirect_to: Option<String>,
}

pub async fn magic_link(State(auth): State<AuthState>, Json(body): Json<EmailBody>) -> impl IntoResponse {
    send_link(
        auth,
        "magiclink",
        body,
        "Tu enlace para entrar",
        "Entrá a Guitar VS con este enlace. Vence en unos minutos.",
        "Entrar",
    )
    .await
}

pub async fn schedule_deletion(State(auth): State<AuthState>, headers: HeaderMap) -> impl IntoResponse {
    let Some(user_id) = caller_id(&auth, &headers).await else {
        return err(StatusCode::UNAUTHORIZED, "Tenés que iniciar sesión.");
    };
    let due = (OffsetDateTime::now_utc() + time::Duration::days(14))
        .format(&Rfc3339)
        .unwrap_or_default();
    if set_deletion_due(&auth, &user_id, Some(&due)).await.is_err() {
        return err(StatusCode::BAD_GATEWAY, "No se pudo programar la eliminación.");
    }
    // Cerrar todas las sesiones abiertas (otros aparatos) del usuario.
    revoke_sessions(&auth, &headers).await;
    ok()
}

pub async fn cancel_deletion(State(auth): State<AuthState>, headers: HeaderMap) -> impl IntoResponse {
    let Some(user_id) = caller_id(&auth, &headers).await else {
        return err(StatusCode::UNAUTHORIZED, "Tenés que iniciar sesión.");
    };
    if set_deletion_due(&auth, &user_id, None).await.is_err() {
        return err(StatusCode::BAD_GATEWAY, "No se pudo restaurar la cuenta.");
    }
    ok()
}

async fn send_link(
    auth: AuthState,
    link_type: &str,
    body: EmailBody,
    subject: &str,
    intro: &str,
    cta: &str,
) -> Response {
    let email = body.email.trim().to_lowercase();
    if !valid_email(&email) {
        return err(StatusCode::BAD_REQUEST, "Email inválido.");
    }
    // Máximo 3 enlaces por destinatario cada 15 minutos.
    if !auth.email_limiter.allow_n(&email, 3, Duration::from_secs(15 * 60)) {
        return err(StatusCode::TOO_MANY_REQUESTS, "Ya enviamos varios enlaces a ese email. Esperá unos minutos.");
    }
    let redirect = body.redirect_to
        .as_deref()
        .and_then(|r| resolve_redirect(Some(r), &auth.config).ok())
        .unwrap_or_else(|| auth.config.app_url.clone());

    let action_link = match generate_link(&auth, link_type, &email, &redirect).await {
        Ok(link) => link,
        // Responder ok igual: no revelar si el email existe.
        Err(()) => return ok(),
    };
    let html = mail_html(intro, &action_link, cta);
    if auth.mailer.send_html(&email, subject, &html).await.is_err() {
        return err(StatusCode::BAD_GATEWAY, "No se pudo enviar el email.");
    }
    ok()
}

async fn generate_link(
    auth: &AuthState,
    link_type: &str,
    email: &str,
    redirect: &str,
) -> Result<String, ()> {
    let payload = json!({
        "type": link_type,
        "email": email,
        "options": { "redirect_to": redirect }
    });

    let url = format!("{}/auth/v1/admin/generate_link", base_url(auth));
    let response = admin_request(auth, reqwest::Method::POST, &url)
        .json(&payload)
        .send()
        .await
        .map_err(|err| tracing::warn!("generate_link: {err}"))?;
    let status = response.status();
    let body: Value = response.json().await.map_err(|err| tracing::warn!("generate_link json: {err}"))?;
    if !status.is_success() {
        tracing::warn!("generate_link status {status}");
        return Err(());
    }
    let action_link = body
        .get("action_link")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| tracing::warn!("generate_link sin action_link"))?;
    // El enlace va dentro de un email: solo aceptamos https/http del propio Supabase.
    let expected = origin_of(base_url(auth));
    if expected.is_none() || origin_of(&action_link) != expected {
        tracing::warn!("generate_link devolvió un enlace con otro origen");
        return Err(());
    }
    Ok(action_link)
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    let token = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")?
        .trim();
    if token.is_empty() || token.len() > 4096 {
        return None;
    }
    Some(token)
}

async fn caller_id(auth: &AuthState, headers: &HeaderMap) -> Option<String> {
    let token = bearer(headers)?;
    let url = format!("{}/auth/v1/user", base_url(auth));
    let response = auth
        .config
        .http
        .get(url)
        .header("apikey", &auth.config.service_key)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body: Value = response.json().await.ok()?;
    let id = body.get("id").and_then(Value::as_str)?;
    Uuid::parse_str(id).ok()?;
    Some(id.to_string())
}

async fn revoke_sessions(auth: &AuthState, headers: &HeaderMap) {
    let Some(token) = bearer(headers) else { return };
    let url = format!("{}/auth/v1/logout?scope=global", base_url(auth));
    let result = auth
        .config
        .http
        .post(url)
        .header("apikey", &auth.config.service_key)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await;
    if let Err(err) = result {
        tracing::warn!("logout global: {err}");
    }
}

async fn set_deletion_due(auth: &AuthState, id: &str, due: Option<&str>) -> Result<(), ()> {
    if Uuid::parse_str(id).is_err() {
        return Err(());
    }
    let url = format!("{}/auth/v1/admin/users/{id}", base_url(auth));
    let current = admin_request(auth, reqwest::Method::GET, &url)
        .send()
        .await
        .map_err(|err| tracing::warn!("leer usuario: {err}"))?;
    if !current.status().is_success() {
        tracing::warn!("leer usuario status {}", current.status());
        return Err(());
    }
    let body: Value = current.json().await.map_err(|err| tracing::warn!("leer usuario json: {err}"))?;
    let mut meta = body.get("app_metadata").cloned().unwrap_or_else(|| json!({}));
    let Some(object) = meta.as_object_mut() else {
        return Err(());
    };
    match due {
        Some(due) => {
            object.insert("deletion_due".into(), Value::String(due.to_string()));
        }
        None => {
            object.remove("deletion_due");
        }
    }
    let response = admin_request(auth, reqwest::Method::PUT, &url)
        .json(&json!({ "app_metadata": meta }))
        .send()
        .await
        .map_err(|err| tracing::warn!("actualizar deletion_due: {err}"))?;
    if response.status().is_success() {
        Ok(())
    } else {
        tracing::warn!("actualizar deletion_due status {}", response.status());
        Err(())
    }
}

pub async fn purge_scheduled_deletions(auth: &AuthState) {
    let now = OffsetDateTime::now_utc();
    let mut due_ids = Vec::new();
    let mut page = 1u32;
    loop {
        let url = format!("{}/auth/v1/admin/users?page={page}&per_page=200", base_url(auth));
        let response = match admin_request(auth, reqwest::Method::GET, &url).send().await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!("no se pudieron listar usuarios: {err}");
                return;
            }
        };
        if !response.status().is_success() {
            tracing::warn!("listar usuarios respondió {}", response.status());
            return;
        }
        let body: Value = match response.json().await {
            Ok(body) => body,
            Err(err) => {
                tracing::warn!("respuesta de usuarios inválida: {err}");
                return;
            }
        };
        let Some(users) = body.get("users").and_then(Value::as_array) else {
            return;
        };
        if users.is_empty() {
            break;
        }
        for user in users {
            let Some(due) = user.pointer("/app_metadata/deletion_due").and_then(Value::as_str) else {
                continue;
            };
            let Ok(when) = OffsetDateTime::parse(due, &Rfc3339) else {
                continue;
            };
            if when > now {
                continue;
            }
            let Some(id) = user.get("id").and_then(Value::as_str) else {
                continue;
            };
            if Uuid::parse_str(id).is_ok() {
                due_ids.push(id.to_string());
            }
        }
        if users.len() < 200 || page >= 500 {
            break;
        }
        page += 1;
    }

    for id in due_ids {
        delete_user_data(auth, &id).await;
    }
}

async fn delete_user_data(auth: &AuthState, id: &str) {
    if Uuid::parse_str(id).is_err() {
        return;
    }
    let songs = format!("{}/rest/v1/community_songs?user_id=eq.{id}", base_url(auth));
    match admin_request(auth, reqwest::Method::DELETE, &songs).send().await {
        Ok(response) if response.status().is_success() => {}
        Ok(response) => {
            tracing::warn!("no se borraron las canciones de {id}: {}", response.status());
            return;
        }
        Err(err) => {
            tracing::warn!("no se borraron las canciones de {id}: {err}");
            return;
        }
    }
    if delete_auth_user(auth, id).await.is_ok() {
        tracing::info!("cuenta eliminada tras 14 días sin volver a entrar");
    }
}

async fn delete_auth_user(auth: &AuthState, id: &str) -> Result<(), ()> {
    if Uuid::parse_str(id).is_err() {
        return Err(());
    }
    let url = format!("{}/auth/v1/admin/users/{id}", base_url(auth));
    match admin_request(auth, reqwest::Method::DELETE, &url).send().await {
        Ok(response) if response.status().is_success() => Ok(()),
        Ok(response) => {
            tracing::warn!("no se pudo eliminar la cuenta: {}", response.status());
            Err(())
        }
        Err(err) => {
            tracing::warn!("no se pudo eliminar la cuenta: {err}");
            Err(())
        }
    }
}

fn admin_request(auth: &AuthState, method: reqwest::Method, url: &str) -> reqwest::RequestBuilder {
    auth.config
        .http
        .request(method, url)
        .header("apikey", &auth.config.service_key)
        .header("Authorization", format!("Bearer {}", auth.config.service_key))
}

fn base_url(auth: &AuthState) -> &str {
    auth.config.supabase_url.trim_end_matches('/')
}

fn mail_html(intro: &str, link: &str, cta: &str) -> String {
    format!(
        "<p>{}</p><p><a href=\"{}\">{}</a></p>",
        escape_html(intro),
        escape_html(link),
        escape_html(cta)
    )
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

pub fn resolve_redirect(requested: Option<&str>, config: &AuthConfig) -> Result<String, ()> {
    let requested = requested.filter(|value| !value.is_empty()).unwrap_or(config.app_url.as_str());
    let origin = origin_of(requested).ok_or(())?;
    let allowed = std::iter::once(config.app_url.as_str()).chain(config.allowed_origins.iter().map(String::as_str));
    if allowed.filter_map(origin_of).any(|item| item == origin) {
        Ok(requested.to_string())
    } else {
        Err(())
    }
}

/// `scheme://host[:port]` en minúsculas, o None si la URL no es http(s) válida.
pub fn origin_of(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return None;
    }
    // Una barra invertida se interpreta distinto según el parser (navegador vs. cliente
    // de correo): se rechaza para que nadie pueda esconder otro host detrás.
    if rest.contains('\\') {
        return None;
    }
    let authority = rest.split(['/', '?', '#']).next()?;
    if authority.is_empty()
        || authority.contains('@')
        || authority.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return None;
    }
    Some(format!("{scheme}://{}", authority.to_ascii_lowercase()))
}

/// Comprueba la cabecera Origin contra la lista permitida.
pub fn origin_allowed(config: &AuthConfig, origin: &str) -> bool {
    let Some(origin) = origin_of(origin) else {
        return false;
    };
    std::iter::once(config.app_url.as_str())
        .chain(config.allowed_origins.iter().map(String::as_str))
        .filter_map(origin_of)
        .any(|item| item == origin)
}

fn valid_email(email: &str) -> bool {
    if email.len() > 254 {
        return false;
    }
    let Some((local, domain)) = email.split_once('@') else {
        return false;
    };
    !local.is_empty()
        && local.len() <= 64
        && !domain.contains('@')
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !email.chars().any(|c| c.is_whitespace() || c.is_control() || c == '<' || c == '>' || c == ',' || c == ';')
}

fn ok() -> Response {
    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

fn err(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "ok": false, "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(app_url: &str, extra: &[&str]) -> AuthConfig {
        AuthConfig {
            supabase_url: "https://example.supabase.co".into(),
            service_key: "test".into(),
            app_url: app_url.into(),
            allowed_origins: extra.iter().map(|item| (*item).to_string()).collect(),
            http: reqwest::Client::new(),
            trust_proxy: false,
        }
    }

    #[test]
    fn redirect_allows_configured_origin_and_rejects_others() {
        let config = config("http://localhost:5173", &["http://192.168.0.10:5173"]);
        assert_eq!(
            resolve_redirect(Some("http://localhost:5173/cuenta"), &config).unwrap(),
            "http://localhost:5173/cuenta"
        );
        assert!(resolve_redirect(Some("https://evil.example/phish"), &config).is_err());
        assert!(resolve_redirect(Some("javascript:alert(1)"), &config).is_err());
        assert!(resolve_redirect(Some("http://localhost:5173\\@evil.example"), &config).is_err());
        assert_eq!(
            resolve_redirect(None, &config).unwrap(),
            "http://localhost:5173"
        );
    }

    #[test]
    fn origin_check_ignores_path_and_case() {
        let config = config("https://quijadajose.github.io/guitar-app/", &[]);
        assert!(origin_allowed(&config, "https://quijadajose.github.io"));
        assert!(origin_allowed(&config, "https://QuijadaJose.github.io"));
        assert!(!origin_allowed(&config, "https://evil.github.io"));
        assert!(!origin_allowed(&config, "null"));
    }

    #[test]
    fn email_rejects_header_noise() {
        assert!(valid_email("a@b.co"));
        assert!(!valid_email("a@b"));
        assert!(!valid_email("a@b.co\nBcc: x@y.z"));
        assert!(!valid_email("a@b.co,c@d.co"));
        assert!(!valid_email("a@b@c.co"));
        assert!(!valid_email("not-an-email"));
    }

    #[test]
    fn limiter_blocks_after_max_and_purges() {
        let limiter = RateLimiter::default();
        for _ in 0..3 {
            assert!(limiter.allow_n("x", 3, Duration::from_secs(60)));
        }
        assert!(!limiter.allow_n("x", 3, Duration::from_secs(60)));
        limiter.purge();
        assert_eq!(limiter.hits.len(), 1);
    }

    #[test]
    fn forwarded_ip_only_when_trusted() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "1.2.3.4, 10.0.0.1".parse().unwrap());
        let peer: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let mut cfg = config("http://localhost:5173", &[]);
        assert_eq!(client_ip(&cfg, &headers, &peer), "10.0.0.1");
        cfg.trust_proxy = true;
        assert_eq!(client_ip(&cfg, &headers, &peer), "1.2.3.4");
    }

    #[test]
    fn past_rfc3339_is_due_and_future_is_not() {
        let now = OffsetDateTime::now_utc();
        let past = (now - time::Duration::days(1)).format(&Rfc3339).unwrap();
        let future = (now + time::Duration::days(1)).format(&Rfc3339).unwrap();
        let past_at = OffsetDateTime::parse(&past, &Rfc3339).unwrap();
        let future_at = OffsetDateTime::parse(&future, &Rfc3339).unwrap();
        assert!(past_at <= now);
        assert!(future_at > now);
        assert!(OffsetDateTime::parse("1970", &Rfc3339).is_err());
    }
}
