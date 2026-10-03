use crate::auth::AuthState;
use axum::extract::FromRef;
use crate::models::{GameMode, Player, PlayerRole, PublicRoom, Room};
use crate::protocol::ServerMessage;
use dashmap::DashMap;
use rand::{distributions::Alphanumeric, Rng};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use tracing::info;

pub const MAX_ROOMS: usize = 64;
pub const MAX_SOCKETS: usize = 128;
pub const MAX_SOCKETS_PER_IP: usize = 6;
const EMPTY_ROOM_SECS: u64 = 120;
const IDLE_ROOM_SECS: u64 = 600;
const WARN_LEAD_SECS: u64 = 60;

pub type RoomHandle = Arc<RwLock<Room>>;
pub type RoomMap = Arc<DashMap<String, RoomHandle>>;

#[derive(Clone)]
pub struct AppState {
    pub rooms: RoomMap,
    pub auth: AuthState,
    sockets: Arc<AtomicUsize>,
    sockets_per_ip: Arc<DashMap<String, usize>>,
}

impl FromRef<AppState> for AuthState {
    fn from_ref(state: &AppState) -> Self {
        state.auth.clone()
    }
}

impl AppState {
    pub fn new(auth: AuthState) -> Self {
        Self {
            rooms: Arc::new(DashMap::new()),
            auth,
            sockets: Arc::new(AtomicUsize::new(0)),
            sockets_per_ip: Arc::new(DashMap::new()),
        }
    }

    /// Una sola IP no puede ocupar todo el cupo global de sockets.
    pub fn try_acquire_ip(&self, ip: &str) -> bool {
        let mut count = self.sockets_per_ip.entry(ip.to_string()).or_insert(0);
        if *count >= MAX_SOCKETS_PER_IP {
            return false;
        }
        *count += 1;
        true
    }

    pub fn release_ip(&self, ip: &str) {
        if let Some(mut count) = self.sockets_per_ip.get_mut(ip) {
            *count = count.saturating_sub(1);
        }
        self.sockets_per_ip.remove_if(ip, |_, count| *count == 0);
    }

    pub fn try_acquire_socket(&self) -> bool {
        let mut current = self.sockets.load(Ordering::Relaxed);
        loop {
            if current >= MAX_SOCKETS {
                return false;
            }
            match self.sockets.compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    pub fn release_socket(&self) {
        self.sockets.fetch_sub(1, Ordering::AcqRel);
    }

    /// Copia los handles para no retener el lock de un shard de DashMap
    /// mientras se espera un RwLock de tokio (eso puede bloquear un worker).
    fn room_handles(&self) -> Vec<(String, RoomHandle)> {
        self.rooms
            .iter()
            .map(|item| (item.key().clone(), item.value().clone()))
            .collect()
    }

    pub async fn create_room(
        &self,
        song_id: String,
        mode: GameMode,
        is_public: bool,
        host_session_id: String,
        host_name: String,
    ) -> Option<(String, RoomHandle)> {
        if self.rooms.len() >= MAX_ROOMS {
            return None;
        }
        let mut room = Room::new(String::new(), song_id, mode, is_public);
        room.host = Some(Player::new(host_session_id, host_name, PlayerRole::Host));

        // Insertar con entry() evita la carrera entre "comprobar si existe" e "insertar".
        for _ in 0..32 {
            let code: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(5)
                .map(char::from)
                .collect::<String>()
                .to_uppercase();
            if let dashmap::mapref::entry::Entry::Vacant(slot) = self.rooms.entry(code.clone()) {
                room.code = code.clone();
                let room_handle = Arc::new(RwLock::new(room));
                slot.insert(room_handle.clone());
                return Some((code, room_handle));
            }
        }
        None
    }

    pub async fn list_public_rooms(&self) -> Vec<PublicRoom> {
        let mut rooms = Vec::new();
        for (_, handle) in self.room_handles() {
            let room = handle.read().await;
            if !room.is_public() || room.is_playing || room.guest.is_some() {
                continue;
            }
            let Some(host) = room.host.as_ref() else {
                continue;
            };
            rooms.push(PublicRoom {
                code: room.code.clone(),
                song_id: room.song_id.clone(),
                mode: room.mode.clone(),
                host_name: host.name.clone(),
            });
        }
        rooms
    }

    pub fn get_room(&self, code: &str) -> Option<RoomHandle> {
        let normalized = code.trim().to_uppercase();
        if normalized.len() > 8 {
            return None;
        }
        self.rooms.get(&normalized).map(|r| r.value().clone())
    }

    pub fn remove_room(&self, code: &str) {
        self.rooms.remove(code);
    }

    pub async fn clean_inactive_rooms(&self) {
        let mut to_remove = Vec::new();
        for (code, handle) in self.room_handles() {
            let room = handle.read().await;
            let empty = room.host.is_none() && room.guest.is_none();
            let idle = room.idle_secs();
            let limit = if empty { EMPTY_ROOM_SECS } else { IDLE_ROOM_SECS };
            if idle > limit {
                let reason = if empty { "empty" } else { "idle" };
                let _ = room.tx.send(ServerMessage::RoomClosed {
                    reason: reason.to_string(),
                });
                to_remove.push(code);
            } else if idle + WARN_LEAD_SECS >= limit
                && !room.closing_notice_sent.swap(true, Ordering::Relaxed)
            {
                let reason = if empty { "empty" } else { "idle" };
                let _ = room.tx.send(ServerMessage::RoomClosing {
                    closes_in_secs: limit.saturating_sub(idle),
                    reason: reason.to_string(),
                });
            }
        }
        for code in to_remove {
            self.rooms.remove(&code);
            info!("Purged inactive room: {}", code);
        }
    }
}

pub fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::auth::{AuthConfig, AuthState, RateLimiter};
    use crate::mail::Mailer;
    use crate::models::GameMode;
    use std::sync::Arc;

    pub(crate) fn test_state() -> AppState {
        let http = reqwest::Client::new();
        let auth = AuthState {
            config: AuthConfig {
                supabase_url: "https://example.supabase.co".into(),
                service_key: "test".into(),
                app_url: "http://localhost:5173".into(),
                allowed_origins: vec!["http://localhost:5173".into()],
                http: http.clone(),
                trust_proxy: false,
            },
            mailer: Arc::new(Mailer::new("test".into(), "onboarding@resend.dev".into(), http)),
            limiter: RateLimiter::default(),
            email_limiter: RateLimiter::default(),
        };
        AppState::new(auth)
    }

    #[tokio::test]
    async fn test_create_and_get_room() {
        let state = test_state();
        let (code, _handle) = state
            .create_room(
                "song_1".to_string(),
                GameMode::Classic,
                true,
                "session_host".to_string(),
                "Host".to_string(),
            )
            .await
            .expect("room");

        assert_eq!(code.len(), 5);
        let room = state.get_room(&code);
        assert!(room.is_some());

        let r = room.unwrap();
        let r_guard = r.read().await;
        assert_eq!(r_guard.song_id, "song_1");
        assert_eq!(r_guard.code, code);
        assert!(r_guard.host.is_some());
    }

    #[tokio::test]
    async fn test_case_insensitive_room_lookup() {
        let state = test_state();
        let (code, _) = state
            .create_room(
                "song_2".to_string(),
                GameMode::SuddenDeath,
                true,
                "host_id".to_string(),
                "Alice".to_string(),
            )
            .await
            .expect("room");

        let lower = code.to_lowercase();
        assert!(state.get_room(&lower).is_some());
        assert!(state.get_room("XXXXXXXXXXXXXXXX").is_none());
    }

    #[tokio::test]
    async fn room_cap_is_enforced() {
        let state = test_state();
        for i in 0..MAX_ROOMS {
            state
                .create_room("fur_elise".into(), GameMode::Classic, false, format!("s{i}"), "x".into())
                .await
                .expect("room");
        }
        assert!(state
            .create_room("fur_elise".into(), GameMode::Classic, false, "extra".into(), "x".into())
            .await
            .is_none());
    }
}
