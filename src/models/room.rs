use super::player::{Player, PlayerRole, PlayerSummary};
use crate::protocol::ServerMessage;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

pub const MAX_SPECTATORS: usize = 8;

#[derive(Debug, Clone, Serialize)]
pub struct PublicRoom {
    pub code: String,
    pub song_id: String,
    pub mode: GameMode,
    pub host_name: String,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GameMode {
    Classic,
    SuddenDeath,
    FaceOff,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttackType {
    InvertScreen,
    BlindStrings,
    TurboSpeed,
}

pub struct Room {
    pub code: String,
    pub song_id: String,
    pub mode: GameMode,
    pub is_public: bool,
    pub host: Option<Player>,
    pub guest: Option<Player>,
    pub spectators: Vec<String>, // session_ids
    pub tx: broadcast::Sender<ServerMessage>,
    pub last_activity_secs: AtomicU64,
    pub closing_notice_sent: AtomicBool,
    pub is_playing: bool,
}

impl Room {
    pub fn new(code: String, song_id: String, mode: GameMode, is_public: bool) -> Self {
        let (tx, _) = broadcast::channel(64);
        Self {
            code,
            song_id,
            mode,
            is_public,
            host: None,
            guest: None,
            spectators: Vec::new(),
            tx,
            last_activity_secs: AtomicU64::new(now_secs()),
            closing_notice_sent: AtomicBool::new(false),
            is_playing: false,
        }
    }

    pub fn touch(&self) {
        self.last_activity_secs.store(now_secs(), Ordering::Relaxed);
        self.closing_notice_sent.store(false, Ordering::Relaxed);
    }

    pub fn idle_secs(&self) -> u64 {
        now_secs().saturating_sub(self.last_activity_secs.load(Ordering::Relaxed))
    }

    pub fn is_public(&self) -> bool {
        self.is_public
    }

    pub fn players_summary(&self) -> Vec<PlayerSummary> {
        let mut list = Vec::new();
        if let Some(ref h) = self.host {
            list.push(h.to_summary());
        }
        if let Some(ref g) = self.guest {
            list.push(g.to_summary());
        }
        list
    }

    pub fn add_player(
        &mut self,
        session_id: String,
        name: String,
        as_spectator: bool,
    ) -> Option<PlayerRole> {
        self.touch();

        if as_spectator {
            if self.spectators.len() >= MAX_SPECTATORS {
                return None;
            }
            self.spectators.push(session_id);
            return Some(PlayerRole::Spectator);
        }
        if self.guest.is_none() {
            self.guest = Some(Player::new(session_id, name, PlayerRole::Guest));
            return Some(PlayerRole::Guest);
        }
        if self.spectators.len() >= MAX_SPECTATORS {
            return None;
        }
        self.spectators.push(session_id);
        Some(PlayerRole::Spectator)
    }

    pub fn remove_session(&mut self, session_id: &str) {
        if let Some(ref h) = self.host {
            if h.session_id == session_id {
                self.host = None;
            }
        }
        if let Some(ref g) = self.guest {
            if g.session_id == session_id {
                self.guest = None;
            }
        }
        self.spectators.retain(|s| s != session_id);
    }
}
