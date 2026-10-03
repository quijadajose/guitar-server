use super::player::{Player, PlayerRole, PlayerSummary};
use crate::protocol::ServerMessage;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Ninguna canción de versus dura tanto: si nadie mandó el resumen, se libera la sala.
const MAX_MATCH_SECS: u64 = 15 * 60;
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
    pub playing_since: Option<Instant>,
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
            playing_since: None,
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

    pub fn start_match(&mut self) {
        self.is_playing = true;
        self.playing_since = Some(Instant::now());
    }

    fn stop_match(&mut self) {
        self.is_playing = false;
        self.playing_since = None;
        for player in [self.host.as_mut(), self.guest.as_mut()].into_iter().flatten() {
            player.ready = false;
        }
    }

    /// Si un cliente se colgó sin mandar el resumen, la sala no queda trabada para siempre.
    pub fn expire_stale_match(&mut self) {
        if self.is_playing
            && self.playing_since.is_some_and(|since| since.elapsed().as_secs() > MAX_MATCH_SECS)
        {
            self.stop_match();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.host.is_none() && self.guest.is_none() && self.spectators.is_empty()
    }

    /// Host o invitado: los espectadores no pueden mandar progreso, notas, ataques ni chat de partida.
    pub fn player_mut(&mut self, session_id: &str) -> Option<&mut Player> {
        if self.host.as_ref().is_some_and(|p| p.session_id == session_id) {
            return self.host.as_mut();
        }
        if self.guest.as_ref().is_some_and(|p| p.session_id == session_id) {
            return self.guest.as_mut();
        }
        None
    }

    pub fn player(&self, session_id: &str) -> Option<&Player> {
        self.host
            .as_ref()
            .filter(|p| p.session_id == session_id)
            .or(self.guest.as_ref().filter(|p| p.session_id == session_id))
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

        // No permitir que la misma sesión ocupe dos lugares.
        if self.player(&session_id).is_some() || self.spectators.contains(&session_id) {
            return None;
        }

        if as_spectator {
            if self.spectators.len() >= MAX_SPECTATORS {
                return None;
            }
            self.spectators.push(session_id);
            return Some(PlayerRole::Spectator);
        }
        // Una partida en curso no admite un invitado nuevo a mitad de canción.
        if self.guest.is_none() && !self.is_playing {
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
        let was_player = self.player(session_id).is_some();
        if self.host.as_ref().is_some_and(|h| h.session_id == session_id) {
            // Si se va el anfitrión, el invitado pasa a ser anfitrión para que la sala siga usable.
            self.host = self.guest.take().map(|mut guest| {
                guest.role = PlayerRole::Host;
                guest
            });
        }
        if self.guest.as_ref().is_some_and(|g| g.session_id == session_id) {
            self.guest = None;
        }
        self.spectators.retain(|s| s != session_id);
        if was_player {
            // Con un solo jugador no hay partida: liberar la sala.
            self.stop_match();
        }
    }

    /// Cierra la partida cuando todos los jugadores presentes mandaron su resumen.
    pub fn finish_if_all_submitted(&mut self) -> bool {
        if !self.is_playing {
            return false;
        }
        let players: Vec<&Player> = [self.host.as_ref(), self.guest.as_ref()].into_iter().flatten().collect();
        if players.is_empty() || players.iter().any(|p| p.checksum.is_none()) {
            return false;
        }
        self.stop_match();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn room() -> Room {
        let mut room = Room::new("ABCDE".into(), "fur_elise".into(), GameMode::Classic, false);
        room.host = Some(Player::new("h".into(), "Ana".into(), PlayerRole::Host));
        room
    }

    #[test]
    fn same_session_cannot_join_twice() {
        let mut room = room();
        assert_eq!(room.add_player("g".into(), "Luis".into(), false), Some(PlayerRole::Guest));
        assert_eq!(room.add_player("g".into(), "Luis".into(), false), None);
        assert_eq!(room.add_player("h".into(), "Ana".into(), true), None);
    }

    #[test]
    fn host_leaving_promotes_guest_and_stops_match() {
        let mut room = room();
        room.add_player("g".into(), "Luis".into(), false);
        room.is_playing = true;
        room.remove_session("h");
        assert!(!room.is_playing);
        let host = room.host.as_ref().expect("nuevo anfitrión");
        assert_eq!(host.session_id, "g");
        assert_eq!(host.role, PlayerRole::Host);
        assert!(room.guest.is_none());
    }

    #[test]
    fn match_ends_when_everyone_submits() {
        let mut room = room();
        room.add_player("g".into(), "Luis".into(), false);
        room.is_playing = true;
        room.host.as_mut().unwrap().checksum = Some("x".into());
        assert!(!room.finish_if_all_submitted());
        room.guest.as_mut().unwrap().checksum = Some("y".into());
        assert!(room.finish_if_all_submitted());
        assert!(!room.is_playing);
    }

    #[test]
    fn no_guest_joins_mid_match() {
        let mut room = room();
        room.is_playing = true;
        assert_eq!(room.add_player("g".into(), "Luis".into(), false), Some(PlayerRole::Spectator));
    }
}
