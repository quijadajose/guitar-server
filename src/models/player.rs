use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlayerRole {
    Host,
    Guest,
    Spectator,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerSummary {
    pub session_id: String,
    pub name: String,
    pub role: PlayerRole,
    pub ready: bool,
    pub score: u32,
    pub combo: u32,
    pub accuracy: f32,
    pub is_disqualified: bool,
}

#[derive(Debug, Clone)]
pub struct Player {
    pub session_id: String,
    pub name: String,
    pub role: PlayerRole,
    pub ready: bool,
    pub score: u32,
    pub combo: u32,
    pub accuracy: f32,
    pub checksum: Option<String>,
    pub is_disqualified: bool,
    /// Aciertos seguidos en Face-Off. Un ataque gasta 8.
    pub attack_charge: u32,
    pub seen_notes: HashSet<u32>,
}

impl Player {
    pub fn new(session_id: String, name: String, role: PlayerRole) -> Self {
        Self {
            session_id,
            name,
            role,
            ready: false,
            score: 0,
            combo: 0,
            accuracy: 100.0,
            checksum: None,
            is_disqualified: false,
            attack_charge: 0,
            seen_notes: HashSet::new(),
        }
    }

    /// Estado limpio para una partida nueva (incluida la revancha).
    pub fn reset_match(&mut self) {
        self.attack_charge = 0;
        self.seen_notes.clear();
        self.checksum = None;
        self.is_disqualified = false;
        self.score = 0;
        self.combo = 0;
        self.accuracy = 100.0;
    }

    /// Solo cuenta un id de nota que existe en la canción, una sola vez.
    pub fn register_note(&mut self, note_id: u32, rating: &str, events: u32) {
        if note_id == 0 || note_id > events {
            return;
        }
        if rating.eq_ignore_ascii_case("MISS") {
            self.attack_charge = 0;
            return;
        }
        if !self.seen_notes.insert(note_id) {
            return;
        }
        if self.attack_charge < 8 {
            self.attack_charge += 1;
        }
    }

    pub fn try_spend_attack(&mut self) -> bool {
        if self.attack_charge < 8 {
            return false;
        }
        self.attack_charge = 0;
        true
    }

    pub fn to_summary(&self) -> PlayerSummary {
        PlayerSummary {
            session_id: self.session_id.clone(),
            name: self.name.clone(),
            role: self.role.clone(),
            ready: self.ready,
            score: self.score,
            combo: self.combo,
            accuracy: self.accuracy,
            is_disqualified: self.is_disqualified,
        }
    }
}
