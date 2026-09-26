use serde::{Deserialize, Serialize};

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
        }
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
        }
    }
}
