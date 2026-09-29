use crate::models::{AttackType, GameMode, PlayerRole, PlayerSummary};
use serde::{Deserialize, Serialize};

/// Mensajes enviados por el cliente WebSocket
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum ClientMessage {
    Ping {
        client_ts: u64,
    },
    CreateRoom {
        song_id: String,
        mode: GameMode,
        player_name: String,
        is_public: bool,
    },
    JoinRoom {
        room_code: String,
        player_name: String,
        as_spectator: bool,
    },
    SetReady {
        ready: bool,
    },
    UpdateRoom {
        song_id: String,
        mode: GameMode,
        is_public: bool,
    },
    Chat {
        text: String,
    },
    StartGame,
    PlayerProgress {
        score: u32,
        combo: u32,
        accuracy: f32,
        measure: u32,
    },
    NoteHit {
        note_id: u32,
        rating: String,
        cents_offset: i16,
    },
    SendAttack {
        attack_type: AttackType,
        duration_ms: u32,
    },
    PlayerEliminated {
        reason: String,
        final_score: u32,
    },
    SubmitSummary {
        final_score: u32,
        max_combo: u32,
        accuracy: f32,
        hits: u32,
        total_notes: u32,
        checksum: String,
    },
    RequestRematch,
}

/// Mensajes enviados por el servidor WebSocket a los clientes
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum ServerMessage {
    Pong {
        client_ts: u64,
        server_ts: u64,
    },
    RoomCreated {
        room_code: String,
        session_id: String,
        role: PlayerRole,
        song_id: String,
        mode: GameMode,
        is_public: bool,
    },
    RoomJoined {
        room_code: String,
        session_id: String,
        role: PlayerRole,
        song_id: String,
        mode: GameMode,
        is_public: bool,
        players: Vec<PlayerSummary>,
        spectators_count: usize,
    },
    RoomUpdated {
        players: Vec<PlayerSummary>,
        spectators_count: usize,
    },
    RoomSettings {
        song_id: String,
        mode: GameMode,
        is_public: bool,
    },
    Chat {
        name: String,
        text: String,
    },
    GameStarting {
        start_at_epoch_ms: u64,
        countdown_ms: u32,
    },
    OpponentProgress {
        session_id: String,
        score: u32,
        combo: u32,
        accuracy: f32,
        measure: u32,
    },
    OpponentNoteHit {
        session_id: String,
        note_id: u32,
        rating: String,
        cents_offset: i16,
    },
    ApplyAttack {
        from_session_id: String,
        attack_type: AttackType,
        duration_ms: u32,
    },
    OpponentEliminated {
        session_id: String,
        reason: String,
        final_score: u32,
    },
    RematchRequested {
        by_session_id: String,
    },
    Error {
        message: String,
    },
    RoomClosing {
        closes_in_secs: u64,
        reason: String,
    },
    RoomClosed {
        reason: String,
    },
}
