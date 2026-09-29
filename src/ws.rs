use crate::{
    protocol::{ClientMessage, ServerMessage},
    state::{now_epoch_ms, AppState},
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use std::time::Instant;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tracing::warn;

const MAX_MESSAGE_BYTES: usize = 4 * 1024;
const MAX_NAME_CHARS: usize = 24;
const MAX_SONG_ID_CHARS: usize = 64;
const MAX_REASON_CHARS: usize = 48;
const MAX_CHECKSUM_CHARS: usize = 128;
const MAX_RATING_CHARS: usize = 16;
const MAX_ATTACK_MS: u32 = 30_000;
const PROGRESS_PER_SEC: u32 = 10;
const NOTES_PER_SEC: u32 = 20;

struct RateGate {
    progress_window: Instant,
    progress_count: u32,
    note_window: Instant,
    note_count: u32,
}

impl RateGate {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            progress_window: now,
            progress_count: 0,
            note_window: now,
            note_count: 0,
        }
    }

    fn allow_progress(&mut self) -> bool {
        allow(&mut self.progress_window, &mut self.progress_count, PROGRESS_PER_SEC)
    }

    fn allow_note(&mut self) -> bool {
        allow(&mut self.note_window, &mut self.note_count, NOTES_PER_SEC)
    }
}

fn allow(window: &mut Instant, count: &mut u32, limit: u32) -> bool {
    if window.elapsed().as_secs() >= 1 {
        *window = Instant::now();
        *count = 0;
    }
    if *count >= limit {
        return false;
    }
    *count += 1;
    true
}

struct SongBudget {
    events: u32,
    points_per_hit: u32,
}

fn song_budget(song_id: &str) -> Option<SongBudget> {
    match song_id {
        "sultans_swing" => Some(SongBudget { events: 8, points_per_hit: 150 }),
        "fur_elise" => Some(SongBudget { events: 130, points_per_hit: 150 }),
        "chords_progression" => Some(SongBudget { events: 5, points_per_hit: 300 }),
        _ => None,
    }
}

fn fnv1a_hex(raw: &str) -> String {
    let mut hash: u32 = 0x811c9dc5;
    for byte in raw.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    format!("{hash:08x}")
}

fn plain_name(value: &str) -> String {
    let cleaned: String = value
        .trim()
        .chars()
        .filter(|c| !c.is_control() && *c != '<' && *c != '>' && *c != '&' && *c != '"' && *c != '\'')
        .take(MAX_NAME_CHARS)
        .collect();
    if cleaned.is_empty() { "Player".to_string() } else { cleaned }
}

fn clip<'a>(value: &'a str, max_chars: usize) -> &'a str {
    match value.char_indices().nth(max_chars) {
        Some((idx, _)) => &value[..idx],
        None => value,
    }
}

fn should_forward(msg: &ServerMessage, session_id: &str) -> bool {
    match msg {
        ServerMessage::OpponentProgress { session_id: s, .. }
        | ServerMessage::OpponentNoteHit { session_id: s, .. }
        | ServerMessage::OpponentEliminated { session_id: s, .. } => s != session_id,
        ServerMessage::ApplyAttack {
            from_session_id: s,
            ..
        } => s != session_id,
        _ => true,
    }
}

struct SocketPermit(AppState);

impl Drop for SocketPermit {
    fn drop(&mut self) {
        self.0.release_socket();
    }
}

fn spawn_forwarder(
    tx: &broadcast::Sender<ServerMessage>,
    session_id: String,
    out_tx: mpsc::Sender<Message>,
) -> JoinHandle<()> {
    let mut rx = tx.subscribe();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    if !should_forward(&msg, &session_id) {
                        continue;
                    }
                    let Ok(json) = serde_json::to_string(&msg) else {
                        continue;
                    };
                    match out_tx.try_send(Message::Text(json)) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!("Broadcast lagged by {skipped} messages for {session_id}");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

async fn emit(out_tx: &mpsc::Sender<Message>, msg: &ServerMessage) {
    let Ok(json) = serde_json::to_string(msg) else {
        return;
    };
    let _ = out_tx.send(Message::Text(json)).await;
}

async fn emit_error(out_tx: &mpsc::Sender<Message>, message: &str) {
    emit(
        out_tx,
        &ServerMessage::Error {
            message: message.to_string(),
        },
    )
    .await;
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.max_message_size(MAX_MESSAGE_BYTES)
        .max_frame_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    if !state.try_acquire_socket() {
        return;
    }
    let _permit = SocketPermit(state.clone());
    let (mut sender, mut receiver) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(64);
    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    let session_id = uuid::Uuid::new_v4().to_string();
    let mut current_room_code: Option<String> = None;
    let mut forward: Option<JoinHandle<()>> = None;
    let mut rates = RateGate::new();

    while let Some(msg_res) = receiver.next().await {
        let msg = match msg_res {
            Ok(Message::Text(txt)) => txt,
            Ok(Message::Close(_)) => break,
            Ok(_) => continue,
            Err(_) => break,
        };

        let client_msg: ClientMessage = match serde_json::from_str(&msg) {
            Ok(m) => m,
            Err(e) => {
                warn!("Bad message from client: {e}");
                continue;
            }
        };

        match client_msg {
            ClientMessage::Ping { client_ts } => {
                emit(
                    &out_tx,
                    &ServerMessage::Pong {
                        client_ts,
                        server_ts: now_epoch_ms(),
                    },
                )
                .await;
            }

            ClientMessage::CreateRoom {
                song_id,
                mode,
                player_name,
                is_public,
            } => {
                let song_id = clip(song_id.trim(), MAX_SONG_ID_CHARS).to_string();
                if song_budget(&song_id).is_none() {
                    emit_error(&out_tx, "canción no disponible para versus").await;
                    continue;
                }
                let player_name = plain_name(&player_name);

                let Some((code, room_handle)) = state
                    .create_room(
                        song_id.clone(),
                        mode.clone(),
                        is_public,
                        session_id.clone(),
                        player_name,
                    )
                    .await
                else {
                    emit_error(&out_tx, "Servidor lleno, no hay salas libres").await;
                    continue;
                };

                if let Some(task) = forward.take() {
                    task.abort();
                }
                {
                    let room = room_handle.read().await;
                    forward = Some(spawn_forwarder(&room.tx, session_id.clone(), out_tx.clone()));
                }
                current_room_code = Some(code.clone());

                emit(
                    &out_tx,
                    &ServerMessage::RoomCreated {
                        room_code: code,
                        session_id: session_id.clone(),
                        role: crate::models::PlayerRole::Host,
                        song_id,
                        mode,
                        is_public,
                    },
                )
                .await;
            }

            ClientMessage::JoinRoom {
                room_code,
                player_name,
                as_spectator,
            } => {
                let player_name = plain_name(&player_name);
                let Some(room_handle) = state.get_room(&room_code) else {
                    emit_error(&out_tx, "Sala no encontrada o expirada").await;
                    continue;
                };

                let joined = {
                    let mut room = room_handle.write().await;
                    room.add_player(session_id.clone(), player_name, as_spectator)
                        .map(|role| {
                            (
                                role,
                                room.code.clone(),
                                room.song_id.clone(),
                                room.mode.clone(),
                                room.is_public(),
                                room.players_summary(),
                                room.spectators.len(),
                            )
                        })
                };
                let Some((role, code, song_id, mode, is_public, players, spectators_count)) = joined else {
                    emit_error(&out_tx, "Sala llena").await;
                    continue;
                };

                if let Some(task) = forward.take() {
                    task.abort();
                }
                {
                    let room = room_handle.read().await;
                    forward = Some(spawn_forwarder(&room.tx, session_id.clone(), out_tx.clone()));
                    let _ = room.tx.send(ServerMessage::RoomUpdated {
                        players: players.clone(),
                        spectators_count,
                    });
                }
                current_room_code = Some(code.clone());

                emit(
                    &out_tx,
                    &ServerMessage::RoomJoined {
                        room_code: code,
                        session_id: session_id.clone(),
                        role,
                        song_id,
                        mode,
                        is_public,
                        players,
                        spectators_count,
                    },
                )
                .await;
            }

            ClientMessage::SetReady { ready } => {
                let Some(ref code) = current_room_code else {
                    continue;
                };
                let Some(room_handle) = state.get_room(code) else {
                    continue;
                };
                let mut room = room_handle.write().await;
                room.touch();
                if let Some(ref mut h) = room.host {
                    if h.session_id == session_id {
                        h.ready = ready;
                    }
                }
                if let Some(ref mut g) = room.guest {
                    if g.session_id == session_id {
                        g.ready = ready;
                    }
                }
                let _ = room.tx.send(ServerMessage::RoomUpdated {
                    players: room.players_summary(),
                    spectators_count: room.spectators.len(),
                });
            }

            ClientMessage::UpdateRoom {
                song_id,
                mode,
                is_public,
            } => {
                let Some(ref code) = current_room_code else {
                    continue;
                };
                let song_id = clip(song_id.trim(), MAX_SONG_ID_CHARS).to_string();
                if song_budget(&song_id).is_none() {
                    emit_error(&out_tx, "canción no disponible para versus").await;
                    continue;
                }
                let Some(room_handle) = state.get_room(code) else {
                    continue;
                };
                let mut room = room_handle.write().await;
                let is_host = room
                    .host
                    .as_ref()
                    .is_some_and(|host| host.session_id == session_id);
                if !is_host || room.is_playing {
                    emit_error(&out_tx, "solo el anfitrión puede cambiar la sala").await;
                    continue;
                }
                room.song_id = song_id.clone();
                room.mode = mode.clone();
                room.is_public = is_public;
                if let Some(ref mut host) = room.host {
                    host.ready = false;
                }
                if let Some(ref mut guest) = room.guest {
                    guest.ready = false;
                }
                room.touch();
                let _ = room.tx.send(ServerMessage::RoomSettings {
                    song_id,
                    mode,
                    is_public,
                });
                let _ = room.tx.send(ServerMessage::RoomUpdated {
                    players: room.players_summary(),
                    spectators_count: room.spectators.len(),
                });
            }

            ClientMessage::Chat { text } => {
                let text = clip(text.trim(), 180).to_string();
                if text.is_empty() {
                    continue;
                }
                let Some(ref code) = current_room_code else {
                    continue;
                };
                let Some(room_handle) = state.get_room(code) else {
                    continue;
                };
                let room = room_handle.read().await;
                let name = room
                    .host
                    .as_ref()
                    .filter(|player| player.session_id == session_id)
                    .or(room.guest.as_ref().filter(|player| player.session_id == session_id))
                    .map(|player| player.name.clone())
                    .unwrap_or_else(|| "Jugador".to_string());
                let _ = room.tx.send(ServerMessage::Chat { name, text });
            }

            ClientMessage::StartGame => {
                let Some(ref code) = current_room_code else {
                    continue;
                };
                let Some(room_handle) = state.get_room(code) else {
                    continue;
                };
                let mut room = room_handle.write().await;
                let is_host = room
                    .host
                    .as_ref()
                    .map(|h| h.session_id == session_id)
                    .unwrap_or(false);
                let both_ready = room.host.as_ref().is_some_and(|h| h.ready)
                    && room.guest.as_ref().is_some_and(|g| g.ready);
                if is_host && both_ready {
                    room.is_playing = true;
                    if let Some(ref mut h) = room.host {
                        h.reset_match();
                    }
                    if let Some(ref mut g) = room.guest {
                        g.reset_match();
                    }
                    room.touch();
                    let countdown_ms = 3500;
                    let _ = room.tx.send(ServerMessage::GameStarting {
                        start_at_epoch_ms: now_epoch_ms() + countdown_ms as u64,
                        countdown_ms,
                    });
                }
            }

            ClientMessage::PlayerProgress {
                score,
                combo,
                accuracy,
                measure,
            } => {
                if !rates.allow_progress() {
                    continue;
                }
                let Some(ref code) = current_room_code else {
                    continue;
                };
                let Some(room_handle) = state.get_room(code) else {
                    continue;
                };
                let room = room_handle.read().await;
                if !room.is_playing {
                    continue;
                }
                room.touch();
                let Some(budget) = song_budget(&room.song_id) else {
                    continue;
                };
                let max_score = budget.events.saturating_mul(budget.points_per_hit).saturating_mul(4);
                let _ = room.tx.send(ServerMessage::OpponentProgress {
                    session_id: session_id.clone(),
                    score: score.min(max_score),
                    combo: combo.min(budget.events),
                    accuracy: accuracy.clamp(0.0, 100.0),
                    measure: measure.min(64),
                });
            }

            ClientMessage::NoteHit {
                note_id,
                rating,
                cents_offset,
            } => {
                if !rates.allow_note() {
                    continue;
                }
                let rating = clip(&rating, MAX_RATING_CHARS).to_string();
                let Some(ref code) = current_room_code else {
                    continue;
                };
                let Some(room_handle) = state.get_room(code) else {
                    continue;
                };
                let mut room = room_handle.write().await;
                if !room.is_playing {
                    continue;
                }
                room.touch();
                let events = song_budget(&room.song_id).map(|budget| budget.events).unwrap_or(0);
                if let Some(ref mut h) = room.host {
                    if h.session_id == session_id {
                        h.register_note(note_id, &rating, events);
                    }
                }
                if let Some(ref mut g) = room.guest {
                    if g.session_id == session_id {
                        g.register_note(note_id, &rating, events);
                    }
                }
                let _ = room.tx.send(ServerMessage::OpponentNoteHit {
                    session_id: session_id.clone(),
                    note_id,
                    rating,
                    cents_offset,
                });
            }

            ClientMessage::SendAttack {
                attack_type,
                duration_ms,
            } => {
                let Some(ref code) = current_room_code else {
                    continue;
                };
                let Some(room_handle) = state.get_room(code) else {
                    continue;
                };
                let mut room = room_handle.write().await;
                if room.mode != crate::models::GameMode::FaceOff || !room.is_playing {
                    continue;
                }
                let charged = if let Some(player) = room.host.as_mut().filter(|h| h.session_id == session_id) {
                    player.try_spend_attack()
                } else if let Some(player) = room.guest.as_mut().filter(|g| g.session_id == session_id) {
                    player.try_spend_attack()
                } else {
                    false
                };
                if !charged {
                    continue;
                }
                room.touch();
                let _ = room.tx.send(ServerMessage::ApplyAttack {
                    from_session_id: session_id.clone(),
                    attack_type,
                    duration_ms: duration_ms.min(MAX_ATTACK_MS),
                });
            }

            ClientMessage::PlayerEliminated {
                reason,
                final_score,
            } => {
                let reason = clip(&reason, MAX_REASON_CHARS).to_string();
                let Some(ref code) = current_room_code else {
                    continue;
                };
                let Some(room_handle) = state.get_room(code) else {
                    continue;
                };
                let room = room_handle.read().await;
                room.touch();
                let _ = room.tx.send(ServerMessage::OpponentEliminated {
                    session_id: session_id.clone(),
                    reason,
                    final_score,
                });
            }

            ClientMessage::RequestRematch => {
                let Some(ref code) = current_room_code else {
                    continue;
                };
                let Some(room_handle) = state.get_room(code) else {
                    continue;
                };
                let room = room_handle.read().await;
                room.touch();
                let _ = room.tx.send(ServerMessage::RematchRequested {
                    by_session_id: session_id.clone(),
                });
            }

            ClientMessage::SubmitSummary {
                final_score,
                max_combo,
                accuracy,
                hits,
                total_notes,
                checksum,
            } => {
                let sanitized_checksum = clip(&checksum, MAX_CHECKSUM_CHARS).to_string();
                let bounded_accuracy = accuracy.clamp(0.0, 100.0);
                let Some(ref code) = current_room_code else {
                    continue;
                };
                let Some(room_handle) = state.get_room(code) else {
                    continue;
                };
                let mut room = room_handle.write().await;
                room.touch();

                let budget = song_budget(&room.song_id);
                let expected = format!(
                    "{}:{final_score}:{max_combo}:{accuracy:.1}:{hits}:{total_notes}",
                    room.song_id
                );
                let checksum_ok = sanitized_checksum == fnv1a_hex(&expected);
                let is_cheated = match budget {
                    Some(budget) => {
                        let max_score = budget.events.saturating_mul(budget.points_per_hit).saturating_mul(4);
                        !checksum_ok
                            || !accuracy.is_finite()
                            || accuracy > 100.0
                            || total_notes != budget.events
                            || hits > budget.events
                            || max_combo > budget.events
                            || final_score > max_score
                    }
                    None => true,
                };

                if let Some(ref mut h) = room.host {
                    if h.session_id == session_id {
                        h.score = if is_cheated { 0 } else { final_score };
                        h.combo = if is_cheated { 0 } else { max_combo };
                        h.accuracy = if is_cheated { 0.0 } else { bounded_accuracy };
                        h.checksum = Some(sanitized_checksum.clone());
                        h.is_disqualified = is_cheated;
                        if is_cheated {
                            warn!("Anti-cheat: Host session {} disqualified (score: {}, combo: {})", session_id, final_score, max_combo);
                        }
                    }
                }
                if let Some(ref mut g) = room.guest {
                    if g.session_id == session_id {
                        g.score = if is_cheated { 0 } else { final_score };
                        g.combo = if is_cheated { 0 } else { max_combo };
                        g.accuracy = if is_cheated { 0.0 } else { bounded_accuracy };
                        g.checksum = Some(sanitized_checksum);
                        g.is_disqualified = is_cheated;
                        if is_cheated {
                            warn!("Anti-cheat: Guest session {} disqualified (score: {}, combo: {})", session_id, final_score, max_combo);
                        }
                    }
                }
                let _ = room.tx.send(ServerMessage::RoomUpdated {
                    players: room.players_summary(),
                    spectators_count: room.spectators.len(),
                });
            }
        }
    }

    if let Some(task) = forward.take() {
        task.abort();
    }
    if let Some(ref code) = current_room_code {
        if let Some(room_handle) = state.get_room(code) {
            let mut room = room_handle.write().await;
            room.remove_session(&session_id);
            room.touch();
            let _ = room.tx.send(ServerMessage::RoomUpdated {
                players: room.players_summary(),
                spectators_count: room.spectators.len(),
            });
        }
    }
    drop(out_tx);
    writer.abort();
}

#[cfg(test)]
mod tests {
    use crate::auth::{AuthConfig, AuthState, RateLimiter};
    use crate::mail::Mailer;
    use crate::models::GameMode;
    use crate::protocol::ServerMessage;
    use crate::state::AppState;
    use std::sync::Arc;

    fn test_state() -> AppState {
        let auth = AuthState {
            config: AuthConfig {
                supabase_url: "https://example.supabase.co".into(),
                service_key: "test".into(),
                app_url: "http://localhost:5173".into(),
                allowed_origins: vec!["http://localhost:5173".into()],
                http: reqwest::Client::new(),
            },
            mailer: Arc::new(Mailer::new("test".into(), "onboarding@resend.dev".into())),
            limiter: RateLimiter::default(),
        };
        AppState::new(auth)
    }

    #[test]
    fn parses_create_room() {
        let raw = r#"{"type":"create_room","payload":{"song_id":"fur_elise","mode":"classic","player_name":"Ana","is_public":false}}"#;
        serde_json::from_str::<crate::protocol::ClientMessage>(raw).expect("parse");
    }

    #[tokio::test]
    async fn two_players_share_a_note_hit() {
        let state = test_state();
        let (_code, room_handle) = state
            .create_room(
                "fur_elise".into(),
                GameMode::Classic,
                false,
                "host-session".into(),
                "Ana".into(),
            )
            .await
            .expect("sala");
        {
            let mut room = room_handle.write().await;
            room.add_player("guest-session".into(), "Luis".into(), false)
                .expect("invitado");
        }
        let room = room_handle.read().await;
        let mut guest_rx = room.tx.subscribe();
        let hit = ServerMessage::OpponentNoteHit {
            session_id: "host-session".into(),
            note_id: 3,
            rating: "perfect".into(),
            cents_offset: 0,
        };
        room.tx.send(hit.clone()).expect("broadcast");
        let received = guest_rx.recv().await.expect("nota");
        assert!(super::should_forward(&received, "guest-session"));
        assert!(!super::should_forward(&hit, "host-session"));
        match received {
            ServerMessage::OpponentNoteHit { note_id, .. } => assert_eq!(note_id, 3),
            other => panic!("mensaje inesperado: {other:?}"),
        }
    }
}
