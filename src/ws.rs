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
                if song_id.is_empty() {
                    emit_error(&out_tx, "song_id vacío").await;
                    continue;
                }
                let player_name = {
                    let trimmed = player_name.trim();
                    let name = if trimmed.is_empty() { "Player" } else { trimmed };
                    clip(name, MAX_NAME_CHARS).to_string()
                };

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
                    },
                )
                .await;
            }

            ClientMessage::JoinRoom {
                room_code,
                player_name,
                as_spectator,
            } => {
                let player_name = {
                    let trimmed = player_name.trim();
                    let name = if trimmed.is_empty() { "Player" } else { trimmed };
                    clip(name, MAX_NAME_CHARS).to_string()
                };
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
                                room.players_summary(),
                                room.spectators.len(),
                            )
                        })
                };
                let Some((role, code, song_id, mode, players, spectators_count)) = joined else {
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
                if is_host {
                    room.is_playing = true;
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
                room.touch();
                let _ = room.tx.send(ServerMessage::OpponentProgress {
                    session_id: session_id.clone(),
                    score,
                    combo,
                    accuracy,
                    measure,
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
                let room = room_handle.read().await;
                room.touch();
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
                let room = room_handle.read().await;
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
                checksum,
            } => {
                let checksum = clip(&checksum, MAX_CHECKSUM_CHARS).to_string();
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
                        h.score = final_score;
                        h.combo = max_combo;
                        h.accuracy = accuracy;
                    }
                }
                if let Some(ref mut g) = room.guest {
                    if g.session_id == session_id {
                        g.score = final_score;
                        g.combo = max_combo;
                        g.accuracy = accuracy;
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
