use crate::{
    auth,
    protocol::{ClientMessage, ServerMessage},
    state::{now_epoch_ms, AppState, RoomHandle},
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use std::{net::SocketAddr, time::{Duration, Instant}};
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
const MAX_CHAT_CHARS: usize = 180;
const MAX_ATTACK_MS: u32 = 30_000;
const PROGRESS_PER_SEC: u32 = 10;
const NOTES_PER_SEC: u32 = 20;
/// Mensajes de control (crear/unirse/chat/listo/...) por ventana de 10 s.
const CONTROL_PER_10S: u32 = 20;
/// Cualquier mensaje, incluidos los inválidos, por segundo.
const ANY_PER_SEC: u32 = 60;
/// Sin ningún mensaje del cliente (ni ping) durante este tiempo, se corta.
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

struct RateGate {
    progress: Window,
    notes: Window,
    control: Window,
    any: Window,
}

struct Window {
    started: Instant,
    count: u32,
    limit: u32,
    span: Duration,
}

impl Window {
    fn new(limit: u32, span: Duration) -> Self {
        Self { started: Instant::now(), count: 0, limit, span }
    }

    fn allow(&mut self) -> bool {
        if self.started.elapsed() >= self.span {
            self.started = Instant::now();
            self.count = 0;
        }
        if self.count >= self.limit {
            return false;
        }
        self.count += 1;
        true
    }
}

impl RateGate {
    fn new() -> Self {
        Self {
            progress: Window::new(PROGRESS_PER_SEC, Duration::from_secs(1)),
            notes: Window::new(NOTES_PER_SEC, Duration::from_secs(1)),
            control: Window::new(CONTROL_PER_10S, Duration::from_secs(10)),
            any: Window::new(ANY_PER_SEC, Duration::from_secs(1)),
        }
    }
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
        .filter(|c| !c.is_control() && !is_invisible(*c) && *c != '<' && *c != '>' && *c != '&' && *c != '"' && *c != '\'')
        .take(MAX_NAME_CHARS)
        .collect();
    let cleaned = cleaned.trim().to_string();
    if cleaned.is_empty() { "Player".to_string() } else { cleaned }
}

/// Caracteres de formato (bidi, ancho cero) que permiten disfrazar nombres o textos.
fn is_invisible(c: char) -> bool {
    matches!(c, '\u{200B}'..='\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2060}'..='\u{2069}' | '\u{FEFF}')
}

fn plain_text(value: &str, max_chars: usize) -> String {
    value
        .trim()
        .chars()
        .filter(|c| (!c.is_control() || *c == ' ') && !is_invisible(*c))
        .take(max_chars)
        .collect::<String>()
        .trim()
        .to_string()
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

struct SocketPermit {
    state: AppState,
    ip: String,
}

impl Drop for SocketPermit {
    fn drop(&mut self) {
        self.state.release_socket();
        self.state.release_ip(&self.ip);
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
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    // Cross-Site WebSocket Hijacking: el navegador siempre manda Origin; si viene, tiene que ser nuestro.
    if let Some(origin) = headers.get(axum::http::header::ORIGIN) {
        let allowed = origin
            .to_str()
            .is_ok_and(|value| auth::origin_allowed(&state.auth.config, value));
        if !allowed {
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let ip = auth::client_ip(&state.auth.config, &headers, &addr);
    if !state.try_acquire_ip(&ip) {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    }
    if !state.try_acquire_socket() {
        state.release_ip(&ip);
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let permit = SocketPermit { state: state.clone(), ip };
    ws.max_message_size(MAX_MESSAGE_BYTES)
        .max_frame_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| handle_socket(socket, state, permit))
        .into_response()
}

/// Saca la sesión de la sala actual y avisa al resto. Borra la sala si queda vacía.
async fn leave_room(state: &AppState, code: &str, session_id: &str) {
    let Some(room_handle) = state.get_room(code) else {
        return;
    };
    let empty = {
        let mut room = room_handle.write().await;
        room.remove_session(session_id);
        room.touch();
        let _ = room.tx.send(ServerMessage::RoomUpdated {
            players: room.players_summary(),
            spectators_count: room.spectators.len(),
        });
        room.is_empty()
    };
    if empty {
        state.remove_room(code);
    }
}

async fn current_room(state: &AppState, code: &Option<String>) -> Option<RoomHandle> {
    code.as_deref().and_then(|code| state.get_room(code))
}

async fn handle_socket(socket: WebSocket, state: AppState, _permit: SocketPermit) {
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

    loop {
        let next = match tokio::time::timeout(IDLE_TIMEOUT, receiver.next()).await {
            Ok(next) => next,
            Err(_) => break, // conexión muda: liberar el cupo
        };
        let Some(msg_res) = next else { break };
        let msg = match msg_res {
            Ok(Message::Text(txt)) => txt,
            Ok(Message::Close(_)) => break,
            Ok(_) => continue,
            Err(_) => break,
        };
        if !rates.any.allow() {
            continue;
        }

        let client_msg: ClientMessage = match serde_json::from_str(&msg) {
            Ok(m) => m,
            Err(_) => continue,
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
                if !rates.control.allow() {
                    emit_error(&out_tx, "Demasiadas acciones seguidas, esperá unos segundos").await;
                    continue;
                }
                let song_id = plain_text(&song_id, MAX_SONG_ID_CHARS);
                if song_budget(&song_id).is_none() {
                    emit_error(&out_tx, "canción no disponible para versus").await;
                    continue;
                }
                let player_name = plain_name(&player_name);

                // Salir de la sala anterior: si no, un socket podía acaparar todas las salas.
                if let Some(task) = forward.take() {
                    task.abort();
                }
                if let Some(code) = current_room_code.take() {
                    leave_room(&state, &code, &session_id).await;
                }

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
                if !rates.control.allow() {
                    emit_error(&out_tx, "Demasiadas acciones seguidas, esperá unos segundos").await;
                    continue;
                }
                let player_name = plain_name(&player_name);
                let Some(room_handle) = state.get_room(&room_code) else {
                    emit_error(&out_tx, "Sala no encontrada o expirada").await;
                    continue;
                };
                let target_code = room_handle.read().await.code.clone();
                if current_room_code.as_deref() == Some(target_code.as_str()) {
                    emit_error(&out_tx, "Ya estás en esta sala").await;
                    continue;
                }

                if let Some(task) = forward.take() {
                    task.abort();
                }
                if let Some(code) = current_room_code.take() {
                    leave_room(&state, &code, &session_id).await;
                }

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
                if !rates.control.allow() {
                    continue;
                }
                let Some(room_handle) = current_room(&state, &current_room_code).await else {
                    continue;
                };
                let mut room = room_handle.write().await;
                room.expire_stale_match();
                if room.is_playing {
                    continue;
                }
                let Some(player) = room.player_mut(&session_id) else {
                    continue;
                };
                player.ready = ready;
                room.touch();
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
                if !rates.control.allow() {
                    continue;
                }
                let song_id = plain_text(&song_id, MAX_SONG_ID_CHARS);
                if song_budget(&song_id).is_none() {
                    emit_error(&out_tx, "canción no disponible para versus").await;
                    continue;
                }
                let Some(room_handle) = current_room(&state, &current_room_code).await else {
                    continue;
                };
                let mut room = room_handle.write().await;
                room.expire_stale_match();
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
                if !rates.control.allow() {
                    continue;
                }
                let text = plain_text(&text, MAX_CHAT_CHARS);
                if text.is_empty() {
                    continue;
                }
                let Some(room_handle) = current_room(&state, &current_room_code).await else {
                    continue;
                };
                let room = room_handle.read().await;
                let name = room
                    .player(&session_id)
                    .map(|player| player.name.clone())
                    .unwrap_or_else(|| "Espectador".to_string());
                room.touch();
                let _ = room.tx.send(ServerMessage::Chat { name, text });
            }

            ClientMessage::StartGame => {
                if !rates.control.allow() {
                    continue;
                }
                let Some(room_handle) = current_room(&state, &current_room_code).await else {
                    continue;
                };
                let mut room = room_handle.write().await;
                let is_host = room
                    .host
                    .as_ref()
                    .is_some_and(|h| h.session_id == session_id);
                let both_ready = room.host.as_ref().is_some_and(|h| h.ready)
                    && room.guest.as_ref().is_some_and(|g| g.ready);
                room.expire_stale_match();
                if is_host && both_ready && !room.is_playing {
                    room.start_match();
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
                if !rates.progress.allow() {
                    continue;
                }
                let Some(room_handle) = current_room(&state, &current_room_code).await else {
                    continue;
                };
                let room = room_handle.read().await;
                if !room.is_playing || room.player(&session_id).is_none() {
                    continue;
                }
                room.touch();
                let Some(budget) = song_budget(&room.song_id) else {
                    continue;
                };
                let max_score = budget.events.saturating_mul(budget.points_per_hit).saturating_mul(4);
                let accuracy = if accuracy.is_finite() { accuracy.clamp(0.0, 100.0) } else { 0.0 };
                let _ = room.tx.send(ServerMessage::OpponentProgress {
                    session_id: session_id.clone(),
                    score: score.min(max_score),
                    combo: combo.min(budget.events),
                    accuracy,
                    measure: measure.min(64),
                });
            }

            ClientMessage::NoteHit {
                note_id,
                rating,
                cents_offset,
            } => {
                if !rates.notes.allow() {
                    continue;
                }
                let rating = plain_text(&rating, MAX_RATING_CHARS);
                let Some(room_handle) = current_room(&state, &current_room_code).await else {
                    continue;
                };
                let mut room = room_handle.write().await;
                if !room.is_playing {
                    continue;
                }
                let events = song_budget(&room.song_id).map(|budget| budget.events).unwrap_or(0);
                if note_id == 0 || note_id > events {
                    continue;
                }
                let Some(player) = room.player_mut(&session_id) else {
                    continue;
                };
                player.register_note(note_id, &rating, events);
                room.touch();
                let _ = room.tx.send(ServerMessage::OpponentNoteHit {
                    session_id: session_id.clone(),
                    note_id,
                    rating,
                    cents_offset: cents_offset.clamp(-1200, 1200),
                });
            }

            ClientMessage::SendAttack {
                attack_type,
                duration_ms,
            } => {
                let Some(room_handle) = current_room(&state, &current_room_code).await else {
                    continue;
                };
                let mut room = room_handle.write().await;
                if room.mode != crate::models::GameMode::FaceOff || !room.is_playing {
                    continue;
                }
                let charged = room
                    .player_mut(&session_id)
                    .is_some_and(|player| player.try_spend_attack());
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
                if !rates.control.allow() {
                    continue;
                }
                let reason = plain_text(&reason, MAX_REASON_CHARS);
                let Some(room_handle) = current_room(&state, &current_room_code).await else {
                    continue;
                };
                let room = room_handle.read().await;
                if !room.is_playing || room.player(&session_id).is_none() {
                    continue;
                }
                let max_score = song_budget(&room.song_id)
                    .map(|b| b.events.saturating_mul(b.points_per_hit).saturating_mul(4))
                    .unwrap_or(0);
                room.touch();
                let _ = room.tx.send(ServerMessage::OpponentEliminated {
                    session_id: session_id.clone(),
                    reason,
                    final_score: final_score.min(max_score),
                });
            }

            ClientMessage::RequestRematch => {
                if !rates.control.allow() {
                    continue;
                }
                let Some(room_handle) = current_room(&state, &current_room_code).await else {
                    continue;
                };
                let room = room_handle.read().await;
                if room.player(&session_id).is_none() {
                    continue;
                }
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
                if !rates.control.allow() {
                    continue;
                }
                let sanitized_checksum = plain_text(&checksum, MAX_CHECKSUM_CHARS);
                let Some(room_handle) = current_room(&state, &current_room_code).await else {
                    continue;
                };
                let mut room = room_handle.write().await;
                if !room.is_playing {
                    continue;
                }
                let song_id = room.song_id.clone();
                let budget = song_budget(&song_id);
                let Some(player) = room.player_mut(&session_id) else {
                    continue;
                };
                // Un solo resumen por partida: no se puede reintentar hasta acertar.
                if player.checksum.is_some() {
                    continue;
                }

                let expected = format!(
                    "{song_id}:{final_score}:{max_combo}:{accuracy:.1}:{hits}:{total_notes}"
                );
                let checksum_ok = sanitized_checksum == fnv1a_hex(&expected);
                let is_cheated = match budget {
                    Some(budget) => {
                        let max_score = budget.events.saturating_mul(budget.points_per_hit).saturating_mul(4);
                        !checksum_ok
                            || !accuracy.is_finite()
                            || !(0.0..=100.0).contains(&accuracy)
                            || total_notes != budget.events
                            || hits > budget.events
                            || max_combo > budget.events
                            || max_combo > hits
                            || final_score > max_score
                    }
                    None => true,
                };

                player.score = if is_cheated { 0 } else { final_score };
                player.combo = if is_cheated { 0 } else { max_combo };
                player.accuracy = if is_cheated { 0.0 } else { accuracy.clamp(0.0, 100.0) };
                player.checksum = Some(sanitized_checksum);
                player.is_disqualified = is_cheated;
                if is_cheated {
                    warn!("Anti-cheat: session {} disqualified (score: {}, combo: {})", session_id, final_score, max_combo);
                }
                room.touch();
                room.finish_if_all_submitted();
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
    if let Some(code) = current_room_code.take() {
        leave_room(&state, &code, &session_id).await;
    }
    drop(out_tx);
    writer.abort();
}

#[cfg(test)]
mod tests {
    use crate::models::GameMode;
    use crate::protocol::ServerMessage;
    use crate::state::tests::test_state;

    #[test]
    fn parses_create_room() {
        let raw = r#"{"type":"create_room","payload":{"song_id":"fur_elise","mode":"classic","player_name":"Ana","is_public":false}}"#;
        serde_json::from_str::<crate::protocol::ClientMessage>(raw).expect("parse");
    }

    #[test]
    fn names_and_text_drop_markup_and_bidi() {
        assert_eq!(super::plain_name("  <b>Ana</b>\u{202E}  "), "bAna/b");
        assert_eq!(super::plain_name("\u{200B}"), "Player");
        assert_eq!(super::plain_text("hola\u{0007} mundo", 180), "hola mundo");
    }

    #[test]
    fn checksum_matches_client_formula() {
        // Misma fórmula que versusLobby.ts (FNV-1a de 32 bits).
        let raw = "fur_elise:1000:10:95.5:20:130";
        assert_eq!(super::fnv1a_hex(raw).len(), 8);
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

    #[tokio::test]
    async fn leaving_last_seat_removes_room() {
        let state = test_state();
        let (code, _) = state
            .create_room("fur_elise".into(), GameMode::Classic, false, "h".into(), "Ana".into())
            .await
            .expect("sala");
        super::leave_room(&state, &code, "h").await;
        assert!(state.get_room(&code).is_none());
    }

    #[test]
    fn per_ip_socket_cap() {
        let state = test_state();
        for _ in 0..crate::state::MAX_SOCKETS_PER_IP {
            assert!(state.try_acquire_ip("1.1.1.1"));
        }
        assert!(!state.try_acquire_ip("1.1.1.1"));
        state.release_ip("1.1.1.1");
        assert!(state.try_acquire_ip("1.1.1.1"));
    }
}
