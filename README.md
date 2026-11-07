# guitar-server

WebSocket multiplayer server and backend services for Guitar VS.

## Features

- Real-time multiplayer rooms via WebSockets
- Rate limiting and anti-cheat validation
- Supabase authentication & authorization integration
- Containerized deployment ready (Docker, Docker Compose)

## Estado en memoria

Las salas, las partidas en curso, las manos de cada jugador, los límites de conexiones por IP y los contadores de rate limit viven **en memoria del proceso**. No hay base de datos local para el juego.

Consecuencias que conviene tener presentes:

- Al reiniciar el servidor (deploy, caída o `SIGTERM`), todas las salas y partidas se pierden. Antes de cerrar, el servidor avisa a los clientes con `room_closed` (motivo `restart`) y espera un instante para que lleguen.
- Las salas vacías o inactivas se limpian solas (motivos `empty` e `idle`).
- Si se ejecutan varias instancias detrás de un balanceador, cada una tiene su propio conjunto de salas: una sala creada en una instancia no existe en la otra. Escalar horizontalmente requiere mover el estado a un almacén compartido (por ejemplo Redis) antes de hacerlo.
- Los datos persistentes (canciones de la comunidad y cuentas) viven en Supabase, no aquí.
