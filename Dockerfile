# Etapa 1: Build
FROM rust:1-slim AS builder

WORKDIR /app
# Copiar solo lo necesario: nunca .env ni target/ (ver .dockerignore)
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

# Etapa 2: Imagen final
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app

WORKDIR /app
COPY --from=builder /app/target/release/server /app/server

ENV PORT=3000
EXPOSE 3000

# No correr como root dentro del contenedor
USER app

CMD ["/app/server"]
