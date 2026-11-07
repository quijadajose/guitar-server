# Etapa 1: Build. Misma versión de Debian que la imagen final (glibc igual en las dos).
FROM rust:1-slim-bookworm AS builder

WORKDIR /app
# Copiar solo lo necesario: nunca .env ni target/ (ver .dockerignore)
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

# Etapa 2: Imagen final
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app

WORKDIR /app
COPY --from=builder /app/target/release/server /app/server

ENV PORT=3000
EXPOSE 3000

# No correr como root dentro del contenedor
USER app

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS "http://127.0.0.1:${PORT}/health" || exit 1

CMD ["/app/server"]
