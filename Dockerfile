# Etapa 1: Build
FROM rust:1-slim AS builder

WORKDIR /app
COPY . .
RUN cargo build --release

# Etapa 2: Imagen final
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/server /app/server

ENV PORT=3000
EXPOSE 3000

CMD ["/app/server"]
