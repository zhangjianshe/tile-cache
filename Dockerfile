# syntax=docker/dockerfile:1
FROM rust:1.88-bookworm AS builder
WORKDIR /app
ARG GIT_HASH
ARG BUILD_TIME
COPY Cargo.toml Cargo.lock build.rs ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    GIT_HASH="$GIT_HASH" BUILD_TIME="$BUILD_TIME" cargo build --release --locked \
    && cp /app/target/release/tile-cache /usr/local/bin/tile-cache

FROM debian:bookworm-slim
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN useradd --system --uid 10001 --home-dir /app --create-home app \
    && mkdir -p /app/tiledata /app/config \
    && chown -R app:app /app
COPY --from=builder /usr/local/bin/tile-cache /usr/local/bin/tile-cache
USER app
WORKDIR /app
VOLUME ["/app/tiledata", "/app/config"]
EXPOSE 7601
ENTRYPOINT ["tile-cache"]
