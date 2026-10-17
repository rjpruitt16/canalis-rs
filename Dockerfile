FROM rust:1.99-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/canalis-rs .
# The waiting line lives on local disk; mount a volume here so it
# survives the container being replaced, not just the process restarting.
ENV CANALIS_LINE_PATH=/data/canalis-line.redb
# Canalis's own L8 key, kept with the line so it survives a new container.
ENV CANALIS_L8_KEY_PATH=/data/.l8-key
# Created here, not left to VOLUME: plain `docker run` makes the mount
# point, but not every runtime does (Dagger, which the contract tests use,
# doesn't), and the line can't open in a directory that isn't there.
RUN mkdir -p /data
VOLUME /data
EXPOSE 8080
# Every caller waiting in line holds a connection, so the default limit
# of open files (often 1,024 or 10,240) is the first ceiling a busy
# Canalis hits. Raise it as far as the runtime allows.
CMD ["sh", "-c", "ulimit -n 1048576 2>/dev/null || ulimit -n 65536 2>/dev/null; exec ./canalis-rs"]
