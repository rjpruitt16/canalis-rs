# Changelog

Canalis follows [semantic versioning](https://semver.org). Before 1.0, a minor version (0.x) may change the protocol shared with Aquifer and Spillway; the changelog says when it does.

## [Unreleased]

### Added

- **Pools** ([#20](https://github.com/rjpruitt16/canalis-rs/issues/20)): instances join pools at registration (`AQUIFER_REGISTRY_POOLS`, `SPILLWAY_REGISTRY_POOLS`), and requests pick one with `X-Aqueduct-Pool`. Each pool waits in line separately.
- **Authentication** ([#25](https://github.com/rjpruitt16/canalis-rs/issues/25)): `CANALIS_AUTH=upstream-secret` (trust only your gateway) or `jwt` (verify the caller's token against the provider's JWKS and take the tenant, and optionally the pool, from its claims).

## [0.1.0] - 2026-10-14

The first release. Canalis is an admission gateway in front of Aquifer and Spillway. It decides whether a request gets in, when, and to which instance, and holds the rest in a line on its own disk.

### Added

- **Shared pool with Aquifer.** Instances are kept in Aquifer's `aqueduct:` Valkey keys and claimed with the same Lua script, so Canalis and Aquifer's cluster mode share one pool. Assignment is sticky per user, ranked by rendezvous hashing. It supports reserved instances (`reserved_for`), drain-mode instances, and instances dedicated to HTTP or WebSocket traffic (`role`).
- **Registration.** `POST /register` takes Aquifer's and Spillway's pings (`AQUIFER_REGISTRY_URL` / `SPILLWAY_REGISTRY_URL`). Registrations expire on their own, and a draining or offline instance is taken out of rotation.
- **Admission by fullness.** Instances report `X-Aqueduct-Fullness` (0–100). At `CANALIS_QUEUE_AT_PERCENT` (80) an instance gets no new users.
- **The line.** When no instance has room, jobs wait in a durable local line (redb), bounded overall, per tenant and by bytes, with a 429 and `Retry-After` past those bounds. Callers get a waiting-room stream (`canalis_queued`, `canalis_position`, `canalis_assigned`, `canalis_result`), then the instance's own stream. `GET /jobs/{key}` reports status and cached results. Repeated idempotency keys are deduplicated.
- **WebSockets.** `GET /websocket` is routed by role and piped both ways, with Aquifer's `aqueduct.v1` protocol passed through. When no instance has room, sessions wait (`canalis_waiting`, `canalis_assigned`) instead of being refused.
- **L8.** Canalis is an L8 receiver (`/.well-known/l8`, `/l8/challenge`). It can accept registrations only from an allowlist of node keys (`CANALIS_L8_TRUSTED_NODE_KEYS`, `CANALIS_REQUIRE_SIGNED_REGISTRATION`). With `CANALIS_L8_SCHEMA_VALIDATION`, requests that don't match the upstream's published `request_schemas` get a 422 before they take a place in line.
- **Graceful shutdown.** On SIGTERM, new work gets a draining 503, `/ready` returns 503, WebSockets get `server_draining` and then a 1012 close, and queued jobs stay on disk.
- **Scale-up signal.** `canalis_scale_up` is POSTed to `CANALIS_SCALE_WEBHOOK_URL` when the line backs up. `/health` reports the line and the fleet.
- **Metrics.** Prometheus text at `GET /metrics`: responses by route and status, latency, the line, each instance, WebSockets, re-routes, and Valkey reachability.
- **Failure handling.**
  - An instance that refuses connections is removed from rotation, and its requests are re-routed.
  - While Valkey is unreachable, jobs wait in the line.
  - A job being handed off when Canalis crashes goes back in line on restart (at-least-once delivery).
- **Packaging.** Multi-arch image at `ghcr.io/rjpruitt16/canalis-rs`, `compose.yaml` for a local stack with Aquifer, and a [configuration reference](docs/configuration.md).

### Known limitations

- Canalis trusts the user ID a caller sends; put it behind a gateway that authenticates callers ([#18](https://github.com/rjpruitt16/canalis-rs/issues/18)).
- Each Canalis has its own line; run one, or route each tenant to the same one ([#19](https://github.com/rjpruitt16/canalis-rs/issues/19)).
- One pool per Canalis ([#20](https://github.com/rjpruitt16/canalis-rs/issues/20)).
- No per-IP limits ([#21](https://github.com/rjpruitt16/canalis-rs/issues/21)), and no published benchmarks yet ([#17](https://github.com/rjpruitt16/canalis-rs/issues/17)).
