# Changelog

Canalis follows [semantic versioning](https://semver.org). Before 1.0, a minor version (0.x) may change the protocol shared with Aquifer and Spillway; the changelog says when it does.

## [Unreleased]

### Added

- **Limits** ([#21](https://github.com/rjpruitt16/canalis-rs/issues/21)):
  - per tenant, on by default: 1,000 req/s and bursts of 2,000 cluster-wide, enforced as each node's share, with per-tenant overrides;
  - per client address (100 req/s, 200 open streams), on with `CANALIS_TRUSTED_PROXIES`, which also makes `X-Forwarded-For` trusted from those proxies;
  - a configurable body limit, and a header-read timeout against slow clients.
- **Config file** (`CANALIS_CONFIG_PATH`, YAML, read at startup; environment variables override it).

### Changed

- `CANALIS_MAX_IN_FLIGHT` defaults to 5,000 per CPU instead of 5,000.
- Canalis runs its own HTTP server loop (hyper) instead of `axum::serve`, for the header-read timeout and 10,000 concurrent HTTP/2 streams per connection.
- **Behavior change:** a single tenant sending more than 1,000 req/s (sustained) is now limited. Set `CANALIS_TENANT_RPS=0` to turn it off, or give that tenant its own limit.

## [0.2.0] - 2026-10-16

Pools, authentication, several Canalis nodes, and the fixes from two rounds of Fly benchmarks ([BENCHMARKS.md](BENCHMARKS.md)). New settings are in [docs/configuration.md](docs/configuration.md); existing deployments keep working unchanged.

### Added

- **Pools** ([#20](https://github.com/rjpruitt16/canalis-rs/issues/20)): instances join pools at registration (`AQUIFER_REGISTRY_POOLS`, `SPILLWAY_REGISTRY_POOLS`), and requests pick one with `X-Aqueduct-Pool`. Each pool waits in line separately.
- **Authentication** ([#25](https://github.com/rjpruitt16/canalis-rs/issues/25)): `CANALIS_AUTH=upstream-secret` (trust only your gateway) or `jwt` (verify the caller's token against the provider's JWKS and take the tenant, and optionally the pool, from its claims).
- **Load shedding:** past `CANALIS_MAX_IN_FLIGHT` (default 5,000) requests in progress, `/proxy` and `/jobs` answer `503` + `Retry-After` instead of slowing down until connections time out.
- **Several Canalis nodes** ([#19](https://github.com/rjpruitt16/canalis-rs/issues/19)): the first node to queue a tenant holds their line, and the other nodes forward that tenant's queued requests and status lookups to it.
- **Taking turns** ([#36](https://github.com/rjpruitt16/canalis-rs/issues/36)): while anyone waits in a pool's line, newcomers join the line instead of cutting in, and a user holding a slot past `CANALIS_MAX_HOLD_SECONDS` (10) gives it up on their next request.
- `CANALIS_LISTEN_ADDR`. Canalis listens on `[::]:8080` by default.

### Changed

- **Lower latency:** the instance list used for claims is reused for `CANALIS_MEMBER_CACHE_MS` (250), so a request costs one Valkey round trip instead of four. The p50 Canalis adds went from about 5 ms to about 1.6 ms; see [BENCHMARKS.md](BENCHMARKS.md).
- **Forwarding between Canalis nodes uses HTTP/2** over a small pool of connections per peer (`CANALIS_PEER_CONNECTIONS`, 32), instead of one connection per waiting caller, and the image raises the open-file limit. Both nodes ran out of open files in the second Fly benchmark without this.
- **Line writes are batched:** enqueues that arrive while one is being written share the next disk flush, and a new job's place in line is a lookup instead of a scan.
- **Faster when the fleet is full:** the line drains at `CANALIS_DRAIN_RPS` / `CANALIS_DRAIN_MAX_CONCURRENT` (100/s, 10 at once per tenant) instead of 2/s one at a time, and a tenant's slot is freed as soon as its backlog drains.
- With no allowlist and signing not required, a registration signed by a key Canalis hasn't learned is accepted as unsigned instead of refused.

### Fixed

- **IPv6:** instances on IPv6 networks such as Fly's were recorded as unusable addresses (`fdaa::5:8080`), and Canalis didn't listen on IPv6.
- **Several nodes and L8:** a key learned through the handshake by one Canalis node is now shared through Valkey. Before, the other nodes accepted that node's signed pings as unsigned (logging every ping), or refused them with `CANALIS_REQUIRE_SIGNED_REGISTRATION`.
- **Restarts:** after a Canalis restart, nodes that had already registered got `401` indefinitely. Learned L8 node keys are now saved on the volume.

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
