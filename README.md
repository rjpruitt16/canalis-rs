# Canalis — a sticky load balancer for pacing, fairness, and agentic burst

**Scale a fleet of Aquifer/Spillway instances without breaking what makes either of them work.**

One Aquifer (or Spillway) instance paces and durably queues requests to a single backend based on what that backend says it can absorb. Canalis does the same job one layer up: given a fleet of such instances, it decides which tenant gets which instance, keeps that assignment sticky for the instance's lifetime, and durably queues a tenant's work when the fleet is genuinely out of capacity — instead of dropping it, retrying blindly, or splitting one tenant's traffic across instances in a way that blinds all of them to that tenant's real burst pattern.

Each fleet member is usually protecting something that can't scale on a whim — a GPU, a database, a CI runner — where "more capacity" means provisioning another machine, not flipping a switch. Canalis's account-queue exists because that gap is real but temporary: it holds work while more compute comes online, it doesn't manufacture capacity that isn't coming. If what's behind the fleet has a genuinely fixed ceiling that more machines can never raise, none of this is worth building — you'd just be queuing in front of a wall.

It's not a generic reverse proxy with rate-limiting bolted on — it's built to sit in front of instances that already speak a pacing dialect (`X-Aqueduct-*`) and keep that dialect meaningful across more than one instance.

Sibling projects, either of which can be a fleet member: [Aquifer](https://github.com/rjpruitt16/aquifer) (Go) and [Spillway](https://github.com/rjpruitt16/spillway) (Elixir).

---

## Why not Envoy or nginx?

Every other layer of infrastructure paces flow already. Highways meter on-ramps so traffic doesn't seize up at rush hour. Phone networks have had busy signals and call admission control since long before packet switching existed. Datacenters pace power draw so a spike doesn't trip a breaker. TCP itself paces packets — starts cautious, speeds up while things go well, backs off hard the instant the other end signals trouble. At layer 7, none of that happened by default: most APIs just get flooded with requests until one side falls over, and then everyone argues over whose fault it was. Aquifer already brought that kind of pacing to layer 7 for one backend — see its [Dynamic Pacing](https://github.com/rjpruitt16/aquifer#dynamic-pacing) docs for how the ceiling, backoff, and recovery actually work. Canalis's job is making that survive having more than one instance behind it.

Traditional load balancers don't get you there because they route structurally — round-robin, least-connections, consistent hashing — and treat every request as interchangeable. That's the wrong shape here in two specific ways.

Splitting one tenant's traffic round-robin across several instances breaks the thing each instance does: Aquifer's `account_queue.go` learns a tenant's real burst pattern by watching *all* of that tenant's traffic and adjusting pacing live from backend feedback. Spread across three instances, each sees a third of the picture and none sees the tenant's true rate — pacing quietly stops working, in a way that's genuinely confusing to debug. Sticky assignment isn't a preference here, it's a correctness requirement.

Health-check-driven failover also tends to flap: a backend gets overwhelmed, is marked unhealthy, traffic reroutes away, it recovers, is marked healthy, traffic floods back and overwhelms it again — a well-known generic-proxy failure mode, and the opposite of what pacing is supposed to buy you. Envoy and nginx have no configuration knob for what fixes this: durable per-tenant assignment memory, a crash-surviving queue, dispatch that adjusts to an instance's live self-reported capacity. Bolt the equivalent on via Lua or WASM filters and you've built Canalis anyway, just less legibly.

## Dynamic pacing, one layer above Aquifer's own

This is the core mechanism the rest of the design exists to support. Aquifer's own pacing adapts a single instance's dispatch rate to what its backend can absorb; Canalis adapts its own dispatch rate to what a *specific fleet instance* can absorb, using the same idea one layer further out.

When an instance frees up, Canalis doesn't just release one queued job and wait for the next heartbeat — it drains that tenant's entire backlog, dispatched concurrently via a `tokio::task::JoinSet`, starting conservative (1 concurrent, 2rps, Aquifer's own defaults) and adjusting live from what the instance reports on each response: `X-Canalis-Max-Concurrent` / `X-Canalis-Rps`, falling back to Aquifer's own `X-Aqueduct-*` / `X-Aquifer-*` names if that's the simpler thing for an instance to emit. Each Canalis node drains its own line: on every new registration it receives, and on a periodic sweep, so a line still drains when the freed instance happened to register with a different Canalis node.

## How this scales Aquifer and Spillway

**Sticky assignment, in Aquifer's own table.** One `user_id` stays on one instance, so a tenant's whole history lands on one instance's own account-queue instead of fragmenting. Canalis doesn't keep a table of its own for this: it uses the one Aquifer's Valkey cluster mode already writes (`aqueduct:instance:<id>`, `aqueduct:assignment:<sha256(user_id)>`) and claims through the same Lua script, so Canalis and Aquifer instances coordinating through Valkey agree on where every user lives. Instances take many users each, up to their capacity, and an assignment that sits idle (`CANALIS_ASSIGNMENT_IDLE_SECONDS`) is released, so capacity comes back on its own.

**Fullness decides who gets new users.** Every instance reports how full it is (`X-Aqueduct-Fullness`, 0–100, on each response and in its registration ping). Past `CANALIS_QUEUE_AT_PERCENT` (80 by default) an instance gets no new users; the ones already on it stay.

**A waiting line when everything is full.** When every instance is past that threshold or at capacity, a tenant's job goes into Canalis's line, on the Canalis node's own disk (redb), rather than being dropped or held in an in-memory retry loop that dies with the process. Whoever has waited longest is handed the next free instance. The line is bounded on purpose: past its limits Canalis answers `429` with `Retry-After` instead of growing, because queueing in front of capacity that isn't coming just hides an outage. The bet: fleet size scales with *machines*, and most tenants just need that gap between "capacity exists" and "capacity is running right now" absorbed gracefully, rather than dedicated infrastructure of their own. Millions of active users doesn't mean millions of simultaneous instances.

**Reserved, drain-mode, and WebSocket instances.** An instance registered with `reserved_for` (`AQUIFER_REGISTRY_RESERVED_FOR`) only ever takes that user, who gets it before any shared instance. A drain-mode instance takes one tenant at a time and is handed to the next as soon as it reports `drain_state: unassigned`. A `role: ws` instance isn't given HTTP traffic. Canalis assumes it sits behind an API gateway that's already authenticated the caller, so it needs no credential system of its own — the gateway is the trust boundary.

## Quick start

The quickest way to try it is the compose file, which starts Valkey, Canalis and two Aquifer instances that register themselves:

```bash
docker compose up --build
curl localhost:8080/health      # "instances":{"total":2,...}
curl -X POST localhost:8080/proxy -d '{"user_id":"acme","idempotent_key":"k1","url":"http://canalis:8080/health","method":"GET","webhook_url":"http://canalis:8080/health"}'
curl localhost:8080/metrics
```

To run the published image by itself, you need a Valkey (or Redis) instance and at least one Aquifer or Spillway instance to route to:

```bash
docker run -p 8080:8080 -v $(pwd)/data:/data -e CANALIS_VALKEY_URL=redis://your-valkey-host:6379 ghcr.io/rjpruitt16/canalis-rs:0.1
```

Every setting is in [docs/configuration.md](docs/configuration.md), and changes between versions are in [CHANGELOG.md](CHANGELOG.md).

```bash
curl localhost:8080/health
curl -X POST localhost:8080/register -d '{"port":"9090","reported_at":"2026-09-04T12:00:00Z"}'
curl -X POST localhost:8080/jobs -d '{"user_id":"acme","idempotent_key":"k1","url":"https://example.com","method":"GET","webhook_url":"https://yourapp.com/webhooks/aquifer"}'
```

`/register` takes the port an instance listens on (its address is derived from the caller's connecting IP plus that port) and re-sends on every heartbeat; a registration expires on its own if pings stop, with no explicit deregistration call.

**Shutting down.** On SIGTERM Canalis drains, the way Aquifer does: new `/proxy`, `/jobs`, and WebSocket handshakes get `503` + `Retry-After` + `X-Aqueduct-Node-State: draining` and `GET /ready` turns `503`, so whatever is in front retries another Canalis node; open WebSocket sessions get Aquifer's `server_draining` status and then a `1012` close; callers waiting in line get `canalis_draining` and their jobs stay in the line, on disk, until this node starts again; handoffs already under way finish and nothing more leaves the line.
**Asking for more capacity.** The line is for the gap until more instances are running, not a substitute for them. Once a job has waited `CANALIS_SCALE_AFTER_SECONDS`, Canalis POSTs a `canalis_scale_up` signal to `CANALIS_SCALE_WEBHOOK_URL`, and whatever starts instances decides what to do; Canalis doesn't start infrastructure itself. `GET /health` reports the same numbers for anything that polls: `{"status":"ok","line":{"jobs":42,"oldest_wait_seconds":17},"instances":{"total":3,"accepting":0},"websocket":{"open":12}}`.

**When things fail.** Each of these was tested by killing real processes:

| What fails | What Canalis does |
| --- | --- |
| An instance dies (connection refused) | It takes the instance out of rotation, which releases its users, and sends the request to another instance or into the line. Only failed connections are re-routed, because those requests never reached the instance. A timeout partway through a request is answered with a 502. If the instance is still alive, its next registration ping brings it back. |
| Valkey is unreachable | New HTTP jobs wait in the line, which is on local disk and doesn't need Valkey. The `canalis_queued` reason is `assignment_store_unavailable`, and jobs are handed out once Valkey is back. New WebSockets get a 503 with `Retry-After`. `/health` reports `"status":"degraded","valkey":"down"` and `/metrics` reports `canalis_valkey_up 0`. Recovery needs no restart. |
| Canalis is killed (`kill -9`) | Jobs in line are still there after the restart. A job that was in the middle of a handoff goes back in line too, so delivery is at-least-once and an instance may see the same idempotency key twice. Aquifer and Spillway answer the repeat from what they already have. |

**Metrics.** `GET /metrics` is Prometheus text. It covers:

- every response by route and status (`canalis_http_responses_total`), which includes 429s for a full line, 422s from the schema gate, 401s for refused registrations and draining 503s;
- time to response headers by route (`canalis_http_request_duration_seconds`);
- the line (`canalis_line_jobs`, `canalis_line_oldest_wait_seconds`, `canalis_line_rejected_total{reason}`);
- each instance's state, fullness, active users and capacity (`canalis_instance_*`);
- open and waiting WebSockets, re-routes away from draining instances, scale-up signals sent, and `canalis_valkey_up`.

`/proxy` and `/jobs` mirror Aquifer's own request body shape exactly, so a caller already speaking Aquifer's API can point at Canalis with no translation layer.

Aquifer and Spillway register themselves when `AQUIFER_REGISTRY_URL` / `SPILLWAY_REGISTRY_URL` points at Canalis's `/register`. Canalis speaks the receiving side of [L8](https://rjpruitt16.github.io/l8-protocol/) (`GET /.well-known/l8`, `POST /l8/challenge`), so those pings arrive signed with no change on the node's side; set `CANALIS_L8_TRUSTED_NODE_KEYS` and `CANALIS_REQUIRE_SIGNED_REGISTRATION=true` to accept only the nodes you run.

When no instance is free, the caller's connection becomes a stream from Canalis's line. Canalis's own events are prefixed `canalis_` so they're never confused with the node's queue:

```
event: canalis_queued      data: {"ahead":3,"reason":"no_instance_free","status":"/jobs/k1?user_id=acme"}
event: canalis_position    data: {"ahead":1}
event: canalis_assigned    data: {"job_id":"..."}        (job_id for /jobs callers)
event: queued / completed  ...                           (the node's own stream, /jobs callers)
event: canalis_result      data: {"status":200,"body":"..."}  (a /proxy answer that isn't a stream)
event: canalis_timeout     data: {"status":"/jobs/k1?user_id=acme"}  (still in line; stream closing)
event: canalis_draining    data: {"status":"/jobs/k1?user_id=acme"}  (this Canalis node is restarting; still in line)
```

## Authentication

By default Canalis believes the `user_id` (or `X-Aqueduct-User-ID`) a request sends. That's fine only when nothing untrusted can reach it. Set `CANALIS_AUTH` to one of:

- **`upstream-secret`:** your gateway does the login and sends `X-Canalis-Upstream-Secret: $CANALIS_UPSTREAM_SECRET`. Anything without it gets a 401.
- **`jwt`:** callers send the token their login provider gave them, as `Authorization: Bearer <token>`. Browser WebSockets, which can't set headers, can use `?access_token=<token>` instead.

In `jwt` mode, Canalis checks the token's signature against the provider's published keys, and checks its expiry, issuer and (optionally) audience. It then takes the tenant from a claim. That tenant replaces whatever user id the request named, for `/proxy`, `/jobs`, `GET /jobs/{key}` and WebSocket routing, so nobody can act as someone else.

Keys are fetched once and cached, so there's no call to the provider per request. When the provider rotates keys, Canalis picks the new ones up without a restart. Any provider that issues JWTs works: Clerk, WorkOS, Auth0, Cognito, Supabase, Firebase. For example:

```bash
CANALIS_AUTH=jwt
CANALIS_JWKS_URL=https://<your-provider>/.well-known/jwks.json
CANALIS_JWT_ISSUER=https://<your-provider>
CANALIS_JWT_TENANT_CLAIM=org_id      # route and queue per organization; default: sub
```

How Canalis answers:

- A missing or bad token gets a `401` with `WWW-Authenticate: Bearer`. HMAC-signed tokens are always refused.
- If the provider's keys can't be fetched and none are cached, it answers `503` with `Retry-After`.
- On WebSockets, the token isn't passed on to the instance. The instance sees the verified tenant as `X-Aqueduct-User-ID`.
- `canalis_auth_rejected_total{reason}` counts refusals.

All the settings are in [docs/configuration.md](docs/configuration.md#who-a-request-is-for).

## Running several Canalis nodes

Each Canalis keeps its line on its own disk. To keep each tenant in one line, the first node that has to queue a tenant claims them in Valkey. Any other node that would queue that tenant forwards the request to the owner instead, and `GET /jobs/{key}` follows the claim too, so a client can hit any node.

- **Cost:** ownership is checked only when a request would be queued, so a request that gets an instance straight away costs nothing extra.
- **While the tenant waits:** the owner renews its claim while the tenant has jobs waiting, and lets go when they're done.
- **If the owner stops heartbeating:** the next node to queue that tenant takes over. Jobs still on the lost node's disk wait for it to come back, and are lost if it never does.
- **If the owner can't be reached:** the job is queued on the node that got it, never dropped.

Node keys learned through the L8 handshake are shared through Valkey too, so a node that shook hands with one Canalis is trusted by all of them. That holds even with `CANALIS_REQUIRE_SIGNED_REGISTRATION`.

Each node needs an address the others can reach. On Fly that's automatic, from `FLY_PRIVATE_IP`. Elsewhere, set `CANALIS_ADVERTISE_URL` (for example `http://10.0.0.7:8080`). A node without one never claims a tenant, which is how a single Canalis runs. The settings are in [docs/configuration.md](docs/configuration.md#several-canalis-nodes).

## Pools and WebSockets

**Pools.** Instances can be split into pools, for example GPU and CPU instances, regions, or a set of instances dedicated to one customer. A node joins pools by listing them at registration: `AQUIFER_REGISTRY_POOLS=gpu,eu` or `SPILLWAY_REGISTRY_POOLS`. A node that lists none is in the `default` pool. Every ping replaces the list, so a node joins or leaves a pool with its next ping.

A request picks its pool with `X-Aqueduct-Pool: gpu` (a `pool` query parameter also works for WebSockets and `GET /jobs/{key}`). Without one, it goes to the default pool. Requests only reach instances in their pool.

Pools share one set of `aqueduct:` keys rather than a prefix each. So an instance in two pools has one capacity, not one per pool. Each pool has its own place in the line, so a pool with no free instances doesn't hold up the others. Queue positions count only jobs waiting in the same pool. A user's assignment is per pool, and the default pool keeps the plain per-user assignment that Aquifer's cluster mode uses.

**WebSockets.** `GET /websocket` is routed to an instance and piped, frames untouched, both ways. Aquifer's and Spillway's own `/websocket` (an `aqueduct.v1` session over a Valkey transcript) does the ordering, replay, and reconnecting, and since the transcript is in shared Valkey, a client that reconnects through Canalis to a different instance replays what it missed. Canalis picks an instance with `role: ws` (or no role) for the gateway's `X-Aqueduct-User-ID`, else the `session_id`; an instance that can't do WebSockets (an Aquifer without Valkey answers 404) should register with `role: http`. No instance with room means the session waits in Canalis's WebSocket waiting room, oldest first, hearing `{"type":"status","state":"canalis_waiting","position":N}` as the line moves and `canalis_assigned` when an instance takes it (past `CANALIS_WS_MAX_WAITING` the handshake gets `503` + `Retry-After`; past `CANALIS_WS_WAIT_TIMEOUT_SECONDS` the session closes with `1013`); an instance's own refusal (a `409` replay gap, say) is relayed; a draining one is skipped. Aquifer's and Spillway's full WebSocket contract passes through Canalis in CI.

`GET /jobs/{idempotent_key}?user_id=...` answers with the finished result, `202 {"status":"queued","ahead":N}`, or `404`. The line is per Canalis node, so ask the node that queued it.

| Env var | Default | Description |
|---|---|---|
| `CANALIS_VALKEY_URL` | `redis://127.0.0.1:6379` | Assignment state and instance registry |
| `CANALIS_LINE_PATH` | `canalis-line.redb` | The waiting line's file (`/data/canalis-line.redb` in the Docker image) |
| `CANALIS_LINE_MAX_JOBS` | `10000` | Jobs in line before new ones get `429` |
| `CANALIS_LINE_MAX_PER_TENANT` | `1000` | One tenant's share of the line, so a single burst can't fill it |
| `CANALIS_LINE_MAX_BYTES` | `268435456` (256MB) | Total size of queued job bodies |
| `CANALIS_LINE_RETRY_AFTER_SECONDS` | `5` | `Retry-After` on a full-line `429` |
| `CANALIS_RESULT_TTL_SECONDS` | `300` | How long a drained job's result stays available |
| `CANALIS_LINE_SWEEP_SECONDS` | `5` | How often the line is checked against free instances (also runs at startup, which is crash recovery) |
| `CANALIS_POOL_WAIT_TIMEOUT_MS` | `30000` | How long a caller's stream stays open in line before `canalis_timeout` |
| `CANALIS_POSITION_INTERVAL_MS` | `1000` | How often waiting callers get `canalis_position` updates |
| `CANALIS_QUEUE_AT_PERCENT` | `80` | At or above this fullness, an instance gets no new users; when every instance is there, new users wait in line |
| `CANALIS_DEFAULT_USER_CAPACITY` | `100` | Users an instance takes when its record doesn't say (drain-mode instances take 1) |
| `CANALIS_ASSIGNMENT_IDLE_SECONDS` | `300` | How long an idle user keeps their instance before it's released |
| `CANALIS_AQUEDUCT_PREFIX` | `aqueduct:` | Valkey key prefix for the shared assignment table; must match `AQUIFER_CLUSTER_VALKEY_PREFIX` |
| `CANALIS_L8_SCHEMA_VALIDATION` | `false` | Check each job's body against the JSON Schema its upstream publishes for that route ([L8 v0.2 `request_schemas`](https://rjpruitt16.github.io/l8-protocol/)) and answer `422` on a mismatch, before the job reaches the line or any instance. External `$ref`s are refused. Off by default because it fetches `/.well-known/l8` from each job's upstream |
| `CANALIS_L8_TRUSTED_NODE_KEYS` | *(none)* | Comma-separated base64 Ed25519 public keys of the nodes allowed to register (each Aquifer logs its own as `[L8] public key: ...`). When set, only these keys can complete the L8 handshake or sign a registration |
| `CANALIS_REQUIRE_SIGNED_REGISTRATION` | `false` | Refuse unsigned registration pings. A ping that is signed but doesn't verify is refused either way |
| `L8_PRIVATE_KEY` / `CANALIS_L8_KEY_PATH` | *(none)* / `.l8-key` | Canalis's own L8 key: base64 in the env var (Go's 64-byte format or a 32-byte seed), else a key file generated on first start (`/data/.l8-key` in the image) |
| `CANALIS_WS_MAX_CONNECTIONS` | `10000` | WebSocket sessions open through this Canalis node; past it, new handshakes get `503` |
| `CANALIS_WS_MAX_WAITING` | `1000` | WebSocket sessions that can wait for an instance with room; past it, new handshakes get `503` |
| `CANALIS_WS_WAIT_TIMEOUT_SECONDS` | `60` | How long a WebSocket session waits for an instance before closing with `1013` (try again later) |
| `CANALIS_SHUTDOWN_TIMEOUT_SECONDS` | `30` | On SIGTERM, how long Canalis drains before exiting regardless |
| `CANALIS_SHUTDOWN_QUIESCE_MS` | `500` | On SIGTERM, how long it keeps answering (with 503s) before closing anything, so load balancers notice |
| `CANALIS_WS_DRAIN_GRACE_SECONDS` | `5` | On SIGTERM, time an open WebSocket session gets between `server_draining` and its 1012 close |
| `CANALIS_SCALE_WEBHOOK_URL` | *(none)* | Where to POST `{"event":"canalis_scale_up", "line_jobs", "oldest_wait_seconds", "instances":{"total","accepting"}}` when the line backs up, for whatever adds instances (a script against your cloud's API, a controller) |
| `CANALIS_SCALE_AFTER_SECONDS` | `10` | How long a job must have waited in line before the scale-up signal goes out |
| `CANALIS_SCALE_WEBHOOK_INTERVAL_SECONDS` | `60` | At most one scale-up signal per this many seconds, per Canalis node |
| `CANALIS_REGISTRATION_INTERVAL_SECONDS` | `15` | Instances' ping interval; a record expires after three missed pings |
