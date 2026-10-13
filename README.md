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

**Sticky assignment.** One `user_id`, one instance, for the instance's lifetime — resolved once via `SET ... NX` against Valkey and never re-decided, so a tenant's whole history lands on one instance's own account-queue instead of fragmenting.

**A shared pool with a waiting line.** Free instances sit in a community pool; when it's genuinely empty, a tenant's job goes into Canalis's line, on the Canalis node's own disk (redb), rather than being dropped or held in an in-memory retry loop that dies with the process. Whoever has waited longest is handed the next free instance. The line is bounded on purpose: past its limits Canalis answers `429` with `Retry-After` instead of growing, because queueing in front of capacity that isn't coming just hides an outage. The bet: fleet size scales with *machines*, and most tenants just need that gap between "capacity exists" and "capacity is running right now" absorbed gracefully, rather than dedicated infrastructure of their own. Millions of active users doesn't mean millions of simultaneous instances.

**Reserved instances for guaranteed capacity** (designed, not yet shipped — see `DESIGN.md`). An instance can register as dedicated to one `user_id` and never enter the community pool, for tenants needing guaranteed unshared capacity rather than a fair share of a pool. Canalis assumes it sits behind an API gateway that's already authenticated the caller, so it needs no credential system of its own — the gateway is the trust boundary.

## Quick start

Needs a running Valkey (or Redis) instance and at least one Aquifer or Spillway instance to route to.

```bash
docker build -t canalis-rs .
docker run -p 8080:8080 -v $(pwd)/data:/data -e CANALIS_VALKEY_URL=redis://your-valkey-host:6379 canalis-rs
```

```bash
curl localhost:8080/health
curl -X POST localhost:8080/register -d '{"port":"9090","reported_at":"2026-09-04T12:00:00Z"}'
curl -X POST localhost:8080/jobs -d '{"user_id":"acme","idempotent_key":"k1","url":"https://example.com","method":"GET"}'
```

`/register` takes the port an instance listens on (its address is derived from the caller's connecting IP plus that port) and re-sends on every heartbeat; a registration expires on its own if pings stop, with no explicit deregistration call.

`/proxy` and `/jobs` mirror Aquifer's own request body shape exactly, so a caller already speaking Aquifer's API can point at Canalis with no translation layer.

Aquifer and Spillway register themselves when `AQUIFER_REGISTRY_URL` / `SPILLWAY_REGISTRY_URL` points at Canalis's `/register`.

When no instance is free, the caller's connection becomes a stream from Canalis's line. Canalis's own events are prefixed `canalis_` so they're never confused with the node's queue:

```
event: canalis_queued      data: {"ahead":3,"reason":"no_instance_free","status":"/jobs/k1?user_id=acme"}
event: canalis_position    data: {"ahead":1}
event: canalis_assigned    data: {"job_id":"..."}        (job_id for /jobs callers)
event: queued / completed  ...                           (the node's own stream, /jobs callers)
event: canalis_result      data: {"status":200,"body":"..."}  (a /proxy answer that isn't a stream)
event: canalis_timeout     data: {"status":"/jobs/k1?user_id=acme"}  (still in line; stream closing)
```

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
