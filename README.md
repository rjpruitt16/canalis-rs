# Canalis — a sticky load balancer for pacing, fairness, and agentic burst

**Scale a fleet of Aquifer/ezthrottle-local instances without breaking what makes either of them work.**

One Aquifer (or ezthrottle-local) instance paces and durably queues requests to a single backend based on what that backend says it can absorb. Canalis does the same job one layer up: given a fleet of such instances, it decides which tenant gets which instance, keeps that assignment sticky for the instance's lifetime, and durably queues a tenant's work when the fleet is genuinely out of capacity — instead of dropping it, retrying blindly, or splitting one tenant's traffic across instances in a way that blinds all of them to that tenant's real burst pattern.

It's not a generic reverse proxy with rate-limiting bolted on — it's built to sit in front of instances that already speak a pacing dialect (`X-Aqueduct-*`) and keep that dialect meaningful across more than one instance.

Sibling projects, either of which can be a fleet member: [Aquifer](https://github.com/rjpruitt16/aquifer) (Go) and [ezthrottle-local](https://github.com/rjpruitt16/ezthrottle-local) (Elixir).

---

## Why not Envoy or nginx?

Every other layer of infrastructure paces flow already. Highways meter on-ramps so traffic doesn't seize up at rush hour. Phone networks have had busy signals and call admission control since long before packet switching existed. Datacenters pace power draw so a spike doesn't trip a breaker. TCP itself paces packets — starts cautious, speeds up while things go well, backs off hard the instant the other end signals trouble. At layer 7, none of that happened by default: most APIs just get flooded with requests until one side falls over, and then everyone argues over whose fault it was. Aquifer already brought that kind of pacing to layer 7 for one backend — see its [Dynamic Pacing](https://github.com/rjpruitt16/aquifer#dynamic-pacing) docs for how the ceiling, backoff, and recovery actually work. Canalis's job is making that survive having more than one instance behind it.

Traditional load balancers don't get you there because they route structurally — round-robin, least-connections, consistent hashing — and treat every request as interchangeable. That's the wrong shape here in two specific ways.

Splitting one tenant's traffic round-robin across several instances breaks the thing each instance does: Aquifer's `account_queue.go` learns a tenant's real burst pattern by watching *all* of that tenant's traffic and adjusting pacing live from backend feedback. Spread across three instances, each sees a third of the picture and none sees the tenant's true rate — pacing quietly stops working, in a way that's genuinely confusing to debug. Sticky assignment isn't a preference here, it's a correctness requirement.

Health-check-driven failover also tends to flap: a backend gets overwhelmed, is marked unhealthy, traffic reroutes away, it recovers, is marked healthy, traffic floods back and overwhelms it again — a well-known generic-proxy failure mode, and the opposite of what pacing is supposed to buy you. Envoy and nginx have no configuration knob for what fixes this: durable per-tenant assignment memory, a crash-surviving queue, dispatch that adjusts to an instance's live self-reported capacity. Bolt the equivalent on via Lua or WASM filters and you've built Canalis anyway, just less legibly.

## Dynamic pacing, one layer above Aquifer's own

This is the core mechanism the rest of the design exists to support. Aquifer's own pacing adapts a single instance's dispatch rate to what its backend can absorb; Canalis adapts its own dispatch rate to what a *specific fleet instance* can absorb, using the same idea one layer further out.

When an instance frees up, Canalis doesn't just release one queued job and wait for the next heartbeat — it drains that tenant's entire backlog, dispatched concurrently via a `tokio::task::JoinSet`, starting conservative (1 concurrent, 2rps, Aquifer's own defaults) and adjusting live from what the instance reports on each response: `X-Canalis-Max-Concurrent` / `X-Canalis-Rps`, falling back to Aquifer's own `X-Aqueduct-*` / `X-Aquifer-*` names if that's the simpler thing for an instance to emit. No separate coordination needed to make this work across multiple Canalis processes either — the backlog itself lives in Valkey, not in any one process's memory, so several processes each reacting to their own registration events already drain safely in parallel.

## How this scales Aquifer and ezthrottle-local

**Sticky assignment.** One `user_id`, one instance, for the instance's lifetime — resolved once via `SET ... NX` against Valkey and never re-decided, so a tenant's whole history lands on one instance's own account-queue instead of fragmenting.

**A shared pool with a durable fallback.** Free instances sit in a community pool; when it's genuinely empty, a tenant's job durably enqueues per-tenant in Valkey rather than being dropped or held in an in-memory retry loop that dies with the process. The bet: fleet size scales with *machines*, and most tenants just need that gap between "capacity exists" and "capacity is running right now" absorbed gracefully, rather than dedicated infrastructure of their own. Millions of active users doesn't mean millions of simultaneous instances.

**Reserved instances for guaranteed capacity** (designed, not yet shipped — see `DESIGN.md`). An instance can register as dedicated to one `user_id` and never enter the community pool, for tenants needing guaranteed unshared capacity rather than a fair share of a pool. Canalis assumes it sits behind an API gateway that's already authenticated the caller, so it needs no credential system of its own — the gateway is the trust boundary.

## Quick start

Needs a running Valkey (or Redis) instance and at least one Aquifer or ezthrottle-local instance to route to.

```bash
docker build -t canalis-rs .
docker run -p 8080:8080 -e CANALIS_VALKEY_URL=redis://your-valkey-host:6379 canalis-rs
```

```bash
curl localhost:8080/health
curl -X POST localhost:8080/register -d '{"port":"9090","reported_at":"2026-09-04T12:00:00Z"}'
curl -X POST localhost:8080/jobs -d '{"user_id":"acme","idempotent_key":"k1","url":"https://example.com","method":"GET"}'
```

`/register` takes the port an instance listens on (its address is derived from the caller's connecting IP plus that port) and re-sends on every heartbeat; a registration expires on its own if pings stop, with no explicit deregistration call.

`/proxy` and `/jobs` mirror Aquifer's own request body shape exactly, so a caller already speaking Aquifer's API can point at Canalis with no translation layer.
