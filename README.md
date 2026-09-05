# Canalis — a sticky load balancer for pacing, fairness, and agentic burst

**Scale a fleet of Aquifer/ezthrottle-local instances without breaking what makes either of them work.**

One Aquifer (or ezthrottle-local) instance already paces and durably queues requests to a single backend, based on what that backend actually says it can absorb. Canalis does the equivalent job one layer up: given a fleet of many such instances, it decides which tenant gets which instance, keeps that assignment sticky for as long as the instance lives, and durably queues a tenant's work when the fleet is genuinely out of free capacity — instead of dropping it, retrying blindly, or spreading one tenant's traffic across several instances in a way that blinds every one of them to that tenant's real burst pattern.

It's not a generic reverse proxy with rate-limiting bolted on. It's built specifically to sit in front of a fleet of instances that already speak a pacing dialect (`X-Aqueduct-*`), and to keep that dialect meaningful once there's more than one instance to route across.

---

## Why not Envoy or nginx?

Traditional load balancers route structurally — round-robin, least-connections, consistent hashing — and treat every request as interchangeable. That's the wrong shape for this problem in two specific ways.

First, splitting one tenant's traffic round-robin across several instances breaks the thing each instance is trying to do. Aquifer's own `account_queue.go` learns a tenant's real burst pattern by watching *all* of that tenant's traffic and adjusting its pacing live from what the backend reports back. Spread across three instances round-robin, each one sees a third of the picture and none of them ever see the tenant's true rate — the exact pacing behavior Aquifer was built to provide quietly stops working, for reasons that would be genuinely confusing to debug. Sticky assignment isn't a preference here, it's a correctness requirement.

Second, health-check-driven failover tends toward flapping: a backend gets briefly overwhelmed, gets marked unhealthy, traffic reroutes away, the backend recovers, gets marked healthy again, traffic floods back in and overwhelms it a second time. That cycle is a well-known failure mode in generic proxies, and it's the opposite of what pacing is supposed to buy you. What Canalis needs instead — durable, per-tenant memory of who's assigned where, a queue that survives a crash, and dispatch that adjusts to an instance's live self-reported capacity — isn't something you configure into Envoy or nginx. It's closer to a small, purpose-built control plane than a proxy config, and by the time you've bolted the equivalent on via Lua or WASM filters, you've built Canalis anyway, just less legibly.

## How this scales Aquifer and ezthrottle-local

**Sticky assignment.** One `user_id`, one instance, for as long as that instance is alive. Resolved once via `SET ... NX` against Valkey and never re-decided afterward, so a tenant's whole request history lands on one instance's own account-queue instead of being fragmented across several.

**A community pool with a durable fallback, not a hard ceiling.** Free instances sit in a shared pool; when it's genuinely empty, a tenant's job durably enqueues per-tenant in Valkey instead of being dropped or held in an in-memory retry loop that dies with the process. This is deliberate, not a stopgap: fleet size scaling with *machines*, not with *tenant count*, is the actual bet — most tenants don't need dedicated infrastructure to get real service, they need the gap between "capacity exists" and "capacity is running right now" absorbed gracefully. Millions of active users doesn't mean millions of instances need to exist simultaneously.

**Concurrent, self-pacing drain.** When an instance frees up, Canalis doesn't just pop one queued job and wait for the next heartbeat — it drains that tenant's *entire* backlog, dispatching concurrently and adjusting its own rate and concurrency live from whatever the instance reports about its own current tolerance (`X-Canalis-Max-Concurrent`/`X-Canalis-Rps`, falling back to Aquifer's own existing `X-Aqueduct-*`/`X-Aquifer-*` names). It's the same signal Aquifer's own queue already listens for from a real backend, propagated one layer further out — Canalis starts conservative and lets the instance tell it how much faster it can safely go.

**Reserved instances for guaranteed capacity** (designed, not yet shipped — see `DESIGN.md`). Alongside the shared pool, an instance can register as dedicated to one specific `user_id` and never enter the community pool at all — for tenants who need guaranteed, unshared capacity rather than a fair share of a pool. Canalis is assumed to sit behind an API gateway that's already authenticated the caller, so this doesn't need its own credential system on top — the gateway is the trust boundary.

## Pacing and fairness the way lower layers of the internet already do it

HTTP APIs mostly don't have anything like what TCP has always had: a sender that starts cautious, creeps its rate up while things go well, and pulls back hard the moment the receiving end signals trouble — plus enough per-flow isolation that one aggressive sender doesn't starve everyone else on the same link. AIMD and per-flow fairness aren't a metaphor bolted on after the fact here — Aquifer's own account-queue already implements a version of it (`rps = min(rps * 1.05, ceiling)` creeping up on quiet completions, a `slowStart` mode that begins below the configured ceiling rather than at it, an immediate pull-back the moment a backend's response says to slow down) and calls it by the TCP-adjacent name it already deserves.

Canalis's job is making sure that behavior survives having more than one instance behind it: sticky assignment keeps a tenant's signal legible to the instance handling it, the durable per-tenant queue keeps one tenant's burst from being able to starve another's turn at a freed instance, and the concurrent drain loop applies the exact same creep-up/pull-back logic one layer further out — instance capacity, not just backend capacity. A traditional load balancer floods whatever's behind it as fast as its routing algorithm allows; the aim here is to end that, and the flapping it causes, the same way TCP congestion control ended it at the packet level decades ago.

## Status

Registration, sticky assignment (community pool), request forwarding (buffered and live-streamed), the durable Valkey-backed account-queue, and concurrent header-driven pacing on drain are built and tested end-to-end — see `DESIGN.md` for the full technical detail, verification notes, and open questions. Reserved-instance support and a `GET /jobs/<idempotent_key>` polling endpoint are designed but not yet implemented. This is a hands-on Rust learning project as much as an infrastructure one — implementation is deliberate and incremental, not a race to a 1.0.

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

`/register` takes the port an instance is listening on (its address is derived from the caller's own connecting IP plus that port) and re-sends on every heartbeat — a registration expires on its own if pings stop arriving, rather than needing an explicit deregistration call.

`/proxy` and `/jobs` mirror Aquifer's own request body shape exactly, so a caller already speaking Aquifer's API can point at Canalis with no translation layer.
