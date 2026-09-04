# Canalis — design doc

Status: registration and assignment (community pool only) are built and
verified end-to-end against a real Valkey. `POST /proxy` and `POST
/jobs` exist as real public endpoints, matching Aquifer's own request
shape exactly, but currently do only the assignment step — no
forwarding to the assigned instance yet, no SSE relay. Pool exhaustion
returns a TODO stub (501), not the waiting room. The reconciliation
sweep, Reserved-pool overrides, the account-queue port, and idempotency
storage are all still design only.

**Real deviation from an earlier plan, found by actually running this,
not decided in the abstract:** the free pool (`canalis:pool:free`) is a
Valkey **Set**, not the List originally planned — registration pings
every ~15s, and a plain List would let an already-in-the-pool,
still-unassigned instance get pushed again on every heartbeat,
creating duplicate entries that could then be claimed twice (the exact
double-assignment bug this whole design exists to prevent). `SADD` is a
no-op on an already-present member, closing that for free. Cost: gave
up FIFO fairness (first-free-wins is now closer to random-free-wins) —
not something this slice needed, per the original scoping, but worth
knowing if fairness becomes a real requirement later.

Assignment itself is race-safe against concurrent requests for the
*same* tenant via `SET ... NX` (see Assignment section below) — not a
Lua script, which was the original plan; `NX` alone turned out to be
sufficient and simpler.

## What it is

Canalis is a control-plane + gateway for the edge/Fly.io side of the Aqueduct
model (Aquifer, and by extension ezthrottle-local). It sits in front of a
fleet of Aquifer instances and does two jobs: assign each tenant to an
instance (sticky, so a given user always lands on the same one), and forward
that tenant's requests to the assigned instance, adapting how it forwards
based on that instance's real-time health.

Written in Rust (Tokio + Tower), backed by Valkey. No BEAM/Elixir version —
Canalis-RS alone is the whole story for this project. One reason for Rust
specifically: some potential adopters won't take a dependency on a BEAM
runtime at all, regardless of how good ezthrottle-local is on its own merits.
A Rust control plane in front of the fleet sidesteps that objection for
anyone who'd otherwise never look past the runtime.

## Why it exists

Aquifer and ezthrottle-local each handle a single instance's local pacing,
queuing, and cross-region failover well. Neither handles "which instance
should this tenant's traffic go to in the first place" — that's a fleet-level
question, not a single-instance question, and answering it requires shared
state (who's assigned where, who's healthy, who's reserved) that doesn't
belong inside either project.

Canalis also has a real chance to close a gap both projects already document
honestly: cross-region redirect's idempotency check is per-instance
(Mnesia/local store), so two regions can independently start a redirect tour
for the same idempotent key and both durably queue it — an accepted
at-least-once, not exactly-once, tradeoff. A real cross-instance idempotency
store (Canalis's Valkey/DB layer) is the actual fix for that, not a further
redesign of the redirect logic itself.

## Architecture

```
client -> Canalis -> assigned Aquifer instance
              |
            Valkey (assignment state, SD registry, idempotency keys)
```

- **Clients call Canalis, not Aquifer directly.** Canalis is the entry
  point. No auth at this layer — that's the caller's/upstream's problem,
  same boundary Aquifer itself already draws.
- **Aquifer instances know nothing about Canalis or Valkey as concepts.**
  Valkey is the sole source of truth for who's assigned to what — Aquifer
  never reads it, never queries it, doesn't need to know an assignment
  exists at all. Its role is purely: drain when told to, report back when
  done. That report is Aquifer's existing drain-mode webhook, pointed at
  Canalis via an environment variable — a generic webhook target as far as
  Aquifer is concerned, no new Aquifer feature, no awareness that "Canalis"
  is the thing on the other end. This keeps Aquifer fully decoupled and
  deployable on its own, with or without Canalis in front of it.
- **Canalis needs zero changes to Aquifer to work.** It's purely a client
  of Aquifer's already-public, already-documented API surface —
  `POST /proxy` and `POST /jobs`. Nothing new to build or trust on the
  Aquifer side.

## Assignment

**Built (community pool only).** Canalis assigns each tenant (user_id) to
a specific Aquifer instance and keeps that assignment sticky — the client
never sees or chooses the instance, so it can't manipulate placement.
`canalis:assignment:<user_id>` (a plain string key, no TTL) holds the
mapping. `canalis:pool:free` (a Set — see the deviation noted at the top
of this doc) holds unclaimed instances; `canalis:assigned` (a Set) holds
claimed ones, checked by the registration handler so a repeatedly-
heartbeating already-assigned instance never leaks back into the free
pool. Race-safety against two concurrent requests for the *same* tenant
is `SET ... NX`: whoever's write loses returns their needlessly-claimed
instance to the pool and reads back whoever actually won, rather than
silently overwriting. Pool exhaustion currently returns a 501 TODO stub —
the waiting room isn't built. Verified end-to-end: two real instances
registered, sticky repeat-assignment confirmed, the second tenant getting
the *other* instance confirmed, exhaustion on a third tenant confirmed,
and the re-registration-doesn't-leak fix confirmed directly against raw
Valkey state, not just the HTTP response.

Two pools, though only Community is built:

- **Reserved instances** — pre-committed capacity, a tenant can be pinned
  to specific machine(s) via an API that registers an instance against a
  user ID permanently; these are excluded from community-pool assignment
  entirely. A registration can be overridden — e.g. to roll out an update:
  register the new machine against the same user ID, new requests start
  routing to it, the old machine keeps serving until it's finished draining,
  then gets decommissioned. Open question: during that overlap window, is
  the old machine still a valid routing target for in-flight work (Valkey
  holds both, checked in a defined order) or does cutover happen
  immediately and the old machine only finishes what it already has
  in-flight without receiving anything new? Needs a real answer before
  this is buildable, not just "roughly blue-green."
- **Community pool** — the shared fleet; Canalis assigns tenants across it
  and can ask the fleet to scale up as load grows.

Community-pool assignment is one-to-one: an instance is checked out to a
single tenant at a time, not shared. Pool capacity is therefore just the
instance count — N instances means N tenants can be actively assigned at
once, no more. That's a deliberate ceiling, not an incidental limit, and
it's what makes pool exhaustion (below) a real, expected case rather than
an edge case.

Valkey holds the assignment table and doubles as the service-discovery
registry (instances register/heartbeat into it). A single region's worth of
assignment lookups is a simple KV read/write — should comfortably handle
very high throughput before needing to partition. Partition Valkey and add
more Aquifer instances as the story that scales past one region/shard.

## Request handling and adaptive routing

Normal case: Canalis forwards the request to the assigned instance's
`POST /proxy` — direct-attempt-then-local-queue-fallback, Aquifer's own
existing behavior, unchanged.

Adaptive case: if the assigned instance starts returning 429 frequently for
a tenant, Canalis stops calling `/proxy` for that tenant and starts calling
`POST /jobs` directly instead — skipping the direct-attempt phase entirely
and going straight to durable queuing. No reassignment, no instance
termination, no scaling event required for this reaction alone — it's a
per-tenant routing-mode switch, reusing Aquifer's own two API shapes instead
of inventing new instance-management machinery. Queuing isn't always the
right call for every workload — this should be adaptive per situation, not
a fixed rule once 429s start.

## Pool exhaustion — waiting queue

Different scenario from the adaptive case above, and not redundant with it:
the adaptive switch handles a tenant who's *already assigned* to an instance
that's struggling. This handles a *new* tenant arriving when every instance
in the community pool is already checked out to someone else and none are
free — a full waiting room, not a noisy occupied seat.

When that happens, Canalis holds the incoming request in its own durable,
Valkey-backed queue rather than rejecting it, and promotes it to a real
assignment as soon as an instance frees up or a scale-up brings a new one
online. Every instance stays reachable and healthy in this case — there's
just nowhere left to put a new tenant, which is exactly why this needs to be
a durable store and not an in-memory retry loop: losing this queue means
losing someone's place in line and the request itself, so it needs the same
persistence bar as real undelivered work (Valkey AOF, not just an in-memory
cache), not the lighter bar the assignment table and idempotency cache can
get away with.

## Pacing philosophy: local truth, no aggregate state

Deliberate, not deferred: Canalis does not track aggregate queue state
across the fleet, and pacing decisions are per-queue-local (each queue's
own AIMD — additive increase, multiplicative decrease on its own ORCA/pace
signal), not fleet-synchronized. This is a CAP-theorem-grounded choice, not
a gap: per-region local truth is the standard answer for this shape of
problem — a region's gateway/Valkey pair is consistent and available for
what actually needs real-time correctness (pacing, assignment), and only
non-critical concerns (billing, etc.) would ever need async cross-region
reconciliation. One big global gateway trying to stay perfectly synced on
exact aggregate flow isn't the goal; being able to scale a queue's pace
down by an order of magnitude quickly when a signal says to is. Aquifer's
existing per-queue pacing granularity (1 req/1s, 1/2s, 1/4s, geometric from
there) already expresses the decelerate-quickly half of this — nothing new
needed there, just naming what already exists.

The accelerate-slowly half is now also a real, shipped feature in both
Aquifer and ezthrottle-local (as of this session): `X-Aqueduct-Slow-Start`
starts a brand-new queue at a low floor rate instead of its full configured
rate, ramping up via the same gradual-recovery mechanism that already
brings a throttled queue back up. Canalis's own ported account-queue
(waiting-room dispatch) should inherit the same behavior for consistency —
a newly-promoted waiting-room entry getting handed to a freshly-assigned
instance is exactly the kind of "new queue coming online" moment slow
start exists for.

## Availability

Multiple Canalis instances, Valkey partitioned/clustered — no single node's
crash takes down assignment or routing. Canalis being fully unreachable
doesn't touch already-flowing traffic to already-assigned instances (nothing
in Aquifer depends on Canalis staying up); it only means no new assignments
or scale-up decisions happen until it recovers. Matches Aquifer's own
degrade-gracefully-without-a-hot-path-dependency principle instead of
contradicting it.

## Idempotency storage

Canalis, not each Aquifer instance, writes idempotency keys and results —
Valkey short-TTL (~1 day) as the fast path, a separate scalable DB per
region for longer-lived storage. Because Canalis is the mandatory entry
point for every request, its idempotency check is naturally authoritative
rather than a secondary reconciliation layer sitting next to each
instance's own local check — this is the actual mechanism that closes the
at-least-once gap described above, not just a store that happens to also
hold idempotency data.

## Account queue inside Canalis

Waiting-queue entries need the same per-tenant isolation and ordering
Aquifer's own account-queue already provides — otherwise one tenant's
backlog could starve another's turn at a freed instance. This needs a real
port, not a reinvention: the underlying pattern (one isolated, ordered
queue per key) maps directly onto Tokio's task-per-key idiom (a spawned
task holding its own `mpsc` channel per account), which is architecturally
the same shape as a BEAM process per account — different runtime, same
actor-per-key structure, so this should carry over cleanly rather than
needing to be redesigned from scratch.

## Testing

- **Aqueduct Runner** (the existing cross-repo contract-test framework
  already used to test Aquifer against ezthrottle-local in CI) should grow
  Canalis contract tests too, so a change on either side that breaks the
  drain/report-back/reassignment contract gets caught the same way a
  break between Aquifer and ezthrottle-local already does.
- **Dagger**, locally to start (not necessarily wired into CI yet): spin up
  Valkey (official Docker image) alongside Canalis-RS and an Aquifer
  instance, and exercise the real drain → report-back → reassignment cycle
  end to end, plus instance restart behavior — the same kind of
  container-based integration coverage this project already leans on for
  cross-repo contract tests, applied to Canalis specifically.

## Suggested Rust crates

Recommendations to evaluate, not settled decisions — this is a hands-on,
learn-Rust project, so these are starting points for research, not a stack
picked on your behalf:

- **Tokio** — async runtime, already decided.
- **Axum** over raw Tower for the HTTP layer. It's built directly on Tower
  and Hyper, so all of Tower's middleware composability (rate limiting,
  tracing, timeouts as stackable layers) is still there, but routing and
  request extraction don't have to be hand-rolled from scratch.
- **`redis` crate** for Valkey (protocol-compatible). Has an async
  (tokio) feature and a built-in multiplexed connection manager with
  automatic reconnection, likely enough on its own without a separate
  pooling crate.
- **`serde` / `serde_json`** for the wire format.
- **`tracing` / `tracing-subscriber`** for structured logs, matching the
  observability bar Aquifer's own redirect orchestration already set.

## Explicitly out of scope (for now)

- Canalis-BEAM. Decided against — Canalis-RS alone is a strong enough
  story on its own; a second implementation isn't worth the maintenance
  burden on top of Aquifer + ezthrottle-local.
- Any code. This doc exists so the design doesn't have to be
  reconstructed from memory later — implementation is a separate,
  future effort, and deliberately a hands-on one (this project is partly
  about building real Rust experience, not delegating it).

## Open questions

- During a reserved-instance override (rolling update), is the old
  machine still a valid routing target for in-flight work during the
  overlap window, or does cutover happen immediately? See Assignment
  section above.
- Exact selection algorithm for which free instance a promoted
  waiting-queue entry gets (first-free-wins? something more deliberate,
  like Aquifer's own rendezvous pick for region candidates?).
- How "ask the fleet to scale up" actually triggers infrastructure —
  Fly Machines API directly, or some intermediate layer.
- Whether the per-region idempotency DB supersedes each Aquifer
  instance's own local idempotency check for redirect-involved requests,
  or only supplements it as a secondary check.
- What "a tenant" maps to concretely (user_id? account? something Aquifer
  already models, like the existing per-account queue isolation header?).
