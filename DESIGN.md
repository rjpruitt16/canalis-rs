# Canalis — design doc

Status: registration, assignment (community pool only), and request
forwarding — both buffered and live-streamed — are built and verified
end-to-end. `POST /proxy` and `POST /jobs` resolve the assignment,
forward the same job body to the assigned instance's own `/proxy` or
`/jobs`, and relay the response: a plain response is buffered and
relayed verbatim (status, content-type, body); an SSE response
(Aquifer's fallback-to-queue path, and always the case for `/jobs`) is
relayed live via `reqwest`'s `bytes_stream()` feeding directly into
Axum's `Body::from_stream` — genuinely proven, not assumed: a mock
sending two chunks 2 seconds apart showed the client receiving them
2.007s apart too, ruling out Canalis secretly buffering the whole
response before relaying it.

**Pool exhaustion now queues durably (`account_queue.rs`), not via an
in-memory retry loop.** The retry-loop version (an earlier iteration of
this same session) genuinely worked, but had a real durability gap: if
Canalis's own process crashed while a request sat in that loop, the
pending work vanished without a trace — a lower durability bar than the
rest of this system holds itself to. Replaced with: pool exhaustion
durably pushes the job onto `canalis:account_queue:<user_id>` (survives
a Canalis crash) and adds `user_id` to `canalis:pending_tenants` (so
draining doesn't need to scan every possible tenant to find work). The
waiting connection polls for a *cached result*
(`canalis:result:<idempotent_key>`, short TTL) rather than retrying
`assign()` itself — delivery is driven by a separate trigger, not by the
polling.

**Draining is triggered by registration, not a background sweep.** Every
`register_if_free` call that actually adds a genuinely new free instance
(not a no-op heartbeat) spawns `try_drain_one`: pop one pending tenant
(`SPOP canalis:pending_tenants` — no fairness ordering across *which*
tenant, matching the already-dropped FIFO-promotion requirement, but
FIFO *within* one tenant's own queue), assign them the freed instance,
and dispatch headlessly to that instance's own `/jobs` (not `/proxy` —
this job was already queued once, no reason to attempt a synchronous
direct attempt again). `/jobs` always resolves to a full SSE lifecycle;
reading the response with `.bytes()` waits for that stream to close and
hands back the complete event history, which becomes the cached result
verbatim — no manual SSE parsing needed, and no information is lost,
only the incremental delivery is (an honest simplification for a result
that's already history by the time anyone's asking).

**One freed instance drains a tenant's *whole* backlog, not just their
oldest job.** The account-queue exists specifically as a durable,
Valkey-backed fallback for periods where fleet capacity genuinely lags
demand — a tenant could plausibly have several jobs piled up by the time
an instance frees up for them, and since assignment is sticky per
`user_id`, the address resolved for their first job stays valid for
every job after it. `try_drain_one` pops their first job via `pop_one`
(the tenant-selection step), then loops on `account_queue::pop_for`
(pops directly from that tenant's own list, no tenant reselection)
until their queue is empty, caching a result for each job dispatched.
This is also what makes the fallback genuinely horizontally scalable,
not just durable: because the queue itself lives in Valkey rather than
in any one Canalis process's memory, multiple Canalis processes each
independently triggering drains off their own registration events —
exactly what happens naturally when several instances register in quick
succession during a scale-up — coordinate safely through the same
atomic Valkey operations already proven for `assign()`, with no
additional coordination layer needed. Verified with a real test:
`one_registration_drains_a_tenants_whole_backlog_not_just_one_job`
queues three jobs for one tenant, triggers a single drain, and confirms
all three (not just the first) got dispatched and cached.

**The drain loop dispatches concurrently and adjusts its own pacing
live, mirroring Aquifer's `account_queue.go` `run()` loop directly** —
this closes the gap the Open Questions section below used to flag, via
a new header pair: `X-Canalis-Max-Concurrent` / `X-Canalis-Rps`, read
off every dispatched job's own response
(`PacingSignal::from_headers` in `main.rs`). These are a deliberate
mirror of Aquifer's existing `X-Aqueduct-*`/`X-Aquifer-*`
`pacingHeader()` convention, but one layer further out: those headers
are the *backend* telling an Aquifer instance how hard it'll tolerate
being hit; `X-Canalis-*` is the *instance* telling Canalis the same
thing about itself. `pacing_header()` checks `X-Canalis-<name>` first,
then falls back to `X-Aqueduct-<name>` and `X-Aquifer-<name>` — Aquifer's
own existing pacing-header names — in case an instance's simplest path
to supporting this is mirroring its already-computed internal
`rps`/`maxConc` back out under headers it already produces, rather than
adding Canalis-specific ones. `drain_tenant_backlog` starts at Aquifer's
own conservative defaults (`DEFAULT_DRAIN_RPS = 2.0`,
`DEFAULT_DRAIN_MAX_CONCURRENT = 1` — i.e. fully sequential, matching
`RateConfig{RPS: 2.0, MaxConcurrent: 1}`), dispatches up to the current
concurrency ceiling via a `tokio::task::JoinSet`, paces each dispatch
*start* at `1/rps` (no jitter — Aquifer's jitter exists to desynchronize
many *different* domains' queues sharing a backend; one tenant's own
drain has no sibling queue to desynchronize from), and adjusts both
values from whichever job's response comes back next, same as Aquifer's
`msg.maxConcurrent`/`msg.rps` handling in its own `done`-channel branch.
A hardcoded ceiling (`MAX_DRAIN_CONCURRENCY_CEILING = 50`) caps how far
an instance's own claimed max-concurrent value can push things,
independent of what it advertises.

**Not yet emitted by Aquifer or ezthrottle-local under any of these
names** — until an instance actually sends one, every drain stays at
the conservative sequential default, which is still strictly correct,
just not yet taking advantage of instance-reported capacity. Proposed
as [aquifer#13](https://github.com/rjpruitt16/aquifer/issues/13) and
[ezthrottle-local#9](https://github.com/rjpruitt16/ezthrottle-local/issues/9)
rather than built directly into those repos in this pass, matching how
the earlier reconciliation-adjacent ideas this project produced were
handled.

Verified with a real timing test, not just that headers get parsed:
`x_canalis_headers_raise_concurrency_and_pacing_mid_drain` seeds a
tenant with 4 queued jobs, each dispatched job answered by a mock that
takes 300ms and advertises `X-Canalis-Max-Concurrent: 4` /
`X-Canalis-Rps: 100`. Job 1 always goes out alone against the
conservative default (300ms by itself); jobs 2–4 then go out together
once the ceiling's been learned. A regression back to fully-sequential
dispatch would take ~4×300ms≈1200ms; the real run lands around ~600ms
(job 1's wave, then one concurrent wave for the rest) — asserted as
`< 900ms` (rules out sequential) and `>= 300ms` (rules out skipping the
delay entirely), the same "loose bound, real claim" style already used
by `sse_response_is_relayed_live_not_buffered`.
`x_aqueduct_headers_are_accepted_as_a_fallback_pacing_signal` runs the
exact same proof again with the response advertising under
`X-Aqueduct-*` instead, with no `X-Canalis-*` present at all, confirming
the fallback chain itself actually works and isn't just dead code.

**A caller with no `webhook_url` whose connection drops before the
result is ready has no way to learn the outcome today** — an accepted,
documented gap, not silently glossed over, matching the same "note the
tradeoff, don't force an answer that isn't needed yet" pattern already
used elsewhere in this project. The `GET /jobs/<idempotent_key>` polling
endpoint that would close this gap is deferred to the next slice
(alongside a longer-lived idempotency dedup marker, separate from the
short-lived result cache).

Proven end-to-end, not just in isolated tests: a real curl request held
for 1.24s against a genuinely empty pool and returned the exact response
a real (mock) instance produced once registration triggered a real
drain — confirmed by inspecting raw Valkey state afterward too
(`canalis:account_queue:*` and `canalis:pending_tenants` both correctly
empty, `canalis:result:*` holding the cached outcome).

Real bug found and fixed while building this, worth remembering: a test
asserting before its own cleanup ran meant a failing run left genuine,
undrained jobs sitting in `canalis:pending_tenants` forever (Valkey
persists across separate `cargo test` invocations, unlike the test
process itself) — a *later*, unrelated test's `SPOP` would then have a
real chance of picking up that stale entry instead of its own, timing
out for a reason that had nothing to do with its own logic. All tests
in this file now run cleanup unconditionally, before any assertion that
could panic.

Separately, a **dead assigned instance does not retry the same way** —
assignment is sticky, so re-running `assign()` would return the exact
same (dead) address forever. That case gets a few quick retries against
the instance itself (a momentary blip — mirrors Aquifer's own
`account_queue.go` retry pattern), then a clean 502. Looping further
would hold the connection open with zero chance of ever resolving,
since that genuinely needs the not-yet-built release mechanism (see
below) to change the outcome at all.

**Known, accepted gap, not solved today:** nothing currently releases an
instance back to the pool once assigned — `canalis:assigned` only grows.
If an assigned instance dies permanently (not a restart on the same
address), that tenant's assignment points at a dead address forever,
and no amount of retrying fixes it. This is the same release-mechanism
gap noted in the Assignment section below; the queuing behavior above
only ever resolves *new* tenant requests against *new* capacity, not
requests stuck on an instance that's gone for good.

**Also explicitly deferred:** streaming job-state transitions (not just
terminal webhooks) from Aquifer/ezthrottle-local, to reduce how much
in-flight work is lost if a machine goes offline *permanently* rather
than just restarting — filed as
[aquifer#12](https://github.com/rjpruitt16/aquifer/issues/12) /
[ezthrottle-local#8](https://github.com/rjpruitt16/ezthrottle-local/issues/8),
not designed or scoped for implementation.

The reconciliation sweep, Reserved-pool overrides, and idempotency
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

## Account queue inside Canalis (superseded)

Superseded by the simpler pool-exhaustion retry loop in "Request handling
and adaptive routing" above — kept here for the record, not because it's
still the plan. The original idea was a real port of Aquifer's
account-queue pattern (a Tokio task-per-tenant holding its own `mpsc`
channel, mirroring a BEAM process per account) to give waiting tenants
FIFO ordering and per-tenant isolation. That turned out to be more
machinery than the actual requirement: FIFO promotion ordering was
explicitly dropped ("the number of Aquifer machines is the only
mechanism for serving N customers," not who's been waiting longest), and
without needing ordering, a plain retry loop calling the already-proven,
already-`NX`-safe `assign()` is sufficient — no new data structure, no
channels, no separate task per waiting tenant. If per-tenant fairness
ordering becomes a real requirement later, this is the design to revisit,
not reach for by default.

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

- `drain_tenant_backlog` now dispatches a tenant's backlog concurrently
  and adjusts its own pacing live from `X-Canalis-*`/`X-Aqueduct-*`/
  `X-Aquifer-*` response headers (see the account-queue section above)
  — the concurrency mechanism itself is built and tested. What's still
  genuinely open: no instance actually emits any of these headers yet,
  so every drain runs at the conservative sequential default in
  practice until Aquifer/ezthrottle-local ship one (aquifer#13,
  ezthrottle-local#9). Also unresolved: is
  registration-time capacity reporting (the fix this section originally
  proposed) still worth building *in addition* to response-header
  pacing, e.g. as a better starting point than the fixed default before
  a tenant's first job has even gone out, or does response-header
  pacing alone cover it well enough once it's actually emitted?
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
