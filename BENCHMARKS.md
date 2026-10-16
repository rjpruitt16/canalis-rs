# Benchmarks

## Fly.io, 2026-10-16

This run was one Canalis in front of two Aquifer instances on Fly's private network in `iad`. The harness is in [`bench/fly/`](bench/fly/).

**Setup:**

| Piece | Size |
|---|---|
| Canalis (main, then each fix below) | performance-1x (1 dedicated vCPU, 2 GB), 1 GB volume |
| Aquifer ×2 (main, pace ceiling 5,000 rps / 500 concurrent) | performance-1x each |
| Valkey 8 | shared-cpu-2x |
| Driver: open-loop load at a fixed rate, plus a mock target API that answers immediately | performance-2x |

**Method:**
- Each request is a `POST /proxy` for one of 100 users, so all of them fit within the instances' default capacity and the line stays out of the way. The full-fleet run is the exception.
- Latency is measured from the driver until the whole response has been read.
- "Direct" sends the same load straight to the two Aquifer instances, round-robin.
- Numbers come from single 20–30 s runs, so treat them as indicative. Back-to-back steps without a pause were visibly noisier.

### What Canalis adds

| Load | Direct p50 / p99 | Canalis p50 / p99 (before) | Canalis p50 / p99 (with the instance cache) |
|---|---|---|---|
| 200 rps | 2.6 / 5.7 ms | 6.4 / 11.3 ms | 3.8 / 7.5 ms |
| 500 rps | 2.2 / 5.3 ms | 7.2 / 17.0 ms | 3.6 / 47 ms |
| 1,000 rps | 2.0 / 22 ms | 7.3 / 40 ms | 3.7 / 55 ms |
| 2,500 rps | 2.2 / 86 ms | 23–39 / 139–168 ms | 7.4 / 84 ms |

Before the fix, every claim read the instance list from Valkey: the set, each record, and each fullness value. That was four Valkey round trips per request, counting the claim script. Reusing the list for 250 ms (`CANALIS_MEMBER_CACHE_MS`) leaves one round trip, the claim script. At normal load that brings the added latency from about 5 ms to about 1.6 ms.

### Ceiling

Canalis is CPU-bound. On one performance-1x it used:
- 79% of its vCPU at 1,500 rps before the cache, and 71% after;
- 100% at 2,500 rps.

Valkey sat around 10%.

It was healthy up to about **2,500 rps**. Past that, it used to collapse:

| Offered load | Before load shedding | With `CANALIS_MAX_IN_FLIGHT=1000` |
|---|---|---|
| 3,000 rps | saturated: p50 128 ms, p90 1.4 s | 2,914/s served, 2,579 × 503, no timeouts, p99 697 ms |
| 4,000 rps | 861/s served, connections timing out, p99 41 s | 2,662/s served, 40,132 × 503, no timeouts, p99 803 ms |

Shedding answers requests past the cap with an immediate `503` + `Retry-After: 1`. Registration, health and metrics are never shed. The default cap is 5,000; raise it if your target APIs are slow, since a request counts until its response starts.

The CPU cost per request is still about 0.4–0.5 ms, high for a proxy. The next step is profiling.

### When the fleet is full

The full-fleet run gave each instance room for 20 users (40 in total) and released assignments after 5 s idle. It sent 200 users at 500 rps for 30 s.

| | Before | After both fixes |
|---|---|---|
| Line empty after | ~288 s | **89 s** |
| Accepted / delivered to the target API | 15,001 / 15,001 | 13,091 / 13,091 |
| 429 (line at its 10,000-job cap) | — | 1,910 |

The two fixes:
1. **Drain pace.** Each user's backlog left the line at 2 jobs/s, one at a time. That's Aquifer's pace toward a target API, but the instance isn't the resource being protected. It's now `CANALIS_DRAIN_RPS` (100) and `CANALIS_DRAIN_MAX_CONCURRENT` (10), and the instance's own `X-Aqueduct-Rps` / `Max-Concurrent` still lower them.
2. **Slot release.** A user whose backlog finished draining kept the slot until the assignment idled out, which is idle seconds plus two heartbeats (35 s in the run). The slot is now released when the drain finishes.

How the line behaved in the sampled run:

| Time (s) | What happened |
|---|---|
| 0–24 | The line fills to its cap |
| 24–65 | Nothing drains, because the users holding slots keep sending and every request renews a slot |
| 65–89 | The line empties at about 420 jobs/s once those slots idle out |

**Still open:**
- **Users waiting during a burst never get a turn.** The users holding slots keep renewing them for as long as they keep sending. Fixing this needs a fairness decision, such as a maximum hold time per slot when others are waiting.
- **Slow responses while filling the line.** Canalis took about 30 s to answer `/metrics` while thousands of jobs were entering the line. Each job is its own synced disk commit, so grouping commits is the likely fix.

### Losing an instance

The run sent 1,000 rps for 60 s and hard-killed (SIGKILL) one of the two Aquifer machines at 20 s:
- **28 of 60,001** requests failed with 502, all within one second. Those were in flight on the dead machine, and they're not retried elsewhere because they may already have run.
- The next second ran at full rate.
- The killed machine restarted and registered again by itself.

### Found and fixed along the way

- **IPv6** ([#30](https://github.com/rjpruitt16/canalis-rs/pull/30)): Canalis listened on IPv4 only, and recorded IPv6 instances as `fdaa::5:8080`, which isn't a usable URL. On Fly's IPv6 network it couldn't have worked at all.
- **Restarts** ([#31](https://github.com/rjpruitt16/canalis-rs/pull/31)): after any Canalis restart, every node that had already registered got 401 indefinitely, because the node keys Canalis learned through L8 lived only in memory. They're now saved on the volume. In the benchmark, Canalis restarted several times and logged `remembered 2 node keys` each time.

### Not covered yet

- WebSocket sessions through Canalis.
- More than one Canalis.
- Slow or failing target APIs.
- A longer soak.
