# Week-long soak

Two Canalis nodes in front of two Aquifer instances and one Spillway instance, on Fly, for a week of load and scheduled failures. It reuses Aquifer's soak harness (`benchmark/soak` in that repository): its driver, its ledger, its status pages, and the supervisor that runs each target and takes kill orders. Everything here builds that harness from a pinned Aquifer commit (`AQUIFER_REF`), so a change there can't move a soak in progress.

**Status: set up, not deployed.** It waits for Aquifer's and Spillway's own soaks to finish, so their lessons can go in first. It also needs the driver changes below.

## What runs

| App | What | Machine | About $/week |
|---|---|---|---|
| `canalis-soak` | Canalis nodes `a` and `b`: one app, two machines, a volume each | 2 × performance-1x, 2 GB | 15 |
| `canalis-soak-aquifer-1`, `-2` | Aquifer instances `q1`, `q2`, registering with Canalis | 2 × performance-1x, 2 GB | 15 |
| `canalis-soak-spillway` | Spillway instance `s`, in the same pool | performance-2x, 4 GB | 15 |
| `canalis-soak-driver` | load, the misbehaving target API and webhook receiver, the ledger, the status pages, and the Valkey the Canalis nodes share | performance-2x, 4 GB | 15 |

That comes to about $60–65 for the week, plus a few dollars for volumes and the monitor. Each target runs under Aquifer's soak supervisor (`:9090`), so the driver can kill, hold, freeze or drain it.

- **Valkey:** Valkey runs on the driver (`-valkey`), so a scheduled failure can take it away.
- **Registration:** instances register at `canalis-soak.internal`, which names both Canalis nodes. They take one URL, and Go's dialer moves on to the next address when one node is down. Spillway's client may not do this; if Spillway drops out of the pool during a hold of one node, that's a finding about running Canalis behind one name.

## Load

- **Jobs:** 500 jobs/s from 50 tenants, spread over both Canalis nodes. There's a 2,500/s burst at the top of every hour, and a daily ramp at 04:00 UTC to 6,000/s.
- **WebSockets:** sessions run through Canalis's `/websocket`. This path hasn't been tested under load before.
- **Misbehaving services:** the target API and webhook receiver misbehave in 10-minute slots: slow, 503s, 429s and outages.

## Failures

`scenarios.py` prints the schedule, which is pasted into `fly.driver.toml`: 94 failures over about 163 hours, starting after a two-hour warm-up. Each day has them in a different order:

- **Kill -9 of a Canalis node,** four times a day. The line is on disk, and ownership has to move.
- **Kill -9 of each instance,** once a day.
- **Node `a` held down for 5 minutes.** Its line ownership expires (30 s) and moves to `b`.
- **Node `b` frozen for 20 s,** less than the ownership lease.
- **A 2-minute Valkey outage.**
- **Both Aquifers held down for 5 minutes.** Only Spillway is left, so the line fills.
- **A rolling restart:** drain `a`, then `b` 10 minutes later.
- **Every other day, every instance down for 3 minutes.** The line has to hold everything, then drain fairly.

The soak's own random kills are pushed out of the week, so every failure is one the schedule chose.

## What counts as passing

- **No lost jobs.** No job is lost that Canalis accepted, over a week of failures. A kill -9 of a Canalis node loses nothing it accepted.
- **Recovery.** After every failure the line drains back to near empty and completions return within minutes.
- **No leaks.** RSS, open files and the line's file size plateau.
- **Stable latency.** p99 in quiet phases doesn't drift, and the daily ramp ceiling doesn't fall.
- **WebSockets.** The WebSocket ledger loses no commands.

## Driver changes needed first (design, for sign-off)

Aquifer's driver talks to Aquifer. Through Canalis it would count healthy jobs as lost or as errors. The changes, on a branch in Aquifer (`benchmark/soak/driver`), off by default so Aquifer's soaks are unchanged:

1. **`-backends id=target|supervisor,...`: the instances behind the gateway.**
   - Load never goes to them, but scheduled failures can name them (`q1`, `q2`, `s`).
   - A job's status is checked there: Canalis's `GET /jobs/{key}` only knows jobs still in its line, and recent results. The ledger asks each backend for the job ID until one knows it. It's lost only if none does.
2. **Jobs answered late.** A job can wait in Canalis's line longer than the driver's 30 s client timeout, and Canalis still delivers it. Today that's a client error, and its webhook later looks like a stray. Instead:
   - Count it as `accepted_unanswered`.
   - Resolve it by its webhook, or by `GET /jobs/{key}?user_id=` on Canalis.
   - Count it as lost only if neither ever knows it.
3. **WebSocket sessions over all `-nodes`,** not only the first.
4. **Per-node stats.** `/status` follows only the first node's supervisor. It should show each node's RSS, open files and restarts, so a leak on `b` isn't missed.

Later, once the Shopify plugin is merged: signed Shopify webhooks to `/hooks/shopify`, with retries of the same event. The ledger then expects each event delivered exactly once.

## Deploy (when it's time)

`AQUIFER_REF` is the Aquifer commit with the driver changes. `SPILLWAY_REF` is the Spillway commit to test. Secrets live in `~/.config/canalis-soak/` and are never printed.

```bash
S=bench/soak; A=<aquifer commit>; SP=<spillway commit>
for app in canalis-soak canalis-soak-aquifer-1 canalis-soak-aquifer-2 canalis-soak-spillway canalis-soak-driver; do
  fly apps create $app --org <org>
done
fly volumes create canalis_soak_driver_data -a canalis-soak-driver -r iad -s 5 -y
openssl rand -hex 32 > ~/.config/canalis-soak/report-token
fly secrets set -a canalis-soak-driver SOAK_REPORT_TOKEN="$(cat ~/.config/canalis-soak/report-token)" --stage
fly deploy -c $S/fly.driver.toml --dockerfile $S/Dockerfile.driver --build-arg AQUIFER_REF=$A --ha=false $S

fly volumes create canalis_soak_data -a canalis-soak -r iad -s 3 -y -n 2
fly deploy -c $S/fly.canalis.toml --dockerfile $S/Dockerfile.canalis --build-arg AQUIFER_REF=$A --ha=false .
fly scale count 2 -a canalis-soak -y
for n in 1 2; do
  fly volumes create aquifer_data -a canalis-soak-aquifer-$n -r iad -s 5 -y
  fly deploy -c $S/fly.aquifer.toml -a canalis-soak-aquifer-$n --dockerfile $S/Dockerfile.aquifer --build-arg AQUIFER_REF=$A --ha=false $S
done
fly volumes create spillway_data -a canalis-soak-spillway -r iad -s 5 -y
fly secrets set -a canalis-soak-spillway SECRET_KEY_BASE="$(openssl rand -hex 64)" --stage
fly deploy -c $S/fly.spillway.toml --dockerfile $S/Dockerfile.spillway --build-arg AQUIFER_REF=$A --build-arg SPILLWAY_REF=$SP --ha=false $S

# Point the driver at the machines (ids from fly machines list):
fly secrets set -a canalis-soak-driver \
  SOAK_NODES="a=http://<id a>.vm.canalis-soak.internal:8080|http://<id a>.vm.canalis-soak.internal:9090,b=..." \
  SOAK_BACKENDS="q1=http://canalis-soak-aquifer-1.internal:8080|http://canalis-soak-aquifer-1.internal:9090,q2=...,s=http://canalis-soak-spillway.internal:4000|http://canalis-soak-spillway.internal:9090"
```

Then check `https://canalis-soak-driver.fly.dev/status` for a few minutes before starting the monitor.

## Monitor

`monitor/` creates a Claude Managed Agent, as Aquifer's soak does. It reads the status pages every 6 hours, posts a report there, and opens a `soak-alert` issue on canalis-rs for a warning or a problem. It's locked to two hosts: the driver and api.github.com. The GitHub token it holds is fine-grained and can only read and write issues.

```bash
ANTHROPIC_API_KEY_FILE=~/.config/canalis-soak/anthropic-key \
  SOAK_REPORT_TOKEN_FILE=~/.config/canalis-soak/report-token \
  GITHUB_TOKEN_FILE=~/.config/canalis-soak/github-token \
  python3 bench/soak/monitor/setup.py
```

The deployment is created paused. Do one manual run first, read its report, then unpause it.

## Teardown

Run `fly apps destroy` on all five apps, and pause or delete the monitor's deployment. Then regenerate the GitHub token.
