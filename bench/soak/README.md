# Canalis soak

Four days of load and scheduled failures against two Canalis nodes on Fly. It's for finding what only time shows: leaks, slow growth, drift.
- **Behind Canalis:** one Aquifer, which has had its own week-long soak and is the steady thing Canalis hands work to.
- **Correctness across the whole chain** (Canalis, two Aquifers, Spillway, webhooks through the plugins) is a separate, shorter test, run once this one is clean.

The soak reuses Aquifer's soak harness (`benchmark/soak` in that repository): the driver, its job ledger and status pages, and the supervisor that runs each target and takes kill orders. Everything here builds that harness from a pinned Aquifer commit (`AQUIFER_REF`), and runs the driver in its gateway mode (`-backends`):
- load goes only to the Canalis nodes;
- job status is checked on the Aquifer;
- a job Canalis put in its line counts as accepted.

## What runs

| App | What | Machine |
|---|---|---|
| `canalis-soak` | Canalis nodes `a` and `b`: one app, two machines, a volume each | 2 × performance-1x, 2 GB |
| `canalis-soak-aquifer-1` | Aquifer `q1`, registering with Canalis | performance-1x, 2 GB |
| `canalis-soak-driver` | load, the misbehaving target API, the healthy webhook receiver, the ledger, the status pages, and the Valkey both Canalis nodes share | performance-2x, 4 GB |

That's about $25 for the four days, volumes included.
- **Valkey** runs on the driver (`-valkey`), so a scheduled failure can take it away.
- **Registration:** Aquifer registers at `canalis-soak.internal`, which names both Canalis nodes.
- **Addressing:** the driver reaches each Canalis machine as `<machine id>.vm.canalis-soak.internal`.

## Load

- **Jobs:** 300 jobs/s from 50 tenants, spread over both Canalis nodes.
- **Bursts and ramps:** a 1,000/s burst at the top of every hour, and a daily 04:00 UTC ramp to 2,000/s. That's past what one Aquifer takes, so Canalis's line fills and drains every day.
- **WebSockets:** sessions run through node `a`'s `/websocket`.
- **Misbehaving target API:** it misbehaves in 10-minute slots: slow, 503s, 429s, outages.
- **Webhook receiver:** stays healthy, so every job's webhook is the record of it finishing.

## Failures

`scenarios.py` prints the schedule pasted into `fly.driver.toml`: 36 failures over about 91 hours, after a two-hour warm-up. There are eight a day, in a different order each day:

- **Kill -9 of each Canalis node.**
- **Node `a` held down for 5 minutes,** past the 30 s ownership lease, so `b` takes its lines.
- **Node `b` frozen for 20 s,** shorter than the lease.
- **A 2-minute Valkey outage.**
- **The Aquifer held down for 5 minutes.** Canalis has nowhere to hand work, so its line fills, then drains.
- **A kill -9 of the Aquifer.**
- **A rolling restart:** drain `a`, then `b` 10 minutes later.

Leak soaks still need churn: leaks hide in reconnects and recovery, not in steady traffic.

## What counts as passing

- **No leaks.** Each node's RSS and open files plateau day over day at the same hour, in the per-node rows of `/status/hourly`.
- **The line shrinks back.** Its file size returns to its baseline after the line drains.
- **No lost jobs:** `lost` and `lost_in_gateway_line` stay 0.
- **Recovery.** After each failure, completions return within minutes and the line drains.
- **No latency drift:** p99 in quiet hours doesn't drift up.

## Deploy

`AQUIFER_REF` is the Aquifer commit whose driver has gateway mode. Secrets live in `~/.config/canalis-soak/` and are never printed.

```bash
S=bench/soak; A=<aquifer commit>
for app in canalis-soak canalis-soak-aquifer-1 canalis-soak-driver; do fly apps create $app --org <org>; done

fly volumes create canalis_soak_driver_data -a canalis-soak-driver -r iad -s 5 -y
openssl rand -hex 32 > ~/.config/canalis-soak/report-token && chmod 600 ~/.config/canalis-soak/report-token
fly secrets set -a canalis-soak-driver SOAK_REPORT_TOKEN="$(cat ~/.config/canalis-soak/report-token)" --stage

fly volumes create canalis_soak_data -a canalis-soak -r iad -s 3 -y
fly volumes create canalis_soak_data -a canalis-soak -r iad -s 3 -y
fly deploy -c $S/fly.canalis.toml --dockerfile $S/Dockerfile.canalis --build-arg AQUIFER_REF=$A --ha=false .
fly scale count 2 -a canalis-soak -y

fly volumes create aquifer_data -a canalis-soak-aquifer-1 -r iad -s 5 -y
fly deploy -c $S/fly.aquifer.toml --dockerfile $S/Dockerfile.aquifer --build-arg AQUIFER_REF=$A --ha=false $S

# The driver last, once it knows the Canalis machines (ids from fly machines list -a canalis-soak):
fly secrets set -a canalis-soak-driver --stage \
  SOAK_NODES="a=http://<id a>.vm.canalis-soak.internal:8080|http://<id a>.vm.canalis-soak.internal:9090,b=http://<id b>.vm.canalis-soak.internal:8080|http://<id b>.vm.canalis-soak.internal:9090"
fly deploy -c $S/fly.driver.toml --dockerfile $S/Dockerfile.driver --build-arg AQUIFER_REF=$A --ha=false $S
```

Then check `https://canalis-soak-driver.fly.dev/status` for a few minutes before starting the monitor.

## Monitor

`monitor/` creates a Claude Managed Agent, as Aquifer's soak does. Every 6 hours it reads the status pages and posts its report there. It opens a `soak-alert` issue on canalis-rs only for a warning or a problem, so it stays quiet unless something's wrong. It can reach only two hosts, the driver and api.github.com. Its GitHub token is fine-grained to issues.

```bash
ANTHROPIC_API_KEY_FILE=~/.config/canalis-soak/anthropic-key \
  SOAK_REPORT_TOKEN_FILE=~/.config/canalis-soak/report-token \
  GITHUB_TOKEN_FILE=~/.config/canalis-soak/github-token \
  python3 bench/soak/monitor/setup.py
```

The deployment is created paused. Do one manual run first, read its report, then unpause it.

## Teardown

After four days:
1. Run `fly apps destroy` on all three apps.
2. Pause or delete the monitor's deployment.
3. Regenerate the GitHub token and the Anthropic key; both were pasted into a chat.
