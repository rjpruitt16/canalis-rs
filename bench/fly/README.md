# Fly benchmark

Results are in [BENCHMARKS.md](../../BENCHMARKS.md). Everything runs on Fly's private network; nothing is public.

```bash
B=$PWD/bench/fly
for a in canalis-bench-valkey canalis-bench canalis-bench-aquifer canalis-bench-driver; do fly apps create $a --org <org>; done
fly volumes create canalis_bench_data -a canalis-bench -r iad -s 1 -y
fly deploy --config $B/fly.valkey.toml --ha=false
fly deploy --config $B/fly.canalis.toml --dockerfile Dockerfile --ha=false .
fly deploy --config $B/fly.aquifer.toml --ha=false $B/aquifer && fly scale count 2 -a canalis-bench-aquifer
fly deploy --config $B/fly.driver.toml --ha=false $B/driver
```

Runs go through the driver:

```bash
fly ssh console -a canalis-bench-driver -C "sh -c 'ulimit -n 65535; /driver ramp -target http://canalis-bench.internal:8080 -spread -rates 200,500,1000 -duration 30s -users 100'"
```

- `-spread` resolves the target once and round-robins across its addresses. Without it, Fly's internal DNS fails under load.
- `-target http://canalis-bench-aquifer.internal:8080` measures Aquifer directly.
- `-timeline` adds per-second ok/failed counts.
- Long runs are safer started with `nohup … > /tmp/out.json &` and read back afterwards.

Tear down with `fly apps destroy` on all four apps.
