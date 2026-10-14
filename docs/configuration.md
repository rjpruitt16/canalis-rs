# Configuration

Canalis is configured only through environment variables. It listens on `0.0.0.0:8080`. Durations are whole numbers in the unit their name says.

CI checks that every variable Canalis reads is listed here (`scripts/check-config-docs.sh`).

## Valkey and the shared pool

| Variable | Default | What it does |
| --- | --- | --- |
| `CANALIS_VALKEY_URL` | `redis://127.0.0.1:6379` | Valkey (or Redis) holding the shared assignment table. |
| `CANALIS_AQUEDUCT_PREFIX` | `aqueduct:` | Key prefix for the pool. Use the same value as Aquifer's cluster mode so they share one pool. |
| `CANALIS_REGISTRATION_INTERVAL_SECONDS` | `15` | Instances' expected ping interval. A registration expires after 3 missed pings. |
| `CANALIS_ASSIGNMENT_IDLE_SECONDS` | `300` | How long a user's assignment to an instance survives without traffic. |
| `CANALIS_DEFAULT_USER_CAPACITY` | `100` | User capacity for instances that don't report `user_capacity`. |
| `CANALIS_QUEUE_AT_PERCENT` | `80` | At or above this `X-Aqueduct-Fullness`, an instance gets no new users. When every instance is past it, new users wait in line. |

## The line

| Variable | Default | What it does |
| --- | --- | --- |
| `CANALIS_LINE_PATH` | `canalis-line.redb` (`/data/canalis-line.redb` in the image) | The waiting line's file. Mount a volume so it survives the container being replaced. |
| `CANALIS_LINE_MAX_JOBS` | `10000` | Jobs the line holds before answering 429. |
| `CANALIS_LINE_MAX_PER_TENANT` | `1000` | Jobs one `user_id` can have waiting. |
| `CANALIS_LINE_MAX_BYTES` | `268435456` (256 MiB) | Total size of waiting jobs. |
| `CANALIS_LINE_RETRY_AFTER_SECONDS` | `5` | `Retry-After` on a full line's 429. |
| `CANALIS_RESULT_TTL_SECONDS` | `300` | How long a handed-off job's result is kept for `GET /jobs/{key}` and repeat requests. |
| `CANALIS_LINE_SWEEP_SECONDS` | `5` | How often the line is checked for jobs that can be handed out. |
| `CANALIS_POSITION_INTERVAL_MS` | `1000` | How often waiting callers get `canalis_position`. |
| `CANALIS_POOL_WAIT_TIMEOUT_MS` | `30000` | How long a caller's stream waits in line before `canalis_timeout`. The job stays in line. |
| `CANALIS_INSTANCE_RETRY_BASE_DELAY_MS` | `200` | Backoff between the 3 attempts to reach an instance, doubling each time. |

## WebSockets

| Variable | Default | What it does |
| --- | --- | --- |
| `CANALIS_WS_MAX_CONNECTIONS` | `10000` | Open sessions through this node. Past it, handshakes get 503. |
| `CANALIS_WS_MAX_WAITING` | `1000` | Sessions waiting for an instance with room. Past it, 503. |
| `CANALIS_WS_WAIT_TIMEOUT_SECONDS` | `60` | How long a session waits before a 1013 close. |
| `CANALIS_WS_DRAIN_GRACE_SECONDS` | `5` | On shutdown, time between `server_draining` and the 1012 close. |

## L8 (signed registration, request schemas)

| Variable | Default | What it does |
| --- | --- | --- |
| `L8_PRIVATE_KEY` | unset | Canalis's own Ed25519 key (base64, 64-byte or 32-byte seed). Takes precedence over the key file. |
| `CANALIS_L8_KEY_PATH` | `.l8-key` (`/data/.l8-key` in the image) | Where Canalis's key is kept. Generated on first start. |
| `CANALIS_L8_TRUSTED_NODE_KEYS` | unset | Comma-separated base64 Ed25519 public keys allowed to register. |
| `CANALIS_REQUIRE_SIGNED_REGISTRATION` | `false` | Refuse unsigned registrations. |
| `CANALIS_L8_SCHEMA_VALIDATION` | `false` | Check request bodies against the upstream's published `request_schemas`, answering 422 on a mismatch. |

## Shutdown and scaling

| Variable | Default | What it does |
| --- | --- | --- |
| `CANALIS_SHUTDOWN_TIMEOUT_SECONDS` | `30` | Hard limit on draining after SIGTERM. |
| `CANALIS_SHUTDOWN_QUIESCE_MS` | `500` | Pause after refusing new work, so load balancers notice. |
| `CANALIS_SCALE_WEBHOOK_URL` | unset | Where to POST `canalis_scale_up` when the line backs up. |
| `CANALIS_SCALE_AFTER_SECONDS` | `10` | How long the oldest job must wait before the signal. |
| `CANALIS_SCALE_WEBHOOK_INTERVAL_SECONDS` | `60` | Minimum time between signals. |

## Logging

| Variable | Default | What it does |
| --- | --- | --- |
| `RUST_LOG` | `info` | Log filter (`tracing-subscriber` syntax), e.g. `canalis_rs=debug`. |
