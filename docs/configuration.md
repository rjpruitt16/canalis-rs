# Configuration

Canalis is configured only through environment variables. It listens on `[::]:8080` (IPv6, and IPv4 on dual-stack hosts such as Linux), falling back to `0.0.0.0:8080` where IPv6 is off; `CANALIS_LISTEN_ADDR` overrides it. Durations are whole numbers in the unit their name says.

CI checks that every variable Canalis reads is listed here (`scripts/check-config-docs.sh`).

## Listening

| Variable | Default | What it does |
| --- | --- | --- |
| `CANALIS_LISTEN_ADDR` | `[::]:8080` | Address to listen on. The default falls back to `0.0.0.0:8080` if IPv6 is unavailable. |

## Several Canalis nodes

See [Running several Canalis nodes](../README.md#running-several-canalis-nodes) in the README.

| Variable | Default | What it does |
| --- | --- | --- |
| `CANALIS_ADVERTISE_URL` | `http://[$FLY_PRIVATE_IP]:8080` on Fly, else unset | How other Canalis nodes reach this one. Unset means this node doesn't take part: it never claims a tenant's line, so a single Canalis behaves as before. |
| `CANALIS_NODE_ID` | `$FLY_MACHINE_ID`, else random per start | This node's id among the Canalis nodes sharing Valkey. |
| `CANALIS_PEER_CONNECTIONS` | `32` | HTTP/2 connections this node opens to each other Canalis node for forwarding. Each carries up to 200 waiting-room streams at once. |
| `CANALIS_LINE_OWNER_TTL_SECONDS` | `30` | How long a node's claim on a tenant's line lasts without renewal. It's renewed every sweep while the tenant has jobs waiting. |

## Load shedding

| Variable | Default | What it does |
| --- | --- | --- |
| `CANALIS_MAX_IN_FLIGHT` | `5000` | Past this many `/proxy` and `/jobs` requests in progress, new ones get `503` + `Retry-After: 1` at once instead of slowing everything down. A request counts until its response starts, so a slow target API keeps requests in progress longer: raise it if yours answer slowly. `0` turns shedding off. Registration, health and metrics are never shed. |

## Valkey and the shared pool

| Variable | Default | What it does |
| --- | --- | --- |
| `CANALIS_VALKEY_URL` | `redis://127.0.0.1:6379` | Valkey (or Redis) holding the shared assignment table. |
| `CANALIS_AQUEDUCT_PREFIX` | `aqueduct:` | Key prefix for the pool. Use the same value as Aquifer's cluster mode so they share one pool. |
| `CANALIS_REGISTRATION_INTERVAL_SECONDS` | `15` | Instances' expected ping interval. A registration expires after 3 missed pings. |
| `CANALIS_ASSIGNMENT_IDLE_SECONDS` | `300` | How long a user's assignment to an instance survives without traffic. |
| `CANALIS_DEFAULT_USER_CAPACITY` | `100` | User capacity for instances that don't report `user_capacity`. |
| `CANALIS_MEMBER_CACHE_MS` | `250` | How long the instance list used for claims is reused before Valkey is read again. Registrations and instances this node finds draining or unreachable take effect at once; fullness may lag by this much. `0` reads Valkey on every claim. |
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
| `CANALIS_DRAIN_RPS` | `100` | How fast one tenant's waiting jobs are handed to their instance once it has room, until the instance says otherwise with `X-Aqueduct-Rps`. |
| `CANALIS_DRAIN_MAX_CONCURRENT` | `10` | How many of one tenant's waiting jobs are handed over at once (at most 50), until the instance says otherwise with `X-Aqueduct-Max-Concurrent`. |
| `CANALIS_INSTANCE_RETRY_BASE_DELAY_MS` | `200` | Backoff between the 3 attempts to reach an instance, doubling each time. |

## WebSockets

| Variable | Default | What it does |
| --- | --- | --- |
| `CANALIS_WS_MAX_CONNECTIONS` | `10000` | Open sessions through this node. Past it, handshakes get 503. |
| `CANALIS_WS_MAX_WAITING` | `1000` | Sessions waiting for an instance with room. Past it, 503. |
| `CANALIS_WS_WAIT_TIMEOUT_SECONDS` | `60` | How long a session waits before a 1013 close. |
| `CANALIS_WS_DRAIN_GRACE_SECONDS` | `5` | On shutdown, time between `server_draining` and the 1012 close. |

## Who a request is for

See [Authentication](../README.md#authentication) in the README.

| Variable | Default | What it does |
| --- | --- | --- |
| `CANALIS_AUTH` | `header` | `header`: believe the `user_id` / `X-Aqueduct-User-ID` sent. `upstream-secret`: the same, but only from a gateway that sends the shared secret. `jwt`: verify the caller's token and take the tenant from it. |
| `CANALIS_UPSTREAM_SECRET` | unset | Required for `upstream-secret`. The gateway sends it as `X-Canalis-Upstream-Secret`. |
| `CANALIS_JWKS_URL` | unset | Required for `jwt`. The login provider's published signing keys. |
| `CANALIS_JWT_ISSUER` | unset | Required for `jwt`. The `iss` tokens must have. |
| `CANALIS_JWT_AUDIENCE` | unset | Comma-separated. If set, `aud` must include one of them. |
| `CANALIS_JWT_AUTHORIZED_PARTIES` | unset | Comma-separated. If set, `azp` must be one of them (Clerk recommends this). |
| `CANALIS_JWT_TENANT_CLAIM` | `sub` | The claim that names the tenant, as a dotted path for nested claims (`org_id`, `o.id`). |
| `CANALIS_JWT_POOL_CLAIM` | unset | A claim naming the request's pool. If present in the token, it overrides `X-Aqueduct-Pool`. |
| `CANALIS_JWKS_REFRESH_SECONDS` | `600` | How often the key set is refetched. An unknown key id also triggers a refetch, at most every 30s. |
| `CANALIS_FORWARD_AUTHORIZATION` | `false` | Pass the caller's `Authorization` header (and `access_token`) on to the instance on WebSocket handshakes. |

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
