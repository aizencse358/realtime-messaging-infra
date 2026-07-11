# realtime-messaging-infra

[![CI](https://github.com/aizencse358/realtime-messaging-infra/actions/workflows/ci.yml/badge.svg)](https://github.com/aizencse358/realtime-messaging-infra/actions/workflows/ci.yml)

A horizontally scalable WebSocket messaging gateway built with FastAPI, Redis,
and DynamoDB — demonstrating that a stateless-at-the-LB, refcounted-at-the-
gateway pub/sub design lets any of N gateway replicas serve any user, with no
sticky sessions.

## Architecture

```
                     ┌────────────┐
        WS clients ─▶│   nginx    │  round-robin, no sticky sessions
                     └─────┬──────┘
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         ┌─────────┐  ┌─────────┐  ┌─────────┐
         │gateway1 │  │gateway2 │  │gateway3 │   FastAPI + websockets
         └────┬────┘  └────┬────┘  └────┬────┘
              │            │            │
              └──────┬─────┴─────┬──────┘
                      ▼           ▼
                  ┌───────┐   ┌──────────┐
                  │ Redis │   │ DynamoDB │
                  │pub/sub│   │  Local   │
                  │presence│  │ (durable)│
                  │registry│  └──────────┘
                  └───────┘
```

Each gateway replica is a plain FastAPI process holding only the WebSocket
connections it physically accepted. There is no shared in-memory state
between replicas — cross-instance behavior (room fanout, direct delivery,
presence, "who owns this user") all goes through Redis. That's what makes it
safe for nginx to round-robin every new connection with zero session
affinity.

### Redis keys/channels

| Key/channel                     | Purpose                                            | TTL  |
|----------------------------------|-----------------------------------------------------|------|
| `chat:room:{room_id}`            | Pub/sub channel; one message published here reaches every gateway with a local member in that room | — |
| `chat:user:{user_id}`            | Pub/sub channel for direct delivery to one user, regardless of which gateway they're connected to | — |
| `presence:user:{user_id}`        | Online/offline flag, refreshed every ~20s while connected | ~45s |
| `registry:user:{user_id}`        | Maps a connected user to the gateway instance currently serving them | ~45s |

### Refcounted room subscriptions

Each gateway subscribes to `chat:room:{room_id}` only when the **first**
locally-connected member joins that room, and unsubscribes when the
**last** local member leaves. A room with 10,000 members spread across 3
gateways results in at most 3 active Redis subscriptions for that room —
not 10,000 — while still delivering every message to every member no
matter which replica they're attached to. See `src/connection_manager.py`.

### DynamoDB schema

| Table         | PK              | SK                      | Notes                                   |
|---------------|-----------------|--------------------------|------------------------------------------|
| `Messages`     | `conversation_id` | `sort_key` = `{timestamp_ms}#{message_id}` | Durable message log per room, naturally time-ordered |
| `Users`        | `user_id`        | —                         | `last_seen_at` flushed on disconnect     |
| `RoomMembers`  | `room_id`        | `user_id`                 | GSI `gsi_user_rooms` reverses the key (`user_id` → `room_id`) for "which rooms is this user in" lookups |

### Gateway responsibilities

- Refcount room subscriptions per instance (subscribe on first local join,
  unsubscribe on last local leave).
- On `send`: publish to the room's Redis channel **and** persist the message
  to DynamoDB — durability doesn't depend on delivery, and vice versa.
- Heartbeat presence (`presence:user:{user_id}`) and refresh the registry
  entry (`registry:user:{user_id}`) every ~20s while connected.
- On disconnect: flush `last_seen_at` to DynamoDB, drop presence/registry
  keys immediately (don't wait out the TTL), and unwind room refcounts.

## Project structure

```
realtime-messaging-infra/
├── docker-compose.yml     # redis, dynamodb-local, table init, 3 gateway replicas, nginx, prometheus, grafana
├── Dockerfile             # uv-based image for the gateway (and load test scripts)
├── nginx/nginx.conf       # WS-aware round-robin LB (no sticky sessions) + static chat client
├── web/index.html         # self-contained browser chat client
├── monitoring/
│   ├── prometheus.yml               # scrapes all 3 gateway replicas
│   └── grafana/                     # auto-provisioned datasource + dashboard
├── pyproject.toml / uv.lock
├── src/
│   ├── main.py                # FastAPI app, WS endpoint, frame dispatch, HTTP routes
│   ├── connection_manager.py  # local connections, refcounted room subs, presence/registry
│   ├── dynamo.py              # table definitions + boto3 access (via asyncio.to_thread)
│   ├── redis_client.py        # shared redis.asyncio client
│   ├── metrics.py              # Prometheus metric definitions
│   ├── observability.py        # structured logging + HTTP timing middleware
│   ├── schemas.py               # WS frame pydantic models
│   ├── config.py                # env config + key/channel name helpers
│   └── init_tables.py            # one-shot DynamoDB table creation (compose service)
├── tests/                  # pytest, fakeredis + moto (no live containers needed)
└── loadtest/
    ├── common.py               # Stats/percentile helpers, ws_url()
    ├── test_connect_rate.py    # connection accept rate
    ├── test_fanout_latency.py  # cross-instance fanout latency (p50/p95/p99)
    ├── test_churn.py           # subscribe/unsubscribe churn cost
    └── run_all.py              # runs everything, prints the combined table
```

## Running it

Everything runs via Docker Compose:

```bash
docker compose up --build
```

This brings up Redis, DynamoDB Local (in-memory, tables created by the
`dynamodb-init` one-shot service), 3 gateway replicas, nginx listening on
`localhost:8080`, and Prometheus + Grafana (see [Observability](#observability)).

### Browser chat client

Open **`http://localhost:8080/`** — a small static page (`web/index.html`,
served by nginx alongside the API) that connects over WebSocket, joins
rooms, loads history, and sends/receives messages. It's also the easiest
way to *see* the no-sticky-sessions claim: every connection displays which
gateway replica (`gateway1`/`2`/`3`) it landed on, so opening a few browser
tabs and joining the same room shows messages fanning out across replicas
in real time.

Or connect a raw WebSocket client to `ws://localhost:8080/ws/{user_id}` and
send JSON frames directly:

```json
{"type": "join", "room_id": "general"}
{"type": "send", "room_id": "general", "text": "hello"}
{"type": "leave", "room_id": "general"}
```

Fetch durable message history for a room (any replica, since it reads
straight from DynamoDB rather than gateway-local state):

```
GET /rooms/{room_id}/messages?limit=50&before={sort_key}
```

Returns the most recent `limit` messages (oldest-to-newest) before the
`before` cursor (defaults to "now" if omitted). The response includes
`next_before` — the `sort_key` of the oldest message on the page — to page
further back; it's `null` once a page comes back short (no older messages
left).

Debug endpoints (hit any replica directly or through nginx):

- `GET /healthz` — reports which gateway instance answered
- `GET /presence/{user_id}` — online/offline
- `GET /registry/{user_id}` — which gateway instance currently owns this user
- `GET /metrics` — Prometheus scrape target for this instance

## Build phases

1. **Single gateway, end-to-end** — one gateway, Redis, DynamoDB Local; join
   a room, send a message, see it persisted and delivered back over the same
   socket.
2. **3 replicas behind nginx** — connect clients repeatedly; nginx's default
   round-robin (no `ip_hash`) scatters them across `gateway1/2/3`. A message
   sent by a client on `gateway1` is delivered to a member connected on
   `gateway3` purely via the `chat:room:{room_id}` Redis channel — proving
   fanout doesn't depend on which replica anyone landed on.
3. **Load tests** — `loadtest/` measures connection accept rate,
   cross-instance fanout latency percentiles, and room subscribe/unsubscribe
   churn cost.

## Observability

Two layers, matching different needs:

- **Structured logs** (`src/observability.py`) — `key=value` lines: every
  HTTP request is timed and logged (`event=request method=... path=...
  status=... duration_ms=...`), and connection lifecycle events fire as they
  happen: `event=ws_connected`, `event=ws_disconnected`, `event=room_joined`,
  `event=room_left`, `event=room_subscribed` / `event=room_unsubscribed`
  (only on the actual refcount transition), `event=message_sent`.
  `LOG_LEVEL` (default `INFO`) controls verbosity; set it per-service in
  `docker-compose.yml`.

- **Prometheus + Grafana** (`src/metrics.py`) — every gateway exposes
  `GET /metrics`. Prometheus (`monitoring/prometheus.yml`) scrapes all 3
  replicas every 5s; Grafana auto-provisions a Prometheus datasource and a
  ready-made **"Realtime Messaging Gateway"** dashboard
  (`monitoring/grafana/dashboards/realtime-messaging.json`) with panels for:

  - active WebSocket connections per gateway
  - active room subscriptions per gateway (the refcounted subscribe/unsubscribe state)
  - message throughput: sent vs. fanout-delivered
  - room subscribe/unsubscribe churn rate
  - HTTP request latency (p95 by route)
  - message persist latency (DynamoDB write, p50/p95/p99)
  - cluster-wide totals: active connections, active subscriptions, room joins

  Open Grafana at `http://localhost:3000` (anonymous viewer access is
  enabled by default) and Prometheus directly at `http://localhost:9090`.

## Tests

Two tiers:

- **`tests/`** — unit-level, no containers needed. `tests/conftest.py` swaps
  in `fakeredis` for Redis and `moto` for DynamoDB, so `ConnectionManager`
  and the `dynamo` module are exercised against realistic (in-memory)
  protocol behavior rather than mocks of the code itself.

  ```bash
  uv sync --group dev
  uv run pytest -v
  ```

  CI (`.github/workflows/ci.yml`) runs this on every push and PR.

- **`tests/integration/`** — drives the real `docker compose` stack through
  nginx: end-to-end join/send/receive, message history pagination against
  real DynamoDB, cross-instance fanout (asserts connections actually land on
  more than one replica and a message crosses between them), and chaos
  tests that kill a gateway container mid-connection and verify the client
  gets disconnected, the cluster keeps serving new connections, and rooms
  still fan out correctly afterward. Auto-skips (not fails) if the stack
  isn't reachable, so it's safe to include in a full `uv run pytest` run
  even without compose up — this is intentionally *not* wired into CI (no
  docker-in-docker there), it's for local verification:

  ```bash
  docker compose up -d
  uv run pytest tests/integration -v
  ```

  This tier is what actually caught two real bugs while building this
  project — a pub/sub listener crash and an nginx routing gotcha — that the
  fakeredis/moto unit tests couldn't see, since both only reproduced against
  the real stack.

## Load testing

Run against the compose stack (through nginx, so traffic is spread across
all 3 replicas):

```bash
uv sync
GATEWAY_URL=ws://localhost:8080 uv run python -m loadtest.run_all
```

Individual scripts (`test_connect_rate.py`, `test_fanout_latency.py`,
`test_churn.py`) can also be run standalone; each takes env vars for its
sample size (`CONNECT_RATE_N`, `FANOUT_RECEIVERS`/`FANOUT_MESSAGES`,
`CHURN_ROOMS`).

Sample output shape:

```
== combined benchmark table ==
Metric                                   avg(ms)   min(ms)   p50(ms)   p95(ms)   p99(ms)   max(ms)       n
------------------------------------------------------------------------------------------------------------
Connection accept latency                X.XXX     X.XXX     X.XXX     X.XXX     X.XXX     X.XXX      500
Cross-instance fanout latency            X.XXX     X.XXX     X.XXX     X.XXX     X.XXX     X.XXX     2000
Room join (subscribe) RTT                X.XXX     X.XXX     X.XXX     X.XXX     X.XXX     X.XXX      300
Room leave (unsubscribe) RTT             X.XXX     X.XXX     X.XXX     X.XXX     X.XXX     X.XXX      300

Connections/sec (accept rate): XXX.XX
Fanout delivered / expected: 1.00
```
