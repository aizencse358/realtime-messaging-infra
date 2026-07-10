from prometheus_client import Counter, Gauge, Histogram

# Every metric is labeled by gateway_id so Grafana can break down per
# replica (or sum across all three) — there's no shared process between
# gateways, so each instance only ever reports its own label value.

ws_connections_active = Gauge(
    "ws_connections_active",
    "WebSocket connections currently held open by this gateway instance",
    ["gateway_id"],
)

room_subscriptions_active = Gauge(
    "room_subscriptions_active",
    "Rooms this gateway instance is currently subscribed to in Redis",
    ["gateway_id"],
)

room_joins_total = Counter(
    "room_joins_total", "Room join operations handled", ["gateway_id"]
)
room_leaves_total = Counter(
    "room_leaves_total", "Room leave operations handled", ["gateway_id"]
)
room_subscribes_total = Counter(
    "room_subscribes_total",
    "Redis SUBSCRIBE calls (first local member joined a room)",
    ["gateway_id"],
)
room_unsubscribes_total = Counter(
    "room_unsubscribes_total",
    "Redis UNSUBSCRIBE calls (last local member left a room)",
    ["gateway_id"],
)

messages_sent_total = Counter(
    "messages_sent_total", "Messages published to a room channel", ["gateway_id"]
)
messages_delivered_total = Counter(
    "messages_delivered_total",
    "Messages delivered to a locally-connected WebSocket",
    ["gateway_id"],
)

message_persist_seconds = Histogram(
    "message_persist_duration_seconds",
    "Time to persist a sent message to DynamoDB",
    ["gateway_id"],
)

http_request_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "path", "status"],
)
