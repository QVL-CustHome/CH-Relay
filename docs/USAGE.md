# Using the Relay bus

Relay is a self-hosted **MQTT 5.0** message broker. It is the communication bus
of the QVL-ToolBox: any service — in any language — talks to it with an
**off-the-shelf MQTT client library**. There is no Relay SDK and no custom wire
format; you speak standard MQTT 5.

This document is written to be understood by **a human integrator** and by **an
AI agent** that needs to wire a service onto the bus. If you are an AI, jump to
[For AI agents — cheat sheet](#for-ai-agents--cheat-sheet) for a compact spec,
then come back here for detail.

---

## 1. Endpoints

| Transport | Default address | Use it for | Enabled |
|---|---|---|---|
| **TCP** (`mqtt://`) | `0.0.0.0:1883` | backends: Rust, Go, Java, Node, Python… | always |
| **WebSocket** (`ws://`) | `0.0.0.0:8083` | browsers and mobile (subprotocol `mqtt`) | always |
| **TLS** (`mqtts://`) | `0.0.0.0:8883` | encrypted native connections | when `tls_cert` + `tls_key` are set |
| **HTTP dashboard** | `127.0.0.1:8080` (example) | monitoring (`/`, `/stats`) | when `http_addr` is set |

- `1883` is the IANA MQTT port, `8883` the MQTT-over-TLS port, `8083` the de-facto
  MQTT-over-WebSocket port.
- The WebSocket listener accepts the upgrade on any path (e.g. `ws://host:8083` or
  `ws://host:8083/mqtt`) and answers the `mqtt` subprotocol.
- **Authentication is optional (opt-in).** With no `[auth]` section the broker
  accepts any client (legacy). With `[auth]` configured, every CONNECT must carry
  a valid **JWT as the MQTT password** and is then restricted by a per-role topic
  **ACL** — see [§11 Authentication & ACL](#11-authentication--acl). Either way,
  for `wss://` terminate TLS at a proxy or use the native `mqtts` listener.

---

## 2. Quick start

### Command line (mosquitto clients)

```bash
# Subscribe (terminal 1)
mosquitto_sub -h 127.0.0.1 -p 1883 -V mqttv5 -t 'sensors/#' -q 1

# Publish (terminal 2)
mosquitto_pub -h 127.0.0.1 -p 1883 -V mqttv5 -t 'sensors/eu/temp' -m '21.4' -q 1
```

### Node / TypeScript (mqtt.js)

```ts
import mqtt from "mqtt";

const client = mqtt.connect("mqtt://127.0.0.1:1883", {
  protocolVersion: 5,
  clientId: "billing-service",
  clean: false,                      // resume a durable session on reconnect
  properties: { sessionExpiryInterval: 3600 }, // keep session 1h while offline
});

client.on("connect", () => {
  client.subscribe("orders/+/created", { qos: 1 });
  client.publish("orders/42/created", JSON.stringify({ total: 9.99 }), { qos: 1 });
});

client.on("message", (topic, payload, packet) => {
  console.log(topic, payload.toString(), packet.properties?.userProperties);
});
```

### Browser (mqtt.js over WebSocket)

```js
const client = mqtt.connect("ws://127.0.0.1:8083", { protocolVersion: 5 });
```

### Python (paho-mqtt v2)

```python
import paho.mqtt.client as mqtt

c = mqtt.Client(client_id="reporting", protocol=mqtt.MQTTv5)
c.connect("127.0.0.1", 1883)
c.subscribe("orders/+/created", qos=1)
c.on_message = lambda cl, ud, msg: print(msg.topic, msg.payload)
c.loop_forever()
```

### Rust (rumqttc)

```rust
use rumqttc::v5::{MqttOptions, AsyncClient};
use rumqttc::v5::mqttbytes::QoS;

let mut opts = MqttOptions::new("inventory", "127.0.0.1", 1883);
let (client, mut eventloop) = AsyncClient::new(opts, 10);
client.subscribe("orders/+/created", QoS::AtLeastOnce).await?;
client.publish("orders/42/created", QoS::AtLeastOnce, false, b"...".to_vec()).await?;
```

### Go (paho.golang) / Java (HiveMQ or Eclipse Paho)

Use any MQTT 5 client pointed at `tcp://127.0.0.1:1883`. Nothing Relay-specific is
required to connect.

---

## 3. MQTT concepts as Relay applies them

### Topics and wildcards

- A **topic name** (what you publish to) has levels separated by `/`:
  `orders/eu/created`. No wildcards allowed in a publish topic.
- A **topic filter** (what you subscribe with) may use wildcards:
  - `+` — exactly one level: `orders/+/created` matches `orders/eu/created`.
  - `#` — the rest of the tree, must be last: `orders/#` matches `orders` and
    `orders/eu/created`.
- **`$`-topics are shielded**: a filter starting with `+` or `#` does **not** match
  topics beginning with `$` (e.g. `$dlq/...`). To watch those, subscribe to them
  explicitly (`$dlq/#`).

### Quality of Service (QoS)

| QoS | Guarantee |
|---|---|
| 0 | at most once (fire and forget) |
| 1 | at least once (PUBACK; may duplicate) |
| 2 | exactly once (PUBREC/PUBREL/PUBCOMP) |

The broker grants up to QoS 2. Effective delivery QoS = `min(publish QoS, granted
QoS)`. Unacknowledged QoS 1/2 messages are redelivered automatically (see
[Retry & dead-letter](#5-dead-letter-queue-dlq)).

### Retained messages

Publish with the **retain** flag to store the *last value* of a topic. New
subscribers receive it immediately on subscribe. Publish an **empty payload with
retain** to clear it.

```ts
client.publish("config/feature-x", "on", { qos: 1, retain: true });
```

### Will (Last Will & Testament)

Set a Will at connect; the broker publishes it if the connection drops
**abnormally** (a clean `DISCONNECT` discards it). Use it for dead-service
detection.

```ts
mqtt.connect("mqtt://127.0.0.1:1883", {
  protocolVersion: 5,
  will: { topic: "presence/billing", payload: "down", qos: 1, retain: true },
});
```

### Sessions (durable vs clean)

- `clean: false` (clean start = false) **+** a non-zero `sessionExpiryInterval`
  makes the session **durable**: its subscriptions and unacknowledged QoS 1/2
  messages are kept while offline and resumed/retransmitted on reconnect with the
  **same `clientId`**. The CONNACK reports `sessionPresent: true` on resume.
- `clean: true` starts fresh and discards any previous session.
- An **empty `clientId`** is given a unique, forced-clean identity — fine for
  pure publishers, never for a durable consumer.
- **Persistence across a broker restart** requires the server to run with
  `data_dir` set. Then durable sessions, their subscriptions, retained messages
  and in-flight queues all survive a restart.

### Shared subscriptions — the work queue

Subscribe to `$share/{group}/{filter}` to make a **competing-consumers queue**:
each matching message goes to **exactly one** member of the group (round-robin),
instead of fanning out to all. Scale a worker pool by giving every worker the same
group.

```ts
// Three workers, same group → each job handled once.
client.subscribe("$share/workers/jobs/+", { qos: 1 });
```

Ordinary (non-`$share`) subscriptions still fan out to every subscriber.

---

## 4. Topic conventions (recommended)

Relay does not impose a topic scheme, but a consistent one keeps the bus legible:

```
{domain}/{entity}/{event}          orders/42/created
{service}/presence                 billing/presence
config/{key}                       config/feature-x      (retained)
$share/{group}/{filter}            $share/workers/jobs/+ (work queue)
$dlq/{client}/{topic}              $dlq/billing/orders/42/created (read-only, broker-produced)
$replay/{from}/{filter}            $replay/0/orders/#    (control, you publish)
```

`$dlq/` and `$replay/` are **reserved** Relay namespaces — see below.

---

## 5. Dead-letter queue (DLQ)

A QoS 1/2 message that cannot be delivered is **redelivered with exponential
back-off** (`retry_base_secs`, doubling, capped at `retry_max_secs`). After
`max_delivery_attempts` failed attempts — or when a durable session expires while
still holding undelivered messages — the message is **dead-lettered**:

- **Republished** on `$dlq/{client_id}/{original_topic}` (so you can watch the
  dead-letter stream live), and
- **Persisted** on disk (when `data_dir` is set) for later inspection.

Consume the dead-letter stream by subscribing to the reserved namespace:

```ts
client.subscribe("$dlq/#", { qos: 0 });
client.on("message", (topic, payload) => {
  // topic = "$dlq/{client}/{original topic}"; payload = original payload
});
```

Notes:
- Dead-letter republishes are **never themselves dead-lettered** (no recursion).
- A QoS 2 message already confirmed received (past PUBREC) is considered delivered
  and is not dead-lettered.

---

## 6. Event log & replay (event sourcing)

When the server runs with `data_dir` set and `event_log_max > 0`, **every
published application message** (any non-`$` topic) is appended to a durable log
with a **global, monotonically increasing offset**. The log is bounded: the oldest
events are pruned beyond `event_log_max`.

### Requesting a replay

Publish a control message to:

```
$replay/{from_offset}/{topic_filter}
```

The broker streams back, **to your own session**, every logged event with
`offset >= from_offset` whose topic matches `{topic_filter}`. Each replayed
message:

- arrives on its **original topic** (not on the `$replay/...` topic),
- is sent at **QoS 0**,
- carries a user property **`x-replay-offset`** = the event's offset.

```ts
// Replay everything under orders/ from the beginning.
client.publish("$replay/0/orders/#", "");

client.on("message", (topic, payload, packet) => {
  const offset = packet.properties?.userProperties?.["x-replay-offset"];
  // resume your processing from `offset + 1` next time
});
```

Use it for catch-up after downtime, rebuilding state, or auditing.

Caveats:
- Send the replay request at **QoS 0**. (At QoS 1 the request's PUBACK can arrive
  before the replayed stream, which is harmless but confusing.)
- The filter is carried *in the topic*, so a wildcard replay request contains `+`
  or `#` in a PUBLISH topic. Most client libraries allow this; a few strict ones
  reject wildcards in publish topics — if so, replay per exact topic, or use a
  library that permits it.
- You receive replayed events on your session whether or not you are also
  subscribed to the topic; subscribe normally if you also want *new* messages.

---

## 7. Configuration reference

Server config is TOML (path via `RELAY_CONFIG`, default `config.toml`). All keys
are optional; defaults shown.

```toml
tcp_addr = "0.0.0.0:1883"     # native MQTT listener
ws_addr  = "0.0.0.0:8083"     # MQTT-over-WebSocket listener

# Persistence (off by default). When set, retained messages, durable sessions,
# in-flight queues, dead letters and the event log all live here and survive
# restarts. Single embedded redb file `relay.redb` is created inside.
# data_dir = "/var/lib/relay"   # or 'C:\relay-data' (TOML literal for Windows)

# Redelivery / dead-letter policy
max_delivery_attempts = 5      # attempts before dead-lettering (1 = no retry)
retry_base_secs       = 5      # back-off base (doubles each attempt)
retry_max_secs        = 60     # back-off cap

# Event log / replay (requires data_dir). 0 disables it.
event_log_max = 100000         # max events kept; oldest pruned beyond this

# TLS (mqtts). Enabled only when both cert and key are set.
# tls_addr = "0.0.0.0:8883"
# tls_cert = "/etc/relay/cert.pem"
# tls_key  = "/etc/relay/key.pem"

# Embedded monitoring dashboard (off by default).
# http_addr = "127.0.0.1:8080"
```

---

## 8. Monitoring dashboard

When `http_addr` is set, the broker serves a tiny built-in dashboard (no separate
service):

- **`GET /`** — a live HTML page that polls the stats every 2 s.
- **`GET /stats`** — a JSON snapshot:

```json
{
  "clients_online": 3,
  "clients_total": 5,
  "subscriptions": 12,
  "retained": 4,
  "dead_letters": 0,
  "events": 1280,
  "next_offset": 1280
}
```

`/stats` is convenient for scraping into your own monitoring.

---

## 9. Patterns / recipes

| Need | How |
|---|---|
| **Pub/sub fan-out** | Plain topics; every subscriber to a matching filter gets a copy. |
| **Work queue** (one worker per job) | All workers `subscribe("$share/group/filter")`. |
| **Last known value** | Publish with `retain: true`; late subscribers get it on subscribe. |
| **Service presence / health** | Retained presence topic + a Will to flip it to "down". |
| **Reliable delivery** | QoS 1 (or 2) + durable session (`clean:false`, expiry > 0). |
| **Request / reply** | Reply-to topic (e.g. `rpc/{service}/reply/{id}`) the caller subscribes to; use MQTT 5 `responseTopic`/`correlationData` properties. |
| **Catch-up after downtime** | Replay from your last `x-replay-offset` via `$replay/{offset}/{filter}`. |
| **Handle poison messages** | Watch `$dlq/#`; alert or reprocess dead letters. |

---

## 10. Limits & current scope

- **Authentication is opt-in** — without `[auth]` the broker accepts any client;
  with it, a valid JWT is required and a topic ACL applies (see §11). Still secure
  the network and use TLS for transport encryption.
- **WebSocket is plain (`ws://`)** — terminate TLS at a reverse proxy for `wss://`
  (native TLS is available on the TCP listener via `mqtts`).
- **Inbound QoS 2 dedup state is not persisted** — an in-progress *inbound* QoS 2
  handshake is lost on a broker restart (outbound in-flight queues *are*
  persisted).
- **No replay API for dead letters** yet — they are persisted but not yet
  exposed for programmatic re-injection.
- Replay filters travel in the PUBLISH topic (wildcard caveat in §6).

---

## 11. Authentication & ACL

Auth is **opt-in and generic**. With no `[auth]` block the broker is open. With
one, the broker enforces JWT auth + a topic ACL — configurable, not tied to any
project.

### Connecting with a token

Send the **JWT as the MQTT password** (the username is ignored). The broker
verifies it (HS256, shared `jwt_secret`) and checks expiry.

```ts
import mqtt from "mqtt";
const client = mqtt.connect("mqtt://127.0.0.1:1883", {
  protocolVersion: 5,
  clientId: "billing-service",
  password: MY_JWT,            // <-- the token goes here
  // username: optional, ignored by the broker
});
```

A missing, malformed, wrong-signature or expired token gets a CONNACK with
reason **Not authorized (0x87)** and the connection is closed.

### How the ACL is built

The broker reads two claims (names configurable): the **identity** (`sub` by
default) and the **roles** (`roles`, a JSON array). It then unions every ACL rule
whose `role` matches one of the client's roles (`role = "*"` matches everyone),
substituting `{claim}` placeholders from the token:

```toml
[auth]
jwt_secret     = "…shared secret…"
identity_claim = "sub"      # default
roles_claim    = "roles"    # default

# Drive users: only their own subtree (requires the `drive` role).
[[auth.acl]]
role      = "drive"
publish   = ["drive/{sub}/#"]
subscribe = ["drive/{sub}/#"]

# Drive service backend: publish-only on upload event topics, no subtree access.
[[auth.acl]]
role      = "drive_service"
publish   = ["users/+/files/+/uploaded"]
subscribe = []

# Admins: the whole tree + dead letters.
[[auth.acl]]
role      = "drive_admin"
publish   = ["drive/#"]
subscribe = ["drive/#", "$dlq/#"]
```

Rules:
- **Publish** to `T` is allowed if some `publish` pattern matches `T`.
- **Subscribe** with filter `F` is allowed only if some `subscribe` pattern
  *subsumes* `F` — every topic `F` could match must also be covered by the
  pattern. So a client granted `drive/{sub}/#` **cannot** subscribe to `drive/#`.
- **Shared** subscriptions (`$share/g/F`) are checked on the inner `F`.
- A **`$replay/{from}/{filter}`** request is checked against the *subscribe* ACL
  for `{filter}` (replay is a read).
- A pattern that references a claim the token doesn't have grants nothing
  (fail closed).

A denied subscribe returns SUBACK **Not authorized** for that filter; a denied
publish returns PUBACK/PUBREC **Not authorized** (QoS 1/2) or is dropped (QoS 0).

---

## For AI agents — cheat sheet

> Goal: connect a service to the Relay MQTT 5.0 bus and use it correctly. Use any
> MQTT 5 client library; do not write a custom protocol.

**Connect**
- TCP: `mqtt://{host}:1883` · WebSocket: `ws://{host}:8083` (subprotocol `mqtt`) ·
  TLS: `mqtts://{host}:8883` (if enabled).
- Always set `protocolVersion = 5`.
- Durable consumer: stable `clientId`, `clean=false`, `sessionExpiryInterval>0`.
  Ephemeral publisher: `clean=true` (empty clientId is acceptable).
- Auth: if the broker has `[auth]`, send your **JWT as the MQTT password**
  (username ignored); else no credentials are required. A bad/expired token →
  CONNACK Not authorized. Your topic ACL is derived from the token's roles +
  claims (e.g. you may be limited to `drive/{sub}/#`); see §11.

**Publish / subscribe**
- Publish to a concrete topic `a/b/c` (no wildcards). Subscribe with filters using
  `+` (one level) and `#` (rest; last only).
- QoS 0/1/2 supported; pick 1 for reliable delivery, pair with a durable session.
- Retain: publish with `retain=true` to set a topic's last value; empty payload
  + retain clears it.

**Reserved namespaces (exact strings)**
- Work queue: subscribe `"$share/{group}/{filter}"` → competing consumers,
  round-robin, one delivery per message per group.
- Dead letters: subscribe `"$dlq/#"`. Failed/expired messages are republished as
  `"$dlq/{clientId}/{originalTopic}"` with the original payload. Read-only.
- Replay: publish (QoS 0, empty payload) to `"$replay/{fromOffset}/{filter}"`. The
  broker streams matching logged events (offset ≥ fromOffset) to your session, on
  their original topics, each with user property `"x-replay-offset"` = its offset.
  Persist the last offset to resume later.

**Behavioral facts**
- Effective QoS = min(publish QoS, granted QoS); max granted = 2.
- Wildcard filters (`+`/`#` at root) do NOT match `$`-topics; subscribe to `$dlq/#`
  explicitly to see dead letters.
- Unacked QoS 1/2 messages are auto-retried with back-off; after
  `max_delivery_attempts` they are dead-lettered.
- Persistence of sessions/retained/events requires the server to have `data_dir`
  set; behavior is otherwise in-memory.

**Health check**
- If a dashboard is enabled: `GET http://{host}:{http_addr}/stats` → JSON with
  `clients_online, clients_total, subscriptions, retained, dead_letters, events,
  next_offset`.

**Minimal Node/TS recipe**
```ts
import mqtt from "mqtt";
const c = mqtt.connect("mqtt://HOST:1883", {
  protocolVersion: 5, clientId: "my-service", clean: false,
  properties: { sessionExpiryInterval: 3600 },
});
c.on("connect", () => c.subscribe("my/topic/#", { qos: 1 }));
c.on("message", (t, p) => {/* handle */});
c.publish("my/topic/event", JSON.stringify({}), { qos: 1 });
```
