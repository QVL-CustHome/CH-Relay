# Relay

Relay is a self-hosted **MQTT 5.0 message broker** written in Rust. It is the
communication bus of the QVL-ToolBox: your services publish and subscribe over a
**standard protocol**, so any client — Node/TypeScript, Go, Java, Rust, the
browser, or mobile — connects with an **off-the-shelf MQTT library**. No custom
SDK, no reinventing the wire format.

```
┌─────────┐   publish   ┌──────────────────────────┐   deliver   ┌──────────┐
│ Service │ ──────────▶ │   relay (broker daemon)  │ ──────────▶ │ Consumer │
└─────────┘             │  MQTT 5.0 over TCP + WS   │             └──────────┘
└─────────┘
```

## Why MQTT?

- **Callable by anyone** — mature client libraries in every language
  (mqtt.js, paho, rumqtt, HiveMQ, CocoaMQTT…).
- **Browser & mobile** — MQTT-over-WebSocket is standard, so the *same* broker
  serves web frontends and native mobile apps. (Browsers cannot open raw TCP;
  WebSocket is the bridge.)
- **Standard, not bespoke** — you implement a public spec, clients use proven libs.

## Architecture

Two crates (mirrors AIGate's `*-core` / `*-server` split):

- **`relay-core`** — the broker engine: topic matching, subscriptions, retained
  store, sessions, QoS state machine. **No I/O**, fully unit-testable.
- **`relay-server`** — the tokio daemon: TCP / WebSocket / TLS listeners, MQTT
  packet codec, on-disk store, redelivery + dead-letter timer, event log, and an
  embedded monitoring dashboard. Drives `relay-core`. Produces the `relay` binary.

## Run

```bash
cargo run -p relay-server
# relay listening on tcp://0.0.0.0:1883
# relay listening on ws://0.0.0.0:8083
```

Configuration is read from `config.toml` (see `config.toml.example`), overridable
with `RELAY_CONFIG`.

## Roadmap

### V1 — core broker ✅ complete
- [x] Topic filter matching (`+`, `#`) + shared-subscription parsing (`$share/…`)
- [x] Config (TOML) + TCP/WebSocket listeners
- [x] MQTT 5.0 packet codec (`rmqtt-codec`) + handshake: CONNECT→CONNACK, PINGREQ→PINGRESP, DISCONNECT (verified end-to-end)
- [x] Pub/Sub routing + wildcards: SUBSCRIBE→SUBACK, PUBLISH → matching subscribers (QoS 0 fan-out, verified end-to-end)
- [x] UNSUBSCRIBE→UNSUBACK — stops delivery, removes the (persisted) subscription (verified end-to-end)
- [x] **Shared subscriptions** (`$share/group/topic`) — competing consumers / round-robin queue (verified end-to-end)
- [x] **QoS 1** (at-least-once) — PUBACK to publisher + QoS-1 delivery with per-connection packet ids, granted via SUBACK (verified end-to-end)
- [x] **Retained messages** — last value per topic, replayed to late subscribers (retain flag set), cleared by an empty payload (verified end-to-end)
- [x] **Will (LWT)** — published on abnormal disconnect, discarded on a clean DISCONNECT (verified end-to-end)
- [x] **WebSocket transport** — MQTT-over-WS (HTTP upgrade, `mqtt` subprotocol) for browser/mobile, same broker loop as TCP (verified end-to-end)
- [x] **QoS 2** (exactly-once) — full PUBREC/PUBREL/PUBCOMP handshake both ways, retransmit-deduplicated on receipt (verified end-to-end)
- [x] **Sessions** (clean start / session expiry) — per-`client_id` session survives disconnect; `clean_start=false` resumes subscriptions and retransmits unacked QoS 1/2, QoS≥1 messages are queued while offline, `session_present` in CONNACK, expiry purge (verified end-to-end)

> **Codec note:** we use `rmqtt-codec` (from the rmqtt broker project: tokio-util 0.7 / bytes 1.x).
> `mqttbytes` 0.6 was rejected — its v5 CONNACK encoding omits the mandatory property-length byte;
> `mqtt-v5` 0.1 was rejected — it pins the obsolete tokio 0.2 / bytes 0.5 ecosystem.

### V2 — the extras ✅ complete
- [x] On-disk persistence — **retained messages** survive restart (`redb` embedded store, opt-in via `data_dir`, verified end-to-end)
- [x] On-disk persistence — **durable sessions**: a `clean_start=false` client's identity + subscriptions survive a restart (`session_present` after reload, verified end-to-end)
- [x] On-disk persistence — **in-flight QoS 1/2 queues**: unacknowledged outbound messages (including those queued while a durable client is offline) survive a restart and are retransmitted on reconnect (verified end-to-end)
- [x] **Dead-letter queue + retry with back-off** — unacknowledged QoS 1/2 messages are redelivered with exponential back-off; after `max_delivery_attempts` (or when a durable session expires undelivered) they are republished on `$dlq/{client}/{topic}` and persisted for replay (verified end-to-end)
- [x] **Replay / event-sourcing from an offset** — every published message is journalled with a global offset (bounded log); a client replays from an offset by publishing `$replay/{from}/{filter}`, receiving the matching events tagged with their offset (verified end-to-end)
- [x] **TLS (mqtts)** — optional secure listener (rustls/`ring`), enabled by pointing `tls_cert` + `tls_key` at PEM files; same broker loop over the TLS stream (verified end-to-end against a self-signed cert)
- [x] **Embedded monitoring dashboard** — a built-in HTTP endpoint (`http_addr`, no separate service): `/` serves a live page, `/stats` a JSON snapshot (clients, subscriptions, retained, dead-letters, event-log size/offset) (verified end-to-end)

## Feature mapping (what MQTT 5 gives us out of the box)

| Need | MQTT 5.0 mechanism |
|---|---|
| Work queue (competing consumers) | Shared subscriptions `$share/group/topic` |
| Pub/Sub fan-out | Topics + wildcards `+` / `#` |
| Delivery guarantees | QoS 0 / 1 / 2 |
| Last known value | Retained messages |
| Dead service detection | Will message (LWT) |
| Message TTL | Message Expiry Interval |
| Undeliverable messages | Dead-letter queue (`$dlq/#`) + retry with back-off *(Relay extension)* |
| Event replay / catch-up | Offset-based event log via `$replay/{from}/{filter}` *(Relay extension)* |
