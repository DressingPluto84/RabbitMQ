# RabbitMQ Mini Clone — Design Notes

A small AMQP-0-9-1 message broker written in Go. Goal is to understand RabbitMQ
at a fundamental level: routing semantics, delivery guarantees, and the wire
protocol — built end-to-end, wire-compatible with the official Go client
(`github.com/rabbitmq/amqp091-go`).

## Scope by version

**v1 — Wire compatibility + default exchange**
- TCP listener on 5672, speak enough AMQP 0-9-1 that `amqp091-go` connects.
- Default (nameless direct) exchange only: a message routes to the queue whose
  name equals the routing key. This is direct routing with the binding hardcoded.
- Milestone: the official Go client publishes a message and a consumer receives it.
- No acks, no durability, no other exchange types yet.

**v2 — Acks + more exchanges**
- Manual consumer acknowledgments + prefetch (QoS). Requeue on consumer disconnect.
- Fanout and topic exchanges + `queue.bind` (topic `*`/`#` matching).
- These are additive: same delivery path, different binding-match functions.

**v3 — Durability (the distributed feature)**
- Durable queues + persistent messages written to a write-ahead log *before*
  the broker confirms them (persist-before-confirm → at-least-once).
- On restart, rebuild queue state by replaying the WAL.
- Verify with a kill-test: publish, `kill -9` mid-flight, restart, check count.
- Acks must land before/with durability, or the guarantee is meaningless.

Build order rationale: wire compatibility is the hard foundation everything
rides on. Exchanges are cheap once it exists (good momentum). Durability is the
finicky one — do it last, against a stable in-memory broker, so we debug one
hard thing at a time.

## v1 detail — what "connect" actually means

The TCP listener is trivial (`net.Listen`). The real work is the handshake:
`amqp.Dial` does NOT return just because the socket opened. The client sends the
protocol header and waits for the broker to drive the negotiation. "Connected"
means the full handshake completed.

Handshake sequence (broker side):
1. Accept TCP connection on 5672.
2. Read + validate the client's protocol header (`AMQP\0\0\9\1`).
3. Send `connection.start` (advertise `PLAIN` auth mechanism).
4. Read `connection.start-ok` — contains credentials; accept but do NOT validate
   (guest/guest "works" because we ignore it, not because we check it).
5. Send `connection.tune` (max channels, max frame size, heartbeat).
6. Read `connection.tune-ok`.
7. Read `connection.open` → send `connection.open-ok`.
8. Now `amqp.Dial` returns a live connection.

Then, per channel: `channel.open` → `channel.open-ok`, and the message path
(`queue.declare`, `basic.publish`, `basic.consume`, `basic.deliver`).

## Things to design carefully (own these; don't just have them generated)

- **Connection/channel state machine** — what state the connection is in at each
  handshake step; per-channel state (consumers; later: unacked messages,
  in-progress message being assembled).
- **Frame assembly** — one logical message = method frame + content-header frame
  + one or more body frames, on the same channel, and channels are interleaved
  on the connection. Need per-channel reassembly state.
- **The routing hop** — message in → find queue by routing key → enqueue.
  Trivial for v1's default exchange, but it's the seam the other exchanges plug
  into later.

Tedious byte-encoding (frame framing, field-table encoding) is fine to delegate.
The state machine and frame assembly are the parts worth designing by hand.

## Tools

- `github.com/rabbitmq/amqp091-go` — the client to test against; read its
  handshake + frame-writing path to see exactly what bytes the broker must speak.
- `github.com/rabbitmq/amqp-0.9.1-spec` — source of truth for each method's
  field order and types.
- Wireshark (has an AMQP dissector) — capture a real RabbitMQ session and diff
  our broker's bytes against it frame-by-frame. Turns cryptic disconnects into a
  visible diff.
