# Next portion — from a live connection to a delivered message

The connection handshake is done: the real `amqp091-go` client completes
`amqp.Dial`. This doc sets up the next portion so you can dive straight in.

## TL;DR of where you are

Run `make test` from the repo root any time. Right now it prints:

```
✓ step 1: amqp.Dial (connection handshake)
✗ step 2 (conn.Channel) FAILED: "channel/connection is not open"
```

Step 1 passes. Step 2 fails **not because channel.open is wrong** — you haven't
written it yet — but because of a structural thing explained next.

---

## The one thing to fix first: `handleConn` must not return

Look at the end of `handleConn` in `main.go`. After `sendConnectionOpenOk` it
**returns**, and `defer conn.Close()` slams the socket shut. `amqp.Dial` already
got its `open-ok` so it reports success — but the moment the client sends the
next frame (`channel.open`), the connection is gone. Hence "channel/connection
is not open."

So the handshake was a fixed, scripted sequence. Everything after it is a
**loop**: the connection is now open and the client sends frames whenever it
wants. Your next structural move is to replace the straight-line handshake tail
with a **read loop + dispatch**:

```
handshake (steps 1–7)          // as now — scripted
for {
    frameType, channel, payload := readFrame(conn)   // block until a frame arrives
    classID, methodID := payload[0:2], payload[2:4]
    switch {
        case classID==20 && methodID==10:  // channel.open   -> reply channel.open-ok
        case classID==50 && methodID==10:  // queue.declare  -> reply queue.declare-ok
        case classID==60 && methodID==40:  // basic.publish  -> read the content frames, route
        case classID==60 && methodID==20:  // basic.consume  -> reply consume-ok, start delivering
        ...
    }
}
```

This loop **is** the connection state machine DESIGN.md told you to own. The
`switch` is the routing seam everything plugs into.

---

## The method map for this portion

Class IDs: **channel = 20**, **queue = 50**, **basic = 60**. (connection was 10.)
All of these ride on a **non-zero channel** (unlike the handshake, which was
channel 0). That's your first taste of per-channel state.

### channel.open / open-ok  (the warm-up — just like the connection handshake)
| method | class/method | fields |
|---|---|---|
| `channel.open`    | 20 / 10 | `reserved-1` (shortstr) — ignore |
| `channel.open-ok` | 20 / 11 | `reserved-1` (longstr) — send empty |

Read one, send the other, **on the same channel number the client used** (not 0).
Nearly identical to what you did for `connection.open` → `open-ok`.

### queue.declare / declare-ok
| method | class/method | fields |
|---|---|---|
| `queue.declare` | 50 / 10 | `reserved-1` (short), `queue` (shortstr), then **5 bits** (passive, durable, exclusive, auto-delete, no-wait), `arguments` (table) |
| `queue.declare-ok` | 50 / 11 | `queue` (shortstr), `message-count` (long/uint32), `consumer-count` (long/uint32) |

To make v1 work you just need to: parse the queue name, create an in-memory
queue with that name if it doesn't exist, and reply `declare-ok` echoing the
name with counts = 0. **New encoding wrinkle: bit packing** — those 5 bits are
packed into a *single octet* (bit 0 = passive, bit 1 = durable, …). See §4.2.5.2.

### basic.publish  (the hard one — this is the reassembly DESIGN.md warned about)
`basic.publish` is `content="1"`, meaning **one logical message = 3 frames** in
sequence on the same channel:

```
[METHOD frame  type=1]  basic.publish (60/40): reserved-1(short), exchange(shortstr),
                        routing-key(shortstr), then 2 bits (mandatory, immediate)
[HEADER frame  type=2]  content header: class-id(2), weight(2)=0, body-size(8, uint64),
                        property-flags(2), property-list
[BODY frame(s) type=3]  the raw message bytes (may be split across several frames)
```

Your read loop must recognize that after a `basic.publish` method frame, the
**next** frame is a type-2 header and then type-3 body frame(s) — and assemble
them into one message before routing. This is the per-channel reassembly state:
"I'm mid-message on channel N, expecting a header, then N body bytes."

For v1 (default exchange), routing is trivial: `exchange` is `""`, and you route
to the queue whose name == `routing-key`. Enqueue the assembled body there.

The **content header** is the fiddliest encoding in the whole project. The
property-flags are a 16-bit field where each set bit means "this property is
present, in order" (bit 15 = content-type, bit 14 = content-encoding, …). For
v1 you mostly need to *read past* it correctly, not produce it — until deliver.
Full detail: §4.2.6.1. The 14 possible properties (content-type, delivery-mode,
etc.) are the `basic` class fields in the XML spec.

### basic.consume / consume-ok, then basic.deliver
| method | class/method | fields |
|---|---|---|
| `basic.consume` | 60 / 20 | `reserved-1`(short), `queue`(shortstr), `consumer-tag`(shortstr), 4 bits (no-local, no-ack, exclusive, no-wait), `arguments`(table) |
| `basic.consume-ok` | 60 / 21 | `consumer-tag` (shortstr) — echo/generate one |
| `basic.deliver` | 60 / 60 (content=1) | `consumer-tag`(shortstr), `delivery-tag`(longlong/uint64), `redelivered`(bit), `exchange`(shortstr), `routing-key`(shortstr) — **followed by header + body frames**, same 3-frame shape as publish |

`basic.consume` registers a consumer on a queue (per-channel state: "channel N
wants messages from queue Q, tag T"). Then the **broker pushes** `basic.deliver`
to the client whenever a message is available — and deliver carries content, so
you're now *producing* the 3-frame sequence you learned to *parse* in publish.
That symmetry is the point: once you can read content framing, writing it is the
mirror.

---

## What you own vs. what to delegate (per DESIGN.md)

**Own (the understanding lives here):**
- The read loop + dispatch switch (the state machine).
- Per-channel reassembly state for content (method → header → body).
- The routing hop: assembled message → find queue by routing key → enqueue.

**Fine to delegate (tedious bytes):**
- Bit-packing/unpacking helpers (§4.2.5.2).
- Content-header property-flags encode/decode (§4.2.6.1).
- The `readShortStr` / `readLongStr` / `readTable` parse helpers (mirrors of the
  write helpers already in `encoding.go`).

## Reading list, in priority order

1. **`amqp091-go/channel.go`** — the client half of everything in this portion.
   Its `Channel()`, `QueueDeclare`, `PublishWithContext`, `Consume` show exactly
   what frames it sends and what replies it blocks on. This is the single most
   useful file to skim before starting.
2. **`amqp091-go/read.go` and `write.go`** — how the client parses/emits content
   headers and body frames. Copy the shape of its property-flags handling.
3. **Spec §4.2.5.2 (bits), §4.2.6 (content framing), §4.2.6.1 (content header)** —
   the two encodings you haven't hit yet.
4. **The XML spec** (`spec/amqp0-9-1.stripped.extended.xml` in the module cache)
   for the exact field lists of any method above.

## Suggested order of attack

1. Turn the handshake tail into the read loop (fixes step 2's structural block).
2. `channel.open` → `open-ok` on the client's channel. (test step 2 goes green)
3. `queue.declare` → `declare-ok` + in-memory queue map. (step 3 green)
4. `basic.publish`: parse the 3-frame sequence, route to queue. (step 4 green)
5. `basic.consume` + `basic.deliver`: push the message back out. (steps 5–6 green)

Each of these makes one more line of `make test` turn green — that's your
scoreboard.
