package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync/atomic"
)

// Monotonic counters for auto-generated consumer tags and delivery tags.
var consumerCounter atomic.Uint64
var deliveryCounter atomic.Uint64

// handleBasicConsume registers a consumer on a queue, replies consume-ok, and
// delivers any messages already waiting in the queue.
//
// basic.consume (60/20) fields, in order:
//	reserved-1    short     (skip)
//	queue         shortstr  -> which queue to subscribe to
//	consumer-tag  shortstr  -> client's tag, or "" (we generate one)
//	no-local, no-ack, exclusive, no-wait -> 4 bits
//	arguments     table     (ignore)
func handleBasicConsume(c *connection, ch uint16, payload []byte) error {
	// Parse queue name and requested consumer-tag (offset 6 = past class/method/reserved).
	queueName, off := readShortStr(payload, 6)
	tag, off := readShortStr(payload, off)
	opts := payload[off]

	ack := false
	if opts & 0x02 == 0 { // bit fields are numbered backwards so not 0x04
		ack = true
	}

	// If the client didn't name its consumer, generate a tag.
	if tag == "" {
		tag = fmt.Sprintf("ctag-%d", consumerCounter.Add(1))
	}

	// Find the queue.
	reg.mu.RLock()
	q := reg.queues[queueName]
	reg.mu.RUnlock()
	if q == nil {
		return fmt.Errorf("consume on unknown queue %q", queueName)
	}

	// Register the consumer, storing the *connection wrapper so deliver can
	// write to this consumer's socket through its lock.
	cons := consumer{
		tag: tag,
		ch:  ch,
		ack: ack,
		c:   c,
	}

	q.mu.Lock()
	q.consumers = append(q.consumers, cons)
	q.mu.Unlock()

	// Reply consume-ok so the client's ch.Consume() call returns.
	if err := sendBasicConsumeOk(c, ch, tag); err != nil {
		return err
	}

	// Drain messages already in the queue to this consumer: pop under the lock,
	// deliver outside it (deliver does socket I/O).
	if err := drainMessages(q); err != nil {
		return err
	}

	return nil
}

func handleBasicAck(c *connection, ch uint16, payload []byte) error {
	tag := binary.BigEndian.Uint64(payload[4:12]) // domain
	multiple := payload[12] & 1 == 1

	k := keyAck{
		ch: ch,
		tag: tag,
	}

	c.mu.Lock()
	defer c.ackMu.Unlock()
	delete(c.unAck, k)
	if multiple {
		for key := range c.unAck {
			if key.ch == ch && key.tag <= tag {
				delete(c.unAck, key)
			}
		}
	}

	return nil
}

// removeConsumers drops every consumer belonging to connection c from all
// queues. Called on disconnect, before requeuing, so requeued messages only go
// to still-alive consumers.
func removeConsumers(c *connection) {
	reg.mu.RLock()
	queues := make([]*queue, 0, len(reg.queues))
	for _, q := range reg.queues {
		queues = append(queues, q)
	}
	reg.mu.RUnlock()

	for _, q := range queues {
		q.mu.Lock()
		kept := q.consumers[:0]
		for _, cons := range q.consumers {
			if cons.c != c {
				kept = append(kept, cons)
			}
		}
		q.consumers = kept
		if len(q.consumers) == 0 {
			q.next = 0
		} else {
			q.next %= len(q.consumers)
		}
		q.mu.Unlock()
	}
}

func drainMessages(q *queue) error {
	q.mu.Lock()
	val := len(q.consumers)
	q.mu.Unlock()

	if val > 0 {
		for {
			q.mu.Lock()
			if len(q.messages) == 0 {
				q.mu.Unlock()
				break
			}

			m := q.messages[0]
			q.messages = q.messages[1:]
			cons := q.consumers[q.next]
			q.next = (q.next + 1) % len(q.consumers)

			q.mu.Unlock()

			if err := deliverMessage(cons, m, q); err != nil {
				return err
			}
		}
	}

	return nil
}

// sendBasicConsumeOk replies to basic.consume. One field: consumer-tag (shortstr).
func sendBasicConsumeOk(c *connection, ch uint16, tag string) error {
	args := new(bytes.Buffer)
	writeShortStr(args, tag)
	body := methodPayload(classBasic, methodBasicConsumeOk, args.Bytes())
	return c.writeFrame(frameMethod, ch, body)
}

// deliverMessage pushes one message to a consumer as the 3-frame basic.deliver
// sequence (method + content header + body) -- the mirror of basic.publish.
//
// NOTE: for the single-connection test there's no write race. When you later
// run send.go/receive.go as two processes, a producer goroutine will write
// these frames to a *different* consumer's socket, and you'll want a
// per-connection write mutex around all three writes so they don't interleave.
func deliverMessage(cons consumer, m message, q *queue) error {
	deliveryTag := deliveryCounter.Add(1)

	if cons.ack {
		key := keyAck{
			ch: cons.ch,
			tag: deliveryTag,
		}
		value := valueAck{
			m: m,
			q: q,
		}

		cons.c.ackMu.Lock()
		cons.c.unAck[key] = value
		cons.c.ackMu.Unlock()
	}

	// --- Frame 1: basic.deliver METHOD frame (60/60) ---
	// Fields in order: consumer-tag(shortstr), delivery-tag(longlong=uint64),
	// redelivered(bit -> 1 byte), exchange(shortstr), routing-key(shortstr).
	args := new(bytes.Buffer)
	writeShortStr(args, cons.tag)                             // consumer-tag
	binary.Write(args, binary.BigEndian, uint64(deliveryTag)) // delivery-tag
	args.WriteByte(0)                                         // redelivered = false
	writeShortStr(args, "")                                   // exchange (default)
	writeShortStr(args, m.routingKey)                         // routing-key

	method := methodPayload(classBasic, methodBasicDeliver, args.Bytes())
	if err := cons.c.writeFrame(frameMethod, cons.ch, method); err != nil {
		return err
	}

	// --- Frame 2: content HEADER frame (type 2) ---
	// Layout (§4.2.6.1): class-id(short), weight(short=0), body-size(longlong),
	// property-flags(short). Minimal valid header: property-flags = 0 (no props).
	header := new(bytes.Buffer)
	binary.Write(header, binary.BigEndian, uint16(classBasic))    // class-id
	binary.Write(header, binary.BigEndian, uint16(0))             // weight (always 0)
	binary.Write(header, binary.BigEndian, uint64(len(m.payload))) // body-size
	binary.Write(header, binary.BigEndian, uint16(0))             // property-flags: none set
	if err := cons.c.writeFrame(frameHeader, cons.ch, header.Bytes()); err != nil {
		return err
	}

	// --- Frame 3: BODY frame (type 3) ---
	return cons.c.writeFrame(frameBody, cons.ch, m.payload)
}
