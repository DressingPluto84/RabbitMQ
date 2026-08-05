package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
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
//	no-local, no-ack, exclusive, no-wait -> 4 bits (1 byte, ignore for v1)
//	arguments     table     (ignore)
func handleBasicConsume(conn net.Conn, ch uint16, payload []byte) error {
	// Parse queue name and requested consumer-tag (offset 6 = past class/method/reserved).
	queueName, off := readShortStr(payload, 6)
	tag, _ := readShortStr(payload, off)

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

	// ---------------------------------------------------------------------
	// TODO (you): register the consumer.
	c := consumer{
		tag: tag,
		ch: ch,
		conn: conn,
	}

	q.mu.Lock()
	q.consumers = append(q.consumers, c)
	q.mu.Unlock()

	// Reply consume-ok so the client's ch.Consume() call returns.
	if err := sendBasicConsumeOk(conn, ch, tag); err != nil {
		return err
	}

	// ---------------------------------------------------------------------
	// TODO (you): drain messages already in the queue to this consumer.
	for {
		q.mu.Lock()
		if len(q.messages) == 0 {
			q.mu.Unlock()
			break
		}

		m := q.messages[0]
		q.messages = q.messages[1:]
		q.mu.Unlock()

		if err := deliverMessage(c, m); err != nil {
			return err
		}
	}

	return nil
}

// sendBasicConsumeOk replies to basic.consume. One field: consumer-tag (shortstr).
func sendBasicConsumeOk(conn net.Conn, ch uint16, tag string) error {
	args := new(bytes.Buffer)
	writeShortStr(args, tag)
	body := methodPayload(classBasic, methodBasicConsumeOk, args.Bytes())
	return writeFrame(conn, frameMethod, ch, body)
}

// deliverMessage pushes one message to a consumer as the 3-frame basic.deliver
// sequence (method + content header + body) -- the mirror of basic.publish.
//
// NOTE: for the single-connection test there's no write race. When you later
// run send.go/receive.go as two processes, a producer goroutine will write
// these frames to a *different* consumer's socket, and you'll want a
// per-connection write mutex around all three writes so they don't interleave.
func deliverMessage(c consumer, m message) error {
	deliveryTag := deliveryCounter.Add(1)
	_ = deliveryTag // TODO: remove once you use deliveryTag in the args below

	// --- Frame 1: basic.deliver METHOD frame (60/60) ---
	// Fields in order: consumer-tag(shortstr), delivery-tag(longlong=uint64),
	// redelivered(bit -> 1 byte), exchange(shortstr), routing-key(shortstr).
	args := new(bytes.Buffer)
	writeShortStr(args, c.tag) // consumer tag
	binary.Write(args, binary.BigEndian, uint64(deliveryTag)) //delivery tag
	args.WriteByte(0) // redelivered
	writeShortStr(args, "") // exchange
	writeShortStr(args, m.routingKey) // routing key
	// -----------------------------------------------------------------
	// TODO 
	method := methodPayload(classBasic, methodBasicDeliver, args.Bytes())
	if err := writeFrame(c.conn, frameMethod, c.ch, method); err != nil {
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
	if err := writeFrame(c.conn, frameHeader, c.ch, header.Bytes()); err != nil {
		return err
	}

	// --- Frame 3: BODY frame (type 3) ---
	return writeFrame(c.conn, frameBody, c.ch, m.payload)
}
