package main

import (
	"fmt"
	"log"
)

// parseExchangeType maps the wire type string to our enum.
func parseExchangeType(s string) (exchangeType, error) {
	switch s {
	case "direct":
		return direct, nil
	case "fanout":
		return fanout, nil
	case "topic":
		return topic, nil
	case "headers":
		return headers, nil
	default:
		return 0, fmt.Errorf("unknown exchange type %q", s)
	}
}

// addExchange registers an exchange, creating it if absent (idempotent, like
// addQueue). Returns the existing or newly-created exchange.
func addExchange(name string, typ exchangeType) *exchange {
	exReg.mu.Lock()
	defer exReg.mu.Unlock()

	if ex, ok := exReg.exchanges[name]; ok {
		return ex
	}
	ex := &exchange{name: name, typ: typ}
	exReg.exchanges[name] = ex
	return ex
}

// handleExchangeDeclare parses exchange.declare (40/10) and registers the exchange.
//
// Fields, in order:
//	reserved-1  short     (skip)          -> offset 6 after class/method
//	exchange    shortstr  -> the name
//	type        shortstr  -> "direct"/"fanout"/"topic"/"headers"
//	passive, durable, auto-delete, internal, no-wait -> 5 bits (1 byte, ignore)
//	arguments   table     (ignore)
func handleExchangeDeclare(c *connection, ch uint16, payload []byte) error {
	name, off := readShortStr(payload, 6)
	typeStr, _ := readShortStr(payload, off)

	typ, err := parseExchangeType(typeStr)
	if err != nil {
		return err
	}
	addExchange(name, typ)
	log.Printf("exchange %q declared (type %s)", name, typeStr)

	return sendExchangeDeclareOk(c, ch)
}

// sendExchangeDeclareOk replies to exchange.declare. It has no fields.
func sendExchangeDeclareOk(c *connection, ch uint16) error {
	payload := methodPayload(classExchange, methodExchangeDeclareOk, nil)
	return c.writeFrame(frameMethod, ch, payload)
}

// addBinding appends a binding to an exchange (under the exchange's lock).
func addBinding(exchangeName string, b binding) error {
	exReg.mu.RLock()
	ex := exReg.exchanges[exchangeName]
	exReg.mu.RUnlock()
	if ex == nil {
		return fmt.Errorf("bind to unknown exchange %q", exchangeName)
	}

	ex.mu.Lock()
	ex.bindings = append(ex.bindings, b)
	ex.mu.Unlock()
	return nil
}

// handleQueueBind parses queue.bind (50/20) and records the binding on the exchange.
//
// Fields, in order:
//	reserved-1   short     (skip)   -> offset 6
//	queue        shortstr  -> the queue to bind
//	exchange     shortstr  -> which exchange
//	routing-key  shortstr  -> binding key/pattern (direct/topic); ignored by fanout
//	no-wait      bit       (skip)
//	arguments    table     -> headers-exchange match criteria (TODO: parse for headers)
func handleQueueBind(c *connection, ch uint16, payload []byte) error {
	queueName, off := readShortStr(payload, 6)
	exchangeName, off := readShortStr(payload, off)
	routingKey, _ := readShortStr(payload, off)

	// TODO (headers exchange): parse the `arguments` table for x-match + header
	// pairs and fill b.matchAll / b.headers. For now bindings carry the routing
	// key/pattern, which covers direct/fanout/topic.
	b := binding{qName: queueName, routingKey: routingKey}
	if err := addBinding(exchangeName, b); err != nil {
		return err
	}
	log.Printf("bound queue %q to exchange %q with key %q", queueName, exchangeName, routingKey)

	return sendQueueBindOk(c, ch)
}

// sendQueueBindOk replies to queue.bind. It has no fields.
func sendQueueBindOk(c *connection, ch uint16) error {
	payload := methodPayload(classQueue, methodQueueBindOk, nil)
	return c.writeFrame(frameMethod, ch, payload)
}
