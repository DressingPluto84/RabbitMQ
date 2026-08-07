package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"strings"
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

// parseContentHeaders extracts the `headers` property from a content-header
// frame payload (§4.2.6.1). Returns an empty map if none is present.
//
// Layout: class-id(2) weight(2) body-size(8) property-flags(2) property-list.
// Basic-class properties come in a fixed order; `headers` is the 3rd, so if
// content-type / content-encoding are present (bits 15/14) we skip them first.
func parseContentHeaders(payload []byte) (map[string]any, error) {
	if len(payload) < 14 {
		return map[string]any{}, nil
	}
	flags := binary.BigEndian.Uint16(payload[12:14])
	off := 14

	if flags&0x8000 != 0 { // content-type (shortstr)
		_, off = readShortStr(payload, off)
	}
	if flags&0x4000 != 0 { // content-encoding (shortstr)
		_, off = readShortStr(payload, off)
	}
	if flags&0x2000 != 0 { // headers (table)
		h, _, err := readTable(payload, off)
		return h, err
	}
	return map[string]any{}, nil
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
	routingKey, off := readShortStr(payload, off)
	off++ // no-wait bit (1 packed byte)

	// arguments table — for a headers binding it carries "x-match" (all/any) and
	// the header pairs to match. Empty for direct/fanout/topic.
	args, _, err := readTable(payload, off)
	if err != nil {
		return err
	}
	matchAll := false
	if xm, ok := args["x-match"].(string); ok {
		matchAll = xm == "all"
	}
	delete(args, "x-match") // x-match is control, not a header to match on

	b := binding{qName: queueName, routingKey: routingKey, matchAll: matchAll, headers: args}
	if err := addBinding(exchangeName, b); err != nil {
		return err
	}
	log.Printf("bound queue %q to exchange %q (key %q, headers %v)", queueName, exchangeName, routingKey, args)

	return sendQueueBindOk(c, ch)
}

// sendQueueBindOk replies to queue.bind. It has no fields.
func sendQueueBindOk(c *connection, ch uint16) error {
	payload := methodPayload(classQueue, methodQueueBindOk, nil)
	return c.writeFrame(frameMethod, ch, payload)
}

func (ex *exchange) route(routingKey string, msgHeaders map[string]any) []string {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	var res []string
	switch ex.typ {
	case direct:
		for _, bind := range ex.bindings {
			if bind.routingKey == routingKey {
				res = append(res, bind.qName)
			}
		}
	case fanout:
		for _, bind := range ex.bindings {
			res = append(res, bind.qName)
		}
	case topic:
		s := strings.Split(routingKey, ".")
		for _, bind := range ex.bindings {
			p := strings.Split(bind.routingKey, ".")
			if topicDP(s, p) {
				res = append(res, bind.qName)
			}
		}
	case headers:
		for _, bind := range ex.bindings {
			if bind.matchAll && allMatch(bind.headers, msgHeaders) {
				res = append(res, bind.qName)
			} else if !bind.matchAll && partialMatch(bind.headers, msgHeaders) {
				res = append(res, bind.qName)
			}
		}
	}

	return res
}

func topicDP(s []string, p []string) bool {
	dp := make([][]bool, len(s)+1)
	for i := range dp { dp[i] = make([]bool, len(p)+1) }
	dp[len(s)][len(p)] = true

	// checks for # to see if there is nothing after it, as if we reach this pint without
	// the loop then we check out of bounds for the string and get False since we did not
	// check if trailing # matches with empty string which it should
	for j := len(p)-1; j >= 0; j -= 1 {
		if p[j] == "#" {
			dp[len(s)][j] = dp[len(s)][j+1]
		}
	}

	for i := len(s) - 1; i > -1; i -= 1 {
		for j := len(p) - 1; j > -1; j -= 1 {
			if s[i] == p[j] || p[j] == "*" {
				dp[i][j] = dp[i + 1][j + 1]
			} else if p[j] == "#" {
				dp[i][j] = dp[i + 1][j] || dp[i][j + 1]
			}
		}
	}

	return dp[0][0]
}

func partialMatch(m1 map[string]any, m2 map[string]any) bool {
	for k, v := range m1 {
		if val, ok := m2[k]; ok && val == v {
			return true
		}
	}

	return false
}

func allMatch(m1 map[string]any, m2 map[string]any) bool {
	for k, v := range m1 {
		if val, ok := m2[k]; !ok || val != v {
			return false
		}
	}

	return true
}