package main

import (
	"sync"
	"net"
)

type message struct {
	payload []byte
	routingKey string
}

type connection struct {
	conn net.Conn
	mu sync.Mutex
}

type consumer struct {
	tag string // tag into the channel in case of multi consumer
	ch uint16
	c *connection
}

type queue struct {
	mu sync.Mutex
	name string
	messages []message 
	consumers []consumer
	next int // round robin choice
}

type registry struct {
	mu sync.RWMutex
	queues map[string]*queue
}

type exchangeType int
const ( direct exchangeType = iota; fanout; topic; headers )

type binding struct {
	qName string
	routingKey string
	matchAll bool
	headers map[string]any
}

type exchange struct {
	name string
	typ exchangeType
	bindings []binding
	mu sync.Mutex
}

type exchangeRegistry struct {
	mu        sync.RWMutex
	exchanges map[string]*exchange
}

func addQueue(reg *registry, name string) *queue {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	if q, ok := reg.queues[name]; ok {
		return q
	}

	q := &queue{
		name: name,
		messages: make([]message, 0),
		consumers: make([]consumer, 0),
		next: 0,
	}
	reg.queues[name] = q

	return q 
}

func addMessage(reg *registry, name string, m message) error {
	reg.mu.Lock()
	q := reg.queues[name]
	reg.mu.Unlock()
	if q == nil {
		return nil
	}

	q.mu.Lock()
	q.messages = append(q.messages, m)
	q.mu.Unlock()

	return drainMessages(q)
}

func (c *connection) writeFrame(frameType byte, channel uint16, payload []byte) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    return writeFrame(c.conn, frameType, channel, payload) // unprotected until we added this func
}