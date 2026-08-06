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

func addMessage(reg *registry, name string, m message) {
	reg.mu.Lock()
	q := reg.queues[name]
	reg.mu.Unlock()
	if q == nil {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	q.messages = append(q.messages, m)
}

func (c *connection) writeFrame(frameType byte, channel uint16, payload []byte) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    return writeFrame(c.conn, frameType, channel, payload) // unprotected until we added this func
}