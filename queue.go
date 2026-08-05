package main

import (
	"sync"
	"net"
)

type message struct {
	payload []byte
	routingKey string
}

type consumer struct {
	tag string // tag into the channel in case of multi consumer
	ch uint16
	conn net.Conn
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