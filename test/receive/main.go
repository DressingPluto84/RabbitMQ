// Consumer for the v1 broker.
//
// v1 supports ONLY the default exchange, so we consume straight from the named
// queue (no QueueBind). And v1 delivers at consume-time (drain): whatever is
// already in the queue when we subscribe gets pushed to us. There's no
// publish-time push yet, so messages sent AFTER we subscribe won't arrive.
//
// So the working order is: run ./send first, THEN run ./receive.
//
//	cd test && go run ./receive
package main

import (
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@[::1]:5672/")
	failOnError(err, "Failed to connect to broker")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open channel")
	defer ch.Close()

	// Same queue name the producer used. Declare is idempotent — if send.go
	// already created it, this just returns the existing one.
	q, err := ch.QueueDeclare("hello", false, false, false, false, nil)
	failOnError(err, "Failed to declare queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer tag ("" = broker generates one)
		true,   // auto-ack (v1 has no manual acks yet)
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register consumer")

	log.Printf("[*] Consuming from %q. Draining queued messages...", q.Name)

	// v1 pushes whatever was already queued, then nothing more (no publish-time
	// trigger). So we print what we get and exit after a short idle gap, rather
	// than blocking forever waiting for pushes that can't come in v1.
	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				log.Println("[*] Delivery channel closed. Exiting.")
				return
			}
			log.Printf("Received: %s", d.Body)
		case <-time.After(2 * time.Second):
			log.Println("[*] No more messages. Exiting.")
			return
		}
	}
}
