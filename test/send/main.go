// Producer for the v1 broker.
//
// v1 supports ONLY the default exchange: a message routes to the queue whose
// name == the routing key. So we publish with exchange="" and routing-key=queue
// name. (No ExchangeDeclare / QueueBind — those are v2 features.)
//
// Run this BEFORE receive: v1 delivers messages at consume-time (drain), so the
// message must already be in the queue when the consumer subscribes.
//
//	cd test && go run ./send                 # sends "hello world"
//	cd test && go run ./send my message here # sends "my message here"
package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	// Dial [::1] (IPv6 loopback) so we hit OUR broker, not a real RabbitMQ that
	// may be listening on 127.0.0.1 (IPv4).
	conn, err := amqp.Dial("amqp://guest:guest@[::1]:5672/")
	failOnError(err, "Failed to connect to broker")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open channel")
	defer ch.Close()

	// Declare the queue so it exists before we publish to it.
	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // auto-delete
		false,   // exclusive
		false,   // no-wait
		nil,     // args
	)
	failOnError(err, "Failed to declare queue")

	body := bodyFrom(os.Args)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"",     // exchange: "" = default exchange
		q.Name, // routing key = queue name (default-exchange routing)
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish")

	log.Printf("[x] Sent %q to queue %q", body, q.Name)
}

func bodyFrom(args []string) string {
	if len(args) < 2 || args[1] == "" {
		return "hello world"
	}
	return strings.Join(args[1:], " ")
}
