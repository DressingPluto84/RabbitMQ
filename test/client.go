// Progressive v1 test client for the broker.
//
// It walks the full v1 message path one method at a time and reports exactly
// where it stops. As you implement each broker step, the client gets one line
// further. Run it with the broker already listening:
//
//	go run .        (from this test/ dir)
//
// We dial [::1] explicitly so we hit OUR broker (IPv6) and not any real
// RabbitMQ that may be running on 127.0.0.1 (IPv4).
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func step(n int, name string, err error) {
	if err != nil {
		fmt.Printf("✗ step %d (%s) FAILED: %v\n", n, name, err)
		os.Exit(1)
	}
	fmt.Printf("✓ step %d: %s\n", n, name)
}

func main() {
	// --- 1. connection handshake (DONE) ---
	conn, err := amqp.Dial("amqp://guest:guest@[::1]:5672/")
	step(1, "amqp.Dial (connection handshake)", err)
	defer conn.Close()

	// --- 2. channel.open ---
	ch, err := conn.Channel()
	step(2, "conn.Channel (channel.open / open-ok)", err)
	defer ch.Close()

	// --- 3. queue.declare ---
	// Server-named? No — name it "hello" so publish/consume can find it via the
	// default exchange (routing key == queue name).
	q, err := ch.QueueDeclare("hello", false, false, false, false, nil)
	step(3, "ch.QueueDeclare (queue.declare / declare-ok)", err)

	// --- 4. basic.publish to the default exchange ---
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("hello world"),
	})
	step(4, "ch.PublishWithContext (basic.publish: method+header+body frames)", err)

	// --- 5. basic.consume + receive the delivery ---
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	step(5, "ch.Consume (basic.consume / consume-ok)", err)

	select {
	case d := <-msgs:
		fmt.Printf("✓ step 6: received delivery: %q\n", d.Body)
		fmt.Println("\n🎉 full v1 path works end-to-end")
	case <-time.After(3 * time.Second):
		fmt.Println("✗ step 6: no delivery received within 3s (basic.deliver not wired yet)")
		os.Exit(1)
	}
}
