package main

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/go-nats"
)

func main() {
	nc, err := nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	// Subscriber
	nc.ActiveSubscribe(context.Background(), "hello",
		func(ctx context.Context, cancel context.CancelFunc, m *nats.Msg) {
			// User should check whether context already done,
			// either by a parent context cancelling or triggered
			// by a cancellation signal received from the requestor.
			// cancel()
			// return

			time.Sleep(2 * time.Second)
			select {
			case <-ctx.Done():
				log.Println("giving up, context is done already")
				return
			default:
				log.Println("context is ongoing")
			}

			// Result of the request
			nc.Publish(m.Reply, []byte("done!"))
		})
	nc.Flush()

	// Requestor
	nc2, err := nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	// Calling cancel here would propagate cancellation by signaling
	// via the special cancellation inbox.
	childCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// time.AfterFunc(1500*time.Millisecond, func() {
	// 	cancel()
	// })
	msg, err := nc2.ActiveRequest(childCtx, "hello", []byte("world"))
	if err != nil {
		log.Printf("Error: %s", err)
	}
	log.Printf("Response: %+v", msg)
	time.Sleep(10 * time.Second)
	if msg != nil {
		log.Printf("Data: %+v", string(msg.Data))
	}
}
