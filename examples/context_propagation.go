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
			log.Println("-------------:D:D:D:D:DD", m, string(m.Data))
			// Make requestor give up... there could be more
			// context aware calls here for example.
			// time.Sleep(1 * time.Millisecond)

			// User should check whether context already done,
			// either by a parent context cancelling or triggered
			// by a cancellation signal received from the requestor.
			select {
			case <-ctx.Done():
				log.Println("???????")
				return
			default:
				log.Println("???")
			}

			// Actual reply
			nc.Publish(m.Reply, []byte("done!"))
		})
	nc.Flush()

	// Requestor
	nc2, err := nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	// Calling cancel here would propagate cancellation with the remote
	// by signaling via the special cancellation inbox.
	childCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg, err := nc2.ActiveRequest(childCtx, "hello", []byte("world"))
	if err != nil {
		log.Printf("Error: %s", err)
	}
	log.Printf("Response: %+v", msg)
}
