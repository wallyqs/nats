package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/go-nats"
)

func main() {
	nc, err := nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	// This is a new context with its own cancellation??
	nc.Subscribe("hello", func(m *nats.Msg) {
		// Expect to be able to split the request
		// to get the last inbox bit.
		log.Println(m.Reply)
		toks := strings.SplitN(m.Reply, ".", 3)
		log.Println(toks)
		log.Println(toks[2])
		cInbox := toks[2]
		log.Println(cInbox)

		inbox := fmt.Sprintf("_INBOX.%s", cInbox)
		ch := make(chan struct{}, 1)
		nc.Subscribe(inbox, func(mm *nats.Msg) {
			if ch != nil {
				log.Println("cancelled!! stop working...", mm)
				close(ch)
				// ch = nil
			}
		})

		go func() {
			log.Println("working...")
			time.Sleep(2 * time.Second)
			// time.Sleep(2 * time.Millisecond)
			// check if context is still ongoing
			// or just give up already...
			select {
			case _, wd := <-ch:
				log.Println("closed already without data: ", wd)
			default:
				log.Println("continue...")
			}

			nc.Publish(m.Reply, []byte("done!"))
		}()

		select {
		case _, wd := <-ch:
			log.Println("----- closed!!", wd)
		}
	})

	// Do not expect responses to grow further than this,
	// that way we can keep them on the stack and reduce
	// allocations.
	payload := []byte("example")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	msg, err := nc.ActiveRequestWithContext(ctx, "hello", payload)	
	if err != nil {
		time.Sleep(3*time.Second)
		log.Fatalf("Error: %s", err)
	}
	log.Printf("Response: %s", string(msg.Data))
}
