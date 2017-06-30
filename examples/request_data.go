package main

import (
	"log"
	"time"

	"github.com/nats-io/go-nats"
)

func main() {
	nc, err := nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	nc.Subscribe("hello", func(m *nats.Msg) {
		nc.Publish(m.Reply, []byte("helloooooooooooooo"))
	})

	// Do not expect responses to grow further than this,
	// that way we can keep them on the stack and reduce
	// allocations.
	data := make([]byte, 32768)
	err = nc.RequestData("hello", []byte("help"), data, 1*time.Second)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	log.Println("Response: %s", string(data))
}
