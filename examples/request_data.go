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
		nc.Publish(m.Reply, []byte("escapes2!!"))
	})

	// Do not expect responses to grow further than this,
	// that way we can keep them on the stack and reduce
	// allocations.
	payload := []byte("bufio escapes1!!!")
	data := make([]byte, 32768)
	err = nc.RequestData("hello", payload, data, 1*time.Second)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	log.Printf("Response: %s", string(data))
}
