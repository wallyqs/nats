package main

import (
	"time"

	"github.com/nats-io/go-nats"
)

var err error
var nc *nats.Conn

// var reply []byte = []byte("helloooooooooooooooooooooooo")
var subject = "hello"

func main() {
	nc, _ = nats.Connect("nats://127.0.0.1:4222")

	// func literal escapes to the heap, but it is ok
	nc.Subscribe("hello", func(m *nats.Msg) {
		// Require an allocation for Msg, can't be helped...
		// leaking param content: m

		// escapes to heap...
		println("replying!!!")
		nc.Publish(m.Reply, []byte("helloooooooooooooo"))
	})

	// escapes to heap because we do not know the size
	// var response []byte
	// var payload [5]byte = [5]byte("world")
	// var payload []byte

	// escapes to the heap
	payload := make([]byte, 512)
	data := make([]byte, 64)
	println("---- user: ", payload, len(payload))
	println("---- user: ", data, len(data))

	// payload: byte literal escapes to the heap...
	err := nc.RequestData(subject, payload, data, 1*time.Second)
	println(err)

	// these do not escape
	var ps string = string(payload)
	var rs string = string(data)
	println("---- user: ", payload, len(payload))
	println("---- user: ", data, len(data))
	println("PAYLOAD: ", ps, "|", len(ps))
	println("RESPONSE: ", rs, "|", len(rs))

	// println("PAYLOAD: ", string(payload))
	// println("RESPONSE: ", response)
}
