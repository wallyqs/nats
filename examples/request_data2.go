package main

import (
	"time"

	"github.com/nats-io/go-nats"
)

var err error
var nc *nats.Conn

// var reply []byte = []byte("helloooooooooooooooooooooooo")
var subject = "hello"

type Response struct {
	data []byte
}

func (r *Response) Write(b []byte) (int, error) {
	println("-----------------------", b)
	r.data = make([]byte, len(b))
	copy(r.data, b)
	// copy(r.data, b)
	return 0, nil
}

func main() {
	nc, _ = nats.Connect("nats://127.0.0.1:4222")

	// func literal escapes to the heap, but it is ok
	nc.Subscribe("hello", func(m *nats.Msg) {
		// Require an allocation for Msg, can't be helped...
		// leaking param content: m

		// escapes to heap...
		println("replying!!!")
		nc.Publish(m.Reply, []byte("hellooooooooaaaaaaaaaaaaaaaaooooooooooooooooooooo"))
	})

	// escapes to heap because we do not know the size
	// var response []byte
	// var payload [5]byte = [5]byte("world")
	// var payload []byte

	// escapes to the heap
	payload := make([]byte, 512)

	// Writer?
	// examples/request_data2.go:45: data escapes to heap
	// examples/request_data2.go:45: 	from data (passed to call[argument escapes]) at examples/request_data2.go:45
	// examples/request_data2.go:38: &bytes.Buffer literal escapes to heap
	// data := &bytes.Buffer{}

	// data := &Response{
	// Data: make([]byte, 16),
	// }
	data := &Response{}

	// possible to expand the size of it?
	// data := make([]byte, 3)
	println("---- user: ", payload, len(payload))
	// println("---- user: ", data, len(data))

	err := nc.RequestData(subject, payload, data, 1*time.Second)
	println("----", err)
	println("----", string(data.data))
	// println(data.Bytes())

	// these do not escape
	// var ps string = string(payload)
	// var rs string = string(data)
	// println("---- user: ", payload, len(payload))
	// println("---- user: ", data, len(data))
	// println("PAYLOAD: ", ps, "|", len(ps))
	// println("RESPONSE: ", rs, "|", len(rs))

	// println("PAYLOAD: ", string(payload))
	// println("RESPONSE: ", response)
}
