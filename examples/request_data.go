package main

import (
	"fmt"
	"time"

	"github.com/nats-io/go-nats"
)

func main() {
	nc, _ := nats.Connect("nats://127.0.0.1:4222")

	// Require an allocation, can't be helped
	nc.Subscribe("hello", func(m *nats.Msg) {
		// Publishes also escape to the heap????
		a := []byte("helloooooooooooooo")
		b := []byte("two")
		nc.Publish(m.Reply, a)
		nc.Publish(m.Reply, b)
	})
	// nc.Subscribe("hello2", func(m *nats.Msg) {
	// 	nc.Publish(m.Reply, []byte("one"))
	// 	nc.Publish(m.Reply, []byte("two"))
	// })

	// result is nats.Msg
	// result, err := nc.Request("hello", []byte("world"), 1*time.Second)
	// fmt.Printf("RESPONSE: %+v || %+v || %s \n", reflect.TypeOf(result), result, string(result.Data))
	// fmt.Println("ERROR:   ", err)
	// fmt.Println("")

	// var payload []byte
	// err := nc.RequestData("hello", []byte("world"), &payload, 1*time.Second)
	// fmt.Printf("RESPONSE: %+v \n", string(payload))
	// fmt.Println("ERROR:   ", err)
	// fmt.Println("")

	// var payload []byte

	// We get this from parsing INFO, so defined at runtime.
	// maxPayload := 1048576

	// Constrain a priori the maximum, it is a runtime value.
	// Set at compile time so that it is on the stack still maybe.
	// &nats.Msg{}
	//
	// type Response struct {
	// 	Payload []byte
	// }
	// m := &Response{}
	//

	// examples/request_data.go:52: make([]byte, 512) escapes to heap
	// examples/request_data.go:52: 	from response (assigned) at examples/request_data.go:52
	// examples/request_data.go:52: 	from response (passed to call[argument escapes]) at examples/request_data.go:54
	// examples/request_data.go:53: ([]byte)("world") escapes to heap
	// examples/request_data.go:53: 	from payload (assigned) at examples/request_data.go:53
	// examples/request_data.go:53: 	from payload (passed to call[argument escapes]) at examples/request_data.go:54
	do(nc)
}

func do(nc *nats.Conn){
	response := make([]byte, 512)
	payload := []byte("world")
	err := nc.RequestData("hello", payload, response, 1*time.Second)
	// fmt.Printf("---- user: %p\n", payload)
	fmt.Printf("PAYLOAD: %+v \n", string(payload))
	fmt.Printf("RESPONSE: %+v \n", string(response))
	fmt.Println("ERROR:   ", err)
	fmt.Println("")
}
