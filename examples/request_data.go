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
		nc.Publish(m.Reply, []byte("helloooooooooooooo"))
		nc.Publish(m.Reply, []byte("two"))
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

	payload := make([]byte, 512)
	err := nc.RequestData("hello", []byte("world"), payload, 1*time.Second)
	// fmt.Printf("---- user: %p\n", payload)
	fmt.Printf("RESPONSE: %+v \n", string(payload))
	fmt.Println("ERROR:   ", err)
	fmt.Println("")
}
