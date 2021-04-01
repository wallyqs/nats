// Copyright 2012-2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nats_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// Shows different ways to create a Conn
func ExampleConnect() {
	nc, _ := nats.Connect(nats.DefaultURL)
	nc.Close()

	nc, _ = nats.Connect("nats://derek:secretpassword@demo.nats.io:4222")
	nc.Close()

	nc, _ = nats.Connect("tls://derek:secretpassword@demo.nats.io:4443")
	nc.Close()

	opts := nats.Options{
		AllowReconnect: true,
		MaxReconnect:   10,
		ReconnectWait:  5 * time.Second,
		Timeout:        1 * time.Second,
	}

	nc, _ = opts.Connect()
	nc.Close()
}

// This Example shows an asynchronous subscriber.
func ExampleConn_Subscribe() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	nc.Subscribe("foo", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	})
}

// This Example shows a synchronous subscriber.
func ExampleConn_SubscribeSync() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	m, err := sub.NextMsg(1 * time.Second)
	if err == nil {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	} else {
		fmt.Println("NextMsg timed out.")
	}
}

func ExampleSubscription_NextMsg() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	m, err := sub.NextMsg(1 * time.Second)
	if err == nil {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	} else {
		fmt.Println("NextMsg timed out.")
	}
}

func ExampleSubscription_Unsubscribe() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	// ...
	sub.Unsubscribe()
}

func ExampleConn_Publish() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	nc.Publish("foo", []byte("Hello World!"))
}

func ExampleConn_PublishMsg() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	msg := &nats.Msg{Subject: "foo", Reply: "bar", Data: []byte("Hello World!")}
	nc.PublishMsg(msg)
}

func ExampleConn_Flush() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	msg := &nats.Msg{Subject: "foo", Reply: "bar", Data: []byte("Hello World!")}
	for i := 0; i < 1000; i++ {
		nc.PublishMsg(msg)
	}
	err := nc.Flush()
	if err == nil {
		// Everything has been processed by the server for nc *Conn.
	}
}

func ExampleConn_FlushTimeout() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	msg := &nats.Msg{Subject: "foo", Reply: "bar", Data: []byte("Hello World!")}
	for i := 0; i < 1000; i++ {
		nc.PublishMsg(msg)
	}
	// Only wait for up to 1 second for Flush
	err := nc.FlushTimeout(1 * time.Second)
	if err == nil {
		// Everything has been processed by the server for nc *Conn.
	}
}

func ExampleConn_Request() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	nc.Subscribe("foo", func(m *nats.Msg) {
		nc.Publish(m.Reply, []byte("I will help you"))
	})
	nc.Request("foo", []byte("help"), 50*time.Millisecond)
}

func ExampleConn_QueueSubscribe() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	received := 0

	nc.QueueSubscribe("foo", "worker_group", func(_ *nats.Msg) {
		received++
	})
}

func ExampleSubscription_AutoUnsubscribe() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	received, wanted, total := 0, 10, 100

	sub, _ := nc.Subscribe("foo", func(_ *nats.Msg) {
		received++
	})
	sub.AutoUnsubscribe(wanted)

	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	nc.Flush()

	fmt.Printf("Received = %d", received)
}

func ExampleConn_Close() {
	nc, _ := nats.Connect(nats.DefaultURL)
	nc.Close()
}

// Shows how to wrap a Conn into an EncodedConn
func ExampleNewEncodedConn() {
	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	c.Close()
}

// EncodedConn can publish virtually anything just
// by passing it in. The encoder will be used to properly
// encode the raw Go type
func ExampleEncodedConn_Publish() {
	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	defer c.Close()

	type person struct {
		Name    string
		Address string
		Age     int
	}

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	c.Publish("hello", me)
}

// EncodedConn's subscribers will automatically decode the
// wire data into the requested Go type using the Decode()
// method of the registered Encoder. The callback signature
// can also vary to include additional data, such as subject
// and reply subjects.
func ExampleEncodedConn_Subscribe() {
	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	defer c.Close()

	type person struct {
		Name    string
		Address string
		Age     int
	}

	c.Subscribe("hello", func(p *person) {
		fmt.Printf("Received a person! %+v\n", p)
	})

	c.Subscribe("hello", func(subj, reply string, p *person) {
		fmt.Printf("Received a person on subject %s! %+v\n", subj, p)
	})

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	c.Publish("hello", me)
}

// BindSendChan() allows binding of a Go channel to a nats
// subject for publish operations. The Encoder attached to the
// EncodedConn will be used for marshaling.
func ExampleEncodedConn_BindSendChan() {
	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	defer c.Close()

	type person struct {
		Name    string
		Address string
		Age     int
	}

	ch := make(chan *person)
	c.BindSendChan("hello", ch)

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	ch <- me
}

// BindRecvChan() allows binding of a Go channel to a nats
// subject for subscribe operations. The Encoder attached to the
// EncodedConn will be used for un-marshaling.
func ExampleEncodedConn_BindRecvChan() {
	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	defer c.Close()

	type person struct {
		Name    string
		Address string
		Age     int
	}

	ch := make(chan *person)
	c.BindRecvChan("hello", ch)

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	c.Publish("hello", me)

	// Receive the publish directly on a channel
	who := <-ch

	fmt.Printf("%v says hello!\n", who)
}

func ExampleJetStream() {
	nc, err := nats.Connect("localhost")
	if err != nil {
		log.Fatal(err)
	}

	// Use the JetStream context to produce and consumer messages
	// that have been persisted.
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	})

	js.Publish("foo", []byte("Hello JS!"))

	// Create async consumer on subject 'foo'. Async subscribers
	// ack a message once exiting the callback.
	js.Subscribe("foo", func(msg *nats.Msg) {
		meta, _ := msg.Metadata()
		fmt.Printf("Stream Sequence  : %v\n", meta.Sequence.Stream)
		fmt.Printf("Consumer Sequence: %v\n", meta.Sequence.Consumer)
	})

	// Async subscriber with manual acks.
	js.Subscribe("foo", func(msg *nats.Msg) {
		msg.Ack()
	}, nats.ManualAck())

	// Async queue subscription where members load balance the
	// received messages together.
	js.QueueSubscribe("foo", "group", func(msg *nats.Msg) {
		msg.Ack()
	}, nats.ManualAck())

	// Subscriber to consume messages synchronously.
	sub, _ := js.SubscribeSync("foo")
	msg, _ := sub.NextMsg(2 * time.Second)
	msg.Ack()

	// QueueSubscribe with group or load balancing.
	sub, _ = js.QueueSubscribeSync("foo", "group")
	msg, _ = sub.NextMsg(2 * time.Second)
	msg.Ack()

	// ChanSubscribe
	msgCh := make(chan *nats.Msg, 8192)
	sub, _ = js.ChanSubscribe("foo", msgCh)

	select {
	case msg := <-msgCh:
		fmt.Println("[Received]", msg)
	case <-time.After(1 * time.Second):
	}
}

func ExamplePubOpt() {
	nc, err := nats.Connect("localhost")
	if err != nil {
		log.Fatal(err)
	}

	// Create JetStream context to produce/consumer messages that will be persisted.
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	// Create stream to persist messages published on 'foo'.
	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	})

	// Publish is synchronous by default, and waits for a PubAck response.
	js.Publish("foo", []byte("Hello JS!"))

	// Publish with a custom timeout.
	js.Publish("foo", []byte("Hello JS!"), nats.AckWait(500*time.Millisecond))

	// Publish with a context.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	js.Publish("foo", []byte("Hello JS!"), nats.Context(ctx))

	// Publish and assert the expected stream name.
	js.Publish("foo", []byte("Hello JS!"), nats.ExpectStream("FOO"))

	// Publish and assert the last sequence.
	js.Publish("foo", []byte("Hello JS!"), nats.ExpectLastSequence(5))

	// Publish and tag the message with an ID.
	js.Publish("foo", []byte("Hello JS!"), nats.MsgId("foo:6"))

	// Publish and assert the last msg ID.
	js.Publish("foo", []byte("Hello JS!"), nats.ExpectLastMsgId("foo:6"))
}

func ExampleMaxWait() {
	nc, _ := nats.Connect("localhost")

	// Set default timeout for JetStream API requests,
	// following requests will inherit this timeout.
	js, _ := nc.JetStream(nats.MaxWait(3 * time.Second))

	// Set custom timeout for a JetStream API request.
	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	}, nats.MaxWait(2*time.Second))

	sub, _ := js.PullSubscribe("foo", "my-durable-name")

	// Fetch using the default timeout of 3 seconds.
	msgs, _ := sub.Fetch(1)

	// Set custom timeout for a pull batch request.
	msgs, _ = sub.Fetch(1, nats.MaxWait(2*time.Second))

	for _, msg := range msgs {
		msg.Ack()
	}
}

func ExampleAckWait() {
	nc, _ := nats.Connect("localhost")
	js, _ := nc.JetStream()

	// Set custom timeout for a JetStream API request.
	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	})

	// Wait for an ack response for 2 seconds.
	js.Publish("foo", []byte("Hello JS!"), nats.AckWait(2*time.Second))

	// Create consumer on 'foo' subject that waits for an ack for 10s,
	// after which the message will be delivered.
	sub, _ := js.SubscribeSync("foo", nats.AckWait(10*time.Second))
	msg, _ := sub.NextMsg(2 * time.Second)

	// Wait for ack of ack for 2s.
	msg.AckSync(nats.AckWait(2 * time.Second))
}
