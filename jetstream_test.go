// Copyright 2020 The NATS Authors
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

package nats

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go/jetstream"
)

func startJetStream(t *testing.T) (*server.Server, *server.Stream, *server.Consumer, *Conn) {
	td, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}

	sopts := natsserver.DefaultTestOptions
	sopts.JetStream = true
	sopts.StoreDir = td
	sopts.Port = -1
	sopts.NoLog = false
	sopts.TraceVerbose = true
	sopts.Trace = true
	sopts.LogFile = "/tmp/nats.log"

	srv, err := server.NewServer(&sopts)
	if err != nil {
		t.Fatal(err)
	}

	srv.ConfigureLogger()
	go srv.Start()

	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatalf("server did not become ready")
	}

	str, err := srv.GlobalAccount().AddStream(&server.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"js.in.test"},
		Storage:  server.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}

	cons, err := str.AddConsumer(&server.ConsumerConfig{
		Durable:   "PULL",
		AckPolicy: server.AckExplicit,
	})
	if err != nil {
		t.Fatalf("consumer create failed: %s", err)
	}

	nc, err := Connect(srv.ClientURL(), UseOldRequestStyle())
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	// TEST stream publishes
	for i := 1; i <= 20; i++ {
		err := nc.Publish("js.in.test", []byte(fmt.Sprintf("msg %d", i)), PublishExpectsStream("TEST"))
		if err != nil {
			t.Fatalf("publish failed: %s", err)
		}
	}

	return srv, str, cons, nc
}

func TestJetStreamPublish(t *testing.T) {
	srv, _, _, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	err := nc.Publish("js.in.test", []byte("hello"), PublishExpectsStream("TEST"), PublishStreamTimeout(time.Second))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	err = nc.Publish("js.in.test", []byte("hello"), PublishExpectsStream("OTHER"), PublishStreamTimeout(time.Second))
	if err == nil {
		t.Fatalf("expected an error but got none")
	}
	if err.Error() != `received ack from stream "TEST"` {
		t.Fatalf("expected wrong stream error, got: %q", err)
	}

	err = nc.Publish("js.test", []byte("hello"), PublishExpectsStream("OTHER"), PublishStreamTimeout(time.Second))
	if err == nil {
		t.Fatalf("expected an error but got none")
	}
	if err != ErrNoResponders {
		t.Fatalf("expected no responders error, got %s", err)
	}

	err = nc.Publish("js.in.test", []byte("hello"), PublishExpectsStream())
	if err != nil {
		t.Fatalf("unexpected error publishing: %s", err)
	}
	err = nc.Publish("js.test", []byte("hello"), PublishExpectsStream())
	if err != ErrNoResponders {
		t.Fatalf("unexpected error publishing: %s", err)
	}
}

func TestMsg_ParseJSMsgMetadata(t *testing.T) {
	cases := []struct {
		meta    string
		pending int
	}{
		{"$JS.ACK.ORDERS.NEW.1.2.3.1587466354254920000", -1},
		{"$JS.ACK.ORDERS.NEW.1.2.3.1587466354254920000.10", 10},
	}

	for _, tc := range cases {
		msg := &Msg{Reply: tc.meta}
		meta, err := msg.JetStreamMetaData()
		if err != nil {
			t.Fatalf("could not get message metadata: %s", err)
		}

		if meta.Stream != "ORDERS" {
			t.Fatalf("Expected ORDERS got %q", meta.Stream)
		}

		if meta.Consumer != "NEW" {
			t.Fatalf("Expected NEW got %q", meta.Consumer)
		}

		if meta.Delivered != 1 {
			t.Fatalf("Expected 1 got %q", meta.Delivered)
		}

		if meta.StreamSeq != 2 {
			t.Fatalf("Expected 2 got %q", meta.Stream)
		}

		if meta.ConsumerSeq != 3 {
			t.Fatalf("Expected 3 got %q", meta.ConsumerSeq)
		}

		if meta.TimeStamp != time.Unix(0, int64(1587466354254920000)) {
			t.Fatalf("Expected 2020-04-21T12:52:34.25492+02:00 got %q", meta.TimeStamp)
		}

		if meta.Pending != tc.pending {
			t.Fatalf("Expected %d got %d", tc.pending, meta.Pending)
		}
	}
}

func TestMsg_Ack(t *testing.T) {
	srv, _, cons, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", nil, time.Second)
	if err != nil {
		t.Fatalf("pull failed: %s", err)
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("received invalid 'msg 1': %q", msg.Data)
	}

	if cons.Info().AckFloor.Stream != 0 {
		t.Fatalf("first message was already acked")
	}

	err = msg.Ack(AckWaitDuration(time.Second))
	if err != nil {
		t.Fatalf("ack failed: %s", err)
	}

	if cons.Info().AckFloor.Stream != 1 {
		t.Fatalf("first message was not acked")
	}
}

func TestMsg_Nak(t *testing.T) {
	srv, _, cons, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", nil, time.Second)
	if err != nil {
		t.Fatalf("pull failed: %s", err)
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("received invalid 'msg 1': %q", msg.Data)
	}

	if cons.Info().AckFloor.Stream != 0 {
		t.Fatalf("first message was already acked")
	}

	err = msg.Nak(AckWaitDuration(time.Second))
	if err != nil {
		t.Fatalf("ack failed: %s", err)
	}

	if cons.Info().AckFloor.Stream != 0 {
		t.Fatalf("first message was acked")
	}
}

func TestMsg_AckTerm(t *testing.T) {
	srv, _, cons, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", nil, time.Second)
	if err != nil {
		t.Fatalf("pull failed: %s", err)
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("received invalid 'msg 1': %q", msg.Data)
	}

	if cons.Info().AckFloor.Stream != 0 {
		t.Fatalf("first message was already acked")
	}

	err = msg.AckTerm(AckWaitDuration(time.Second))
	if err != nil {
		t.Fatalf("ack failed: %s", err)
	}

	if cons.Info().AckFloor.Stream != 1 {
		t.Fatalf("first message was not acked")
	}
}

func TestMsg_AckProgress(t *testing.T) {
	srv, _, cons, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", nil, time.Second)
	if err != nil {
		t.Fatalf("pull failed: %s", err)
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("received invalid 'msg 1': %q", msg.Data)
	}

	if cons.Info().AckFloor.Stream != 0 {
		t.Fatalf("first message was already acked")
	}

	err = msg.AckProgress(AckWaitDuration(time.Second))
	if err != nil {
		t.Fatalf("ack failed: %s", err)
	}

	if cons.Info().AckFloor.Stream != 0 {
		t.Fatalf("first message was acked")
	}

	err = msg.Ack(AckWaitDuration(time.Second))
	if err != nil {
		t.Fatalf("ack failed: %s", err)
	}

	if cons.Info().AckFloor.Stream != 1 {
		t.Fatalf("first message was not acked")
	}
}

func TestMsg_AckAndFetch(t *testing.T) {
	srv, _, cons, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", []byte("1"), time.Second)
	if err != nil {
		t.Fatalf("request failed: %s", err)
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("received incorrect message %q", msg.Data)
	}

	for i := 1; i < 20; i++ {
		if cons.Info().AckFloor.Stream == uint64(i) {
			t.Fatalf("message %d was already acked", i)
		}
		msg, err = msg.AckAndFetch()
		if err != nil {
			t.Fatalf("ack failed: %s", err)
		}
		if cons.Info().AckFloor.Stream != uint64(i) {
			t.Fatalf("message %d was not acked", i)
		}
		if !bytes.Equal(msg.Data, []byte(fmt.Sprintf("msg %d", i+1))) {
			t.Fatalf("received incorrect message %q", msg.Data)
		}
	}
}

func TestMsg_AckNext(t *testing.T) {
	srv, _, cons, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	sub, err := nc.SubscribeSync(NewInbox())
	if err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}

	err = nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", sub.Subject, nil)
	if err != nil {
		t.Fatalf("pull failed: %s", err)
	}

	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("next failed: %s", err)
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("received invalid 'msg 1': %q", msg.Data)
	}

	if cons.Info().AckFloor.Stream != 0 {
		t.Fatalf("first message was already acked")
	}

	err = msg.AckNextRequest(&AckNextRequest{Batch: 5})
	if err != nil {
		t.Fatalf("ack failed: %s", err)
	}

	for i := 2; i < 7; i++ {
		msg, err = sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("next failed: %s", err)
		}

		expect := fmt.Sprintf("msg %d", i)
		if !bytes.Equal(msg.Data, []byte(expect)) {
			t.Fatalf("expected %s got %#v", expect, msg)
		}
	}

	if cons.Info().AckFloor.Stream != 1 {
		t.Fatalf("first message was not acked")
	}
}

func TestJetStreamContext_Publish(t *testing.T) {
	srv, _, _, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	// Possible to add the publish options on the context.
	js, err := nc.JetStream(jetstream.PublishStreamTimeout(time.Second))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	ack, err := js.Publish("js.in.test", []byte("hello"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if ack.Stream != "TEST" {
		t.Fatalf("unexpected stream name: %v", err)
	}

	js, err = nc.JetStream(jetstream.Stream("OTHER"), jetstream.PublishStreamTimeout(time.Second))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Should get an error since `js.in.test` maps to stream TEST not OTHER.
	ack, err = js.Publish("js.in.test", []byte("world"))
	if err == nil {
		t.Fatalf("expected an error but got none")
	}
	if err.Error() != `received ack from stream "TEST"` {
		t.Fatalf("expected wrong stream error, got: %q", err)
	}
}

func TestJetStreamContext_PublishNoAPIAccess(t *testing.T) {
	// Creates the TEST stream.
	srv, _, _, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	_, err := srv.GlobalAccount().AddStream(&server.StreamConfig{
		Name:     "ANOTHER",
		Subjects: []string{"another.stream"},
		Storage:  server.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Publish implicitly to the TEST stream.
	ack, err := js.Publish("js.in.test", []byte("hello"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if ack.Stream != "TEST" {
		t.Fatalf("unexpected stream name: %v", err)
	}
	got := ack.Sequence
	expected := 21
	if got != expected {
		t.Fatalf("expected %d, got: %d", expected, got)
	}

	// Publish implicitly to ANOTHER stream.
	ack, err = js.Publish("another.stream", []byte("hello world"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if ack.Stream != "ANOTHER" {
		t.Fatalf("unexpected stream name: %v", err)
	}
	got = ack.Sequence
	expected = 1
	if got != expected {
		t.Fatalf("expected %d, got: %d", expected, got)
	}

	// Explicitly binding to a different stream causes an error
	// if the response comes from another stream.
	js2, err := nc.JetStream(jetstream.Stream("OTHER"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Should get an error since `js.in.test` maps to stream TEST not OTHER.
	ack, err = js2.Publish("js.in.test", []byte("world"))
	if err == nil {
		t.Fatalf("expected an error but got none")
	}
	if err.Error() != `received ack from stream "TEST"` {
		t.Fatalf("expected wrong stream error, got: %q", err)
	}
}

func TestJetStreamContext_Subscribe(t *testing.T) {
	srv, _, _, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	cfg := &jetstream.ConsumerConfig{
		// NOTE: Subscribe is only for ephemeral consumers.
		// Durable:       "nats",
		DeliverPolicy: jetstream.DeliverAll,
		AckPolicy:     jetstream.AckExplicit,
		AckWait:       5 * time.Second,
		ReplayPolicy:  jetstream.ReplayInstant,
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	seen := 0

	sub, err := js.Subscribe("js.in.test", func(m *Msg) {
		m.Ack()
		seen++
		if seen == 20 {
			cancel()
		}
	}, jetstream.Consumer(cfg))
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	defer sub.Unsubscribe()

	<-ctx.Done()

	if seen != 20 {
		t.Fatalf("Expected 20 messages got %d", seen)
	}
}

func TestJetStreamContext_SubscribeStreamLookup(t *testing.T) {
	srv, _, _, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	seen := 0

	sub, err := js.Subscribe("js.in.test", func(m *Msg) {
		m.Ack()
		seen++
		if seen == 20 {
			cancel()
		}
	})
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	defer sub.Unsubscribe()

	<-ctx.Done()

	if seen != 20 {
		t.Fatalf("Expected 20 messages got %d", seen)
	}
}

func TestJetStreamContext_DurableSubscribe(t *testing.T) {
	srv, _, _, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	consumer := jetstream.Consumer(&jetstream.ConsumerConfig{
		Durable:       "nats:1",
		DeliverPolicy: jetstream.DeliverAll,
		AckPolicy:     jetstream.AckExplicit,
		AckWait:       5 * time.Second,
		ReplayPolicy:  jetstream.ReplayInstant,
	})

	js, err := nc.JetStream(consumer)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	seen := 0

	sub, err := js.Subscribe("js.in.test", func(m *Msg) {
		m.Ack()
		seen++
		if seen == 20 {
			cancel()
		}
	})

	ods := jetstream.DeliverySubject(sub.Subject)
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	defer sub.Unsubscribe()

	<-ctx.Done()

	if seen != 20 {
		t.Fatalf("Expected 20 messages got %d", seen)
	}
	nc.Drain()

	// Reconnect and reuse durable with same DeliverySubject
	nc2, err := Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc2.Close()

	nc.Publish("js.in.test", []byte("msg 21"))
	nc.Publish("js.in.test", []byte("msg 22"))

	js2, err := nc2.JetStream()
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	seen = 0
	consumer2 := jetstream.Consumer(&jetstream.ConsumerConfig{
		Durable:       "nats:1",
		DeliverPolicy: jetstream.DeliverAll,
		AckPolicy:     jetstream.AckExplicit,
		AckWait:       5 * time.Second,
		ReplayPolicy:  jetstream.ReplayInstant,
	})
	sub2, err := js2.Subscribe("js.in.test", func(m *Msg) {
		m.Ack()
		seen++
		if seen == 2 {
			cancel()
		}
	}, jetstream.Stream("TEST"), consumer2, ods)
	if sub2.Subject != sub.Subject {
		t.Fatalf("expected %s, got: %s", sub.Subject, sub2.Subject)
	}
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}

	<-ctx.Done()

	if seen != 2 {
		t.Fatalf("Expected 2 messages got %d", seen)
	}
}

func TestJetStreamContext_SubscribeMultiSubject(t *testing.T) {
	srv, _, _, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	_, err := srv.GlobalAccount().AddStream(&server.StreamConfig{
		Name: "MULTISUBJECT",
		Subjects: []string{"jsm.dev.*", "jsm.test.*", "jsm.prod.*"},
		Storage:  server.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}

	nc.Publish("jsm.test.one", []byte("1"))
	nc.Publish("jsm.test.two", []byte("2"))
	nc.Publish("jsm.test.three", []byte("3"))
	nc.Publish("jsm.test.three", []byte("33"))
	nc.Publish("jsm.test.two", []byte("22"))
	nc.Publish("jsm.test.one", []byte("11"))
	nc.Publish("jsm.prod.one", []byte("10001"))
	nc.Publish("jsm.prod.two", []byte("10002"))

	cfg := &jetstream.ConsumerConfig{
		DeliverPolicy: jetstream.DeliverAll,
		AckPolicy:     jetstream.AckExplicit,
		AckWait:       5 * time.Second,
		ReplayPolicy:  jetstream.ReplayInstant,
	}

	js, err := nc.JetStream(jetstream.Consumer(cfg))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	seen := 0
	expected := 6
	sub, err := js.Subscribe("jsm.test.*", func(m *Msg) {
		m.Ack()
		seen++
		if seen == expected {
			cancel()
		}
	})
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	sub.Unsubscribe()

	<-ctx.Done()

	if seen != expected {
		t.Fatalf("Expected %d messages got %d", expected, seen)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	// Using empty string means that filter subject is empty so fetches all.
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Fetch all the messages from all the subjects from a stream.
	seen = 0
	expected = 8
	sub, err = js.Subscribe("", func(m *Msg) {
		m.Ack()
		seen++
		if seen == expected {
			cancel()
		}
	}, jetstream.Stream("MULTISUBJECT"))
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	sub.Unsubscribe()

	<-ctx.Done()

	if seen != expected {
		t.Fatalf("Expected %d messages got %d", expected, seen)
	}

	// Narrow down to single subject.
	seen = 0
	expected = 2
	sub, err = js.Subscribe("jsm.test.two", func(m *Msg) {
		m.Ack()
		seen++
		if seen == expected {
			cancel()
		}
	})
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	sub.Unsubscribe()

	<-ctx.Done()

	if seen != expected {
		t.Fatalf("Expected %d messages got %d", expected, seen)
	}
}

func TestJetStreamContext_SubscribeDefaultEphemeralConsumer(t *testing.T) {
	srv, _, _, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	seen := 0
	sub, err := js.Subscribe("js.in.test", func(m *Msg) {
		m.Ack()
		seen++
		if seen == 20 {
			cancel()
		}
	})
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	defer sub.Unsubscribe()

	<-ctx.Done()

	if seen != 20 {
		t.Fatalf("Expected 20 messages got %d", seen)
	}
}

func TestJetStreamContext_PullSubscriber(t *testing.T) {
	srv, _, _, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	sub, err := nc.JetStream(
		jetstream.Stream("TEST"),
		jetstream.Consumer(&jetstream.ConsumerConfig{Durable: "PULL"}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only for pull based consumers.
	msg, err := sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := string(msg.Data)
	expected := "msg 1"
	if got != expected {
		t.Fatalf("expected %s, got: %q", expected, got)
	}

	sub, err = nc.JetStream(jetstream.Stream("TEST"))
	_, err = sub.NextMsg(1 * time.Second)
	if err == nil {
		t.Fatalf("expected error fetching message")
	}
}

func TestJetStreamContext_PublishSubscribe(t *testing.T) {
	srv, _, _, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	_, err := srv.GlobalAccount().AddStream(&server.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
		Storage:  server.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	for i := 0; i < 20; i++ {
		ack, err := js.Publish("foo", []byte("hello world"))
		if err != nil {
			t.Errorf("Unexpected error publishing to stream: %s", err)
		}
		if ack.Stream != "FOO" {
			t.Errorf("Unexpected ack from stream %s:", ack.Stream)
		}

		got := ack.Sequence
		expected := i + 1
		if got != expected {
			t.Errorf("expected %d, got: %d", expected, got)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	seen := 0
	sub, err := js.Subscribe("foo", func(m *Msg) {
		m.Ack()
		seen++
		if seen == 20 {
			cancel()
		}
	})
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	defer sub.Unsubscribe()

	<-ctx.Done()

	if seen != 20 {
		t.Fatalf("Expected 20 messages got %d", seen)
	}
}

func TestJetStreamContext_PublishSubscribeOptions(t *testing.T) {
	srv, _, _, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	_, err := srv.GlobalAccount().AddStream(&server.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
		Storage:  server.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	for i := 0; i < 20; i++ {
		ack, err := js.Publish("foo", []byte("hello world"), jetstream.Stream("FOO"))
		if err != nil {
			t.Errorf("Unexpected error publishing to stream: %s", err)
		}
		if ack.Stream != "FOO" {
			t.Errorf("Unexpected ack from stream %s:", ack.Stream)
		}

		got := ack.Sequence
		expected := i + 1
		if got != expected {
			t.Errorf("expected %d, got: %d", expected, got)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	seen := 0
	sub, err := js.Subscribe("foo", func(m *Msg) {
		m.Ack()
		seen++
		if seen == 20 {
			cancel()
		}
	})
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}
	defer sub.Unsubscribe()

	<-ctx.Done()

	if seen != 20 {
		t.Fatalf("Expected 20 messages got %d", seen)
	}
}
