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

package test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestJetStreamNotEnabled(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	if _, err := nc.JetStream(); err != nats.ErrJetStreamNotEnabled {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
}

func TestJetStreamNotAccountEnabled(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
			},
			IU: {
				users: [ {user: rip, password: bar} ]
			},
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	if _, err := nc.JetStream(); err != nats.ErrJetStreamNotEnabled {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
}

func TestJetStreamPublish(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we get a proper failure when no stream is present.
	_, err = js.Publish("foo", []byte("Hello JS"))
	if err != nats.ErrNoStreamResponse {
		t.Fatalf("Expected a no stream error but got %v", err)
	}

	// Create the stream using our client API.
	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test", "foo", "bar"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Double check that file-based storage is default.
	if si.Config.Storage != nats.FileStorage {
		t.Fatalf("Expected FileStorage as default, got %v", si.Config.Storage)
	}

	// Lookup the stream for testing.
	mset, err := s.GlobalAccount().LookupStream("TEST")
	if err != nil {
		t.Fatalf("stream lookup failed: %v", err)
	}

	var pa *nats.PubAck
	expect := func(seq, nmsgs uint64) {
		t.Helper()
		if seq > 0 && pa == nil {
			t.Fatalf("Missing pubAck to test sequence %d", seq)
		}
		if pa != nil {
			if pa.Stream != "TEST" {
				t.Fatalf("Wrong stream name, expected %q, got %q", "TEST", pa.Stream)
			}
			if seq > 0 && pa.Sequence != seq {
				t.Fatalf("Wrong stream sequence, expected %d, got %d", seq, pa.Sequence)
			}
		}
		if state := mset.State(); state.Msgs != nmsgs {
			t.Fatalf("Expected %d messages, got %d", nmsgs, state.Msgs)
		}
	}

	msg := []byte("Hello JS")

	// Basic publish like NATS core.
	pa, err = js.Publish("foo", msg)
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	expect(1, 1)

	// Test stream expectation.
	pa, err = js.Publish("foo", msg, nats.ExpectStream("ORDERS"))
	if err == nil || !strings.Contains(err.Error(), "stream does not match") {
		t.Fatalf("Expected an error, got %v", err)
	}
	// Test last sequence expectation.
	pa, err = js.Publish("foo", msg, nats.ExpectLastSequence(10))
	if err == nil || !strings.Contains(err.Error(), "wrong last sequence") {
		t.Fatalf("Expected an error, got %v", err)
	}
	// Messages should have been rejected.
	expect(0, 1)

	// Send in a stream with a msgId
	pa, err = js.Publish("foo", msg, nats.MsgId("ZZZ"))
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	expect(2, 2)

	// Send in the same message with same msgId.
	pa, err = js.Publish("foo", msg, nats.MsgId("ZZZ"))
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	if pa.Sequence != 2 {
		t.Fatalf("Expected sequence of 2, got %d", pa.Sequence)
	}
	if !pa.Duplicate {
		t.Fatalf("Expected duplicate to be set")
	}
	expect(2, 2)

	// Now try to send one in with the wrong last msgId.
	pa, err = js.Publish("foo", msg, nats.ExpectLastMsgId("AAA"))
	if err == nil || !strings.Contains(err.Error(), "wrong last msg") {
		t.Fatalf("Expected an error, got %v", err)
	}
	// Make sure expected sequence works.
	pa, err = js.Publish("foo", msg, nats.ExpectLastSequence(22))
	if err == nil || !strings.Contains(err.Error(), "wrong last sequence") {
		t.Fatalf("Expected an error, got %v", err)
	}
	expect(0, 2)

	// This should work ok.
	pa, err = js.Publish("foo", msg, nats.ExpectLastSequence(2))
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	expect(3, 3)

	// Now test context and timeouts.
	// Both set should fail.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = js.Publish("foo", msg, nats.MaxWait(time.Second), nats.Context(ctx))
	if err != nats.ErrContextAndTimeout {
		t.Fatalf("Expected %q, got %q", nats.ErrContextAndTimeout, err)
	}

	// Create dummy listener for timeout and context tests.
	sub, _ := nc.SubscribeSync("baz")
	defer sub.Unsubscribe()

	_, err = js.Publish("baz", msg, nats.MaxWait(time.Nanosecond))
	if err != nats.ErrTimeout {
		t.Fatalf("Expected %q, got %q", nats.ErrTimeout, err)
	}

	go cancel()
	_, err = js.Publish("baz", msg, nats.Context(ctx))
	if err != context.Canceled {
		t.Fatalf("Expected %q, got %q", context.Canceled, err)
	}
}

func TestJetStreamSubscribe(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz", "foo.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Lookup the stream for testing.
	mset, err := s.GlobalAccount().LookupStream("TEST")
	if err != nil {
		t.Fatalf("stream lookup failed: %v", err)
	}

	msg := []byte("Hello JS")

	// Basic publish like NATS core.
	js.Publish("foo", msg)

	q := make(chan *nats.Msg, 4)

	// Now create a simple ephemeral consumer.
	sub, err := js.Subscribe("foo", func(m *nats.Msg) {
		q <- m
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	select {
	case m := <-q:
		if _, err := m.MetaData(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive the messages in time")
	}

	// Now do same but sync.
	sub, err = js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	waitForPending := func(n int) {
		timeout := time.Now().Add(2 * time.Second)
		for time.Now().Before(timeout) {
			if msgs, _, _ := sub.Pending(); msgs == n {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		msgs, _, _ := sub.Pending()
		t.Fatalf("Expected to receive %d messages, but got %d", n, msgs)
	}

	waitForPending(1)

	// Make sure we are set to explicit ack for callback based subscriptions and that the messages go down etc.
	mset.Purge()
	toSend := 10
	for i := 0; i < toSend; i++ {
		js.Publish("bar", msg)
	}
	if state := mset.State(); state.Msgs != 10 {
		t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
	}

	done := make(chan bool, 1)
	var received int
	sub, err = js.Subscribe("bar", func(m *nats.Msg) {
		received++
		if received == toSend {
			done <- true
		}
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive all of the messages in time")
	}

	// If we are here we have received all of the messages.
	// We hang the ConsumerInfo option off of the subscription, so we use that to check status.
	info, _ := sub.ConsumerInfo()
	if info.Config.AckPolicy != nats.AckExplicitPolicy {
		t.Fatalf("Expected ack explicit policy, got %q", info.Config.AckPolicy)
	}
	if info.Delivered.Consumer != uint64(toSend) {
		t.Fatalf("Expected to have received all %d messages, got %d", toSend, info.Delivered.Consumer)
	}
	// Make sure we auto-ack'd
	if info.AckFloor.Consumer != uint64(toSend) {
		t.Fatalf("Expected to have ack'd all %d messages, got ack floor of %d", toSend, info.AckFloor.Consumer)
	}
	sub.Unsubscribe()

	// Now create a sync subscriber that is durable.
	dname := "derek"
	sub, err = js.SubscribeSync("foo", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	// Make sure we registered as a durable.
	if info, _ := sub.ConsumerInfo(); info.Config.Durable != dname {
		t.Fatalf("Expected durable name to be set to %q, got %q", dname, info.Config.Durable)
	}
	deliver := sub.Subject
	sub.Unsubscribe()

	// Create again and make sure that works and that we attach to the same durable with different delivery.
	sub, err = js.SubscribeSync("foo", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	if deliver == sub.Subject {
		t.Fatalf("Expected delivery subject to be different then %q", deliver)
	}
	deliver = sub.Subject

	// Now test that we can attach to an existing durable.
	sub, err = js.SubscribeSync("foo", nats.Attach(mset.Name(), dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	if deliver != sub.Subject {
		t.Fatalf("Expected delivery subject to be the same when attaching, got different")
	}

	// Now try pull based subscribers.

	// Check some error conditions first.
	if _, err := js.Subscribe("bar", func(m *nats.Msg) {}, nats.Pull(1)); err != nats.ErrPullModeNotAllowed {
		t.Fatalf("Expected an error trying to do PullMode on callback based subscriber, got %v", err)
	}

	batch := 5
	sub, err = js.SubscribeSync("bar", nats.Durable("rip"), nats.Pull(batch))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	// The first batch if available should be delivered and queued up.
	waitForPending(batch)

	if info, _ := sub.ConsumerInfo(); info.NumAckPending != batch || info.NumPending != uint64(batch) {
		t.Fatalf("Expected %d pending ack, and %d still waiting to be delivered, got %d and %d", batch, batch, info.NumAckPending, info.NumPending)
	}

	// Now go ahead and consume these and ack, but not ack+next.
	for i := 0; i < batch; i++ {
		m, err := sub.NextMsg(10 * time.Millisecond)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		m.Respond(nats.AckAck)
	}
	if info, _ := sub.ConsumerInfo(); info.AckFloor.Consumer != uint64(batch) {
		t.Fatalf("Expected ack floor to be %d, got %d", batch, info.AckFloor.Consumer)
	}

	// Now we are stuck so to speak. So we can unstick the sub by calling poll.
	waitForPending(0)
	sub.Poll()
	waitForPending(batch)
	sub.Unsubscribe()

	// Now test attaching to a pull based durable.

	// Test that if we are attaching that the subjects will match up. rip from
	// above was created with a filtered subject of bar, so this should fail.
	_, err = js.SubscribeSync("baz", nats.Attach(mset.Name(), "rip"), nats.Pull(batch))
	if err != nats.ErrSubjectMismatch {
		t.Fatalf("Expected a %q error but got %q", nats.ErrSubjectMismatch, err)
	}

	// Queue up 10 more messages.
	for i := 0; i < toSend; i++ {
		js.Publish("bar", msg)
	}

	sub, err = js.SubscribeSync("bar", nats.Attach(mset.Name(), "rip"), nats.Pull(batch))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	waitForPending(batch)

	if info, _ := sub.ConsumerInfo(); info.NumAckPending != batch*2 || info.NumPending != uint64(toSend-batch) {
		t.Fatalf("Expected ack pending of %d and pending to be %d, got %d %d", batch*2, toSend-batch, info.NumAckPending, info.NumPending)
	}

	// Create a new pull based consumer.
	batch = 1
	msgs := make(chan *nats.Msg, 100)
	sub, err = js.ChanSubscribe("baz", msgs, nats.Durable("dlc"), nats.Pull(batch))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Since this sub is on 'baz' no messages are waiting for us to start.
	waitForPending(0)

	// Now send in 10 messages to baz.
	for i := 0; i < toSend; i++ {
		js.Publish("baz", msg)
	}
	// We should get 1 queued up.
	waitForPending(batch)

	for received := 0; received < toSend; {
		select {
		case m := <-msgs:
			received++
			// This will do the AckNext version since it knows we are pull based.
			m.Ack()
		case <-time.After(time.Second):
			t.Fatalf("Timeout waiting for messages")
		}
	}

	// Prevent invalid durable names
	if _, err := js.SubscribeSync("baz", nats.Durable("test.durable")); err != nats.ErrInvalidDurableName {
		t.Fatalf("Expected invalid durable name error")
	}
}

func TestAckForNonJetStream(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	nc.PublishRequest("foo", "_INBOX_", []byte("OK"))
	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := m.Ack(); err != nats.ErrNotJSMessage {
		t.Fatalf("Expected an error of '%v', got '%v'", nats.ErrNotJSMessage, err)
	}
}

func TestJetStreamManagement(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create the stream using our client API.
	if _, err := js.AddStream(nil); err == nil {
		t.Fatalf("Unexpected success")
	}
	si, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "foo" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}

	for i := 0; i < 25; i++ {
		js.Publish("foo", []byte("hi"))
	}

	// Check info calls.
	si, err = js.StreamInfo("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "foo" {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}

	// Update the stream using our client API.
	if _, err := js.UpdateStream(nil); err == nil {
		t.Fatal("Unexpected success")
	}
	prevMaxMsgs := si.Config.MaxMsgs
	si, err = js.UpdateStream(&nats.StreamConfig{Name: "foo", MaxMsgs: prevMaxMsgs + 100})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si == nil || si.Config.Name != "foo" || si.Config.MaxMsgs == prevMaxMsgs {
		t.Fatalf("StreamInfo is not correct %+v", si)
	}

	// Create a consumer using our client API.
	ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci == nil || ci.Name != "dlc" || ci.Stream != "foo" {
		t.Fatalf("ConsumerInfo is not correct %+v", ci)
	}

	if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "test.durable"}); err != nats.ErrInvalidDurableName {
		t.Fatalf("Expected invalid durable name error")
	}

	// Check info calls.
	ci, err = js.ConsumerInfo("foo", "dlc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci == nil || ci.Config.Durable != "dlc" {
		t.Fatalf("ConsumerInfo is not correct %+v", si)
	}

	sl := js.NewStreamLister()
	if !sl.Next() {
		if err := sl.Err(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		t.Fatalf("Unexpected stream lister next")
	}
	if p := sl.Page(); len(p) != 1 || p[0].Config.Name != "foo" {
		t.Fatalf("StreamInfo is not correct %+v", p)
	}
	if err := sl.Err(); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if cl := js.NewConsumerLister(""); cl.Next() {
		t.Fatalf("Unexpected next ok")
	} else if err := cl.Err(); err == nil {
		if cl.Next() {
			t.Fatalf("Unexpected next ok")
		}
		t.Fatalf("Unexpected nil error")
	}
	cl := js.NewConsumerLister("foo")
	if !cl.Next() {
		if err := cl.Err(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		t.Fatalf("Unexpected consumer lister next")
	}
	if p := cl.Page(); len(p) != 1 || p[0].Stream != "foo" || p[0].Config.Durable != "dlc" {
		t.Fatalf("ConsumerInfo is not correct %+v", p)
	}
	if err := cl.Err(); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Delete a consumer using our client API.
	if err := js.DeleteConsumer("", ""); err == nil {
		t.Fatalf("Unexpected success")
	}
	if err := js.DeleteConsumer("foo", "dlc"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Purge a stream using our client API.
	if err := js.PurgeStream("foo"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si, err := js.StreamInfo("foo"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else if si.State.Msgs != 0 {
		t.Fatalf("StreamInfo.Msgs is not correct")
	}

	// Delete a stream using our client API.
	if err := js.DeleteStream(""); err == nil {
		t.Fatal("Unexpected success")
	}
	if err := js.DeleteStream("foo"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := js.StreamInfo("foo"); err == nil {
		t.Fatalf("Unexpected success")
	}

	t.Run("fetch account info", func(t *testing.T) {
		info, err := js.AccountInfo()
		if err != nil {
			t.Fatal(err)
		}
		if info.Limits.MaxMemory < 1 {
			t.Errorf("Expected to have memory limits, got: %v", info.Limits.MaxMemory)
		}
		if info.Limits.MaxStore < 1 {
			t.Errorf("Expected to have disk limits, got: %v", info.Limits.MaxMemory)
		}
		if info.Limits.MaxStreams != -1 {
			t.Errorf("Expected to not have stream limits, got: %v", info.Limits.MaxStreams)
		}
		if info.Limits.MaxConsumers != -1 {
			t.Errorf("Expected to not have consumer limits, got: %v", info.Limits.MaxConsumers)
		}
	})
}

func TestJetStreamManagement_DeleteMsg(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 5; i++ {
		js.Publish("foo.A", []byte("A"))
		js.Publish("foo.B", []byte("B"))
		js.Publish("foo.C", []byte("C"))
	}

	si, err := js.StreamInfo("foo")
	if err != nil {
		t.Fatal(err)
	}
	var total uint64 = 15
	if si.State.Msgs != total {
		t.Errorf("Expected %d msgs, got: %d", total, si.State.Msgs)
	}

	expected := 5
	msgs := make([]*nats.Msg, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub, err := js.Subscribe("foo.C", func(msg *nats.Msg) {
		msgs = append(msgs, msg)
		if len(msgs) == expected {
			cancel()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	sub.Unsubscribe()

	got := len(msgs)
	if got != expected {
		t.Fatalf("Expected %d, got %d", expected, got)
	}

	msg := msgs[0]
	meta, err := msg.MetaData()
	if err != nil {
		t.Fatal(err)
	}
	originalSeq := meta.Stream

	err = js.DeleteMsg("foo", originalSeq)
	if err != nil {
		t.Fatal(err)
	}

	si, err = js.StreamInfo("foo")
	if err != nil {
		t.Fatal(err)
	}
	total = 14
	if si.State.Msgs != total {
		t.Errorf("Expected %d msgs, got: %d", total, si.State.Msgs)
	}

	// There should be only 4 messages since one deleted.
	expected = 4
	msgs = make([]*nats.Msg, 0)
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub, err = js.Subscribe("foo.C", func(msg *nats.Msg) {
		msgs = append(msgs, msg)

		if len(msgs) == expected {
			cancel()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	sub.Unsubscribe()

	msg = msgs[0]
	meta, err = msg.MetaData()
	if err != nil {
		t.Fatal(err)
	}
	newSeq := meta.Stream

	// First message removed
	if newSeq <= originalSeq {
		t.Errorf("Expected %d to be higher sequence than %d", newSeq, originalSeq)
	}
}

func TestJetStreamImport(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [ { service: "$JS.API.>" },  { service: "foo" }]
			},
			U: {
				users: [ {user: rip, password: bar} ]
				imports [
					{ service: { subject: "$JS.API.>", account: JS } , to: "dlc.>" }
					{ service: { subject: "foo", account: JS } }
				]
			},
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	// Create a stream using the server directly.
	acc, _ := s.LookupAccount("JS")
	mset, err := acc.AddStream(&server.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
	})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}
	defer mset.Delete()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	// Since we import with a prefix from above we can use that when creating our JS context.
	js, err := nc.JetStream(nats.APIPrefix("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msg := []byte("Hello JS Import!")

	if _, err = js.Publish("foo", msg); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	if state := mset.State(); state.Msgs != 1 {
		t.Fatalf("Expected %d messages, got %d", 1, state.Msgs)
	}
}

func TestJetStreamImportDirectOnly(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [
					# For the stream publish.
					{ service: "ORDERS" }
					# For the pull based consumer. Response type needed for batchsize > 1
					{ service: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d1", response: stream }
					# For the push based consumer delivery and ack.
					{ stream: "p.d" }
					# For the acks. Service in case we want an ack to our ack.
					{ service: "$JS.ACK.ORDERS.*.>" }
				]
			},
			U: {
				users: [ {user: rip, password: bar} ]
				imports [
					{ service: { subject: "ORDERS", account: JS } , to: "orders" }
					{ service: { subject: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d1", account: JS } }
					{ stream:  { subject: "p.d", account: JS } }
					{ service: { subject: "$JS.ACK.ORDERS.*.>", account: JS } }
				]
			},
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	// Create a stream using the server directly.
	acc, _ := s.LookupAccount("JS")
	mset, err := acc.AddStream(&server.StreamConfig{Name: "ORDERS"})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}
	defer mset.Delete()

	// Create a pull based consumer.
	o1, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "d1", AckPolicy: server.AckExplicit})
	if err != nil {
		t.Fatalf("pull consumer create failed: %v", err)
	}
	defer o1.Delete()

	// Create a push based consumer.
	o2, err := mset.AddConsumer(&server.ConsumerConfig{Durable: "d2", AckPolicy: server.AckExplicit, DeliverSubject: "p.d"})
	if err != nil {
		t.Fatalf("push consumer create failed: %v", err)
	}
	defer o2.Delete()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.DirectOnly())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now make sure we can send to the stream.
	toSend := 100
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("orders", []byte(fmt.Sprintf("ORDER-%d", i+1))); err != nil {
			t.Fatalf("Unexpected error publishing message %d: %v", i+1, err)
		}
	}
	if state := mset.State(); state.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
	}

	// Check for correct errors.
	if _, err := js.SubscribeSync("ORDERS"); err != nats.ErrDirectModeRequired {
		t.Fatalf("Expected an error of '%v', got '%v'", nats.ErrDirectModeRequired, err)
	}

	var sub *nats.Subscription

	waitForPending := func(n int) {
		timeout := time.Now().Add(2 * time.Second)
		for time.Now().Before(timeout) {
			if msgs, _, _ := sub.Pending(); msgs == n {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		msgs, _, _ := sub.Pending()
		t.Fatalf("Expected to receive %d messages, but got %d", n, msgs)
	}

	// Do push based direct consumer.
	sub, err = js.SubscribeSync("ORDERS", nats.PushDirect("p.d"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	waitForPending(toSend)

	// Ack the messages from the push consumer.
	for i := 0; i < toSend; i++ {
		m, err := sub.NextMsg(100 * time.Millisecond)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Test that can expect an ack of the ack.
		err = m.AckSync()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	// Now pull based consumer.
	batch := 10
	sub, err = js.SubscribeSync("ORDERS", nats.PullDirect("ORDERS", "d1", batch))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	waitForPending(batch)

	for i := 0; i < toSend; i++ {
		m, err := sub.NextMsg(100 * time.Millisecond)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Tests that acks flow since we need these to do AckNext for this to work.
		err = m.Ack()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

func TestJetStreamAutoMaxAckPending(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	mset, err := s.GlobalAccount().AddStream(&server.StreamConfig{Name: "foo"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10_000

	msg := []byte("Hello")
	for i := 0; i < toSend; i++ {
		// Use plain NATS here for speed.
		nc.Publish("foo", msg)
	}
	nc.Flush()

	if state := mset.State(); state.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
	}

	// Create a consumer.
	msgs := make(chan *nats.Msg, 500)
	sub, err := js.ChanSubscribe("foo", msgs)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	expectedMaxAck, _, _ := sub.PendingLimits()

	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.Config.MaxAckPending != expectedMaxAck {
		t.Fatalf("Expected MaxAckPending to be set to %d, got %d", expectedMaxAck, ci.Config.MaxAckPending)
	}

	waitForPending := func(n int) {
		timeout := time.Now().Add(2 * time.Second)
		for time.Now().Before(timeout) {
			if msgs, _, _ := sub.Pending(); msgs == n {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		msgs, _, _ := sub.Pending()
		t.Fatalf("Expected to receive %d messages, but got %d", n, msgs)
	}

	waitForPending(expectedMaxAck)
	// We do it twice to make sure it does not go over.
	waitForPending(expectedMaxAck)

	// Now make sure we can consume them all with no slow consumers etc.
	for i := 0; i < toSend; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error receiving %d: %v", i+1, err)
		}
		m.Ack()
	}
}

func TestJetStreamInterfaces(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	var js nats.JetStream
	var jsm nats.JetStreamManager
	var jsctx nats.JetStreamContext

	// JetStream that can publish/subscribe but cannot manage streams.
	js, err = nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	js.Publish("foo", []byte("hello"))

	// JetStream context that can manage streams/consumers but cannot produce messages.
	jsm, err = nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	jsm.AddStream(&nats.StreamConfig{Name: "FOO"})

	// JetStream context that can both manage streams/consumers
	// as well as publish/subscribe.
	jsctx, err = nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	jsctx.AddStream(&nats.StreamConfig{Name: "BAR"})
	jsctx.Publish("bar", []byte("hello world"))

	publishMsg := func(js nats.JetStream, payload []byte) {
		js.Publish("foo", payload)
	}
	publishMsg(js, []byte("hello world"))
}

// WIP(dlc) - This is in support of stall based tests and processing.
func TestJetStreamPullBasedStall(t *testing.T) {
	t.SkipNow()

	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: enabled
		no_auth_user: pc
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: pc, password: foo} ]
			},
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create a stream.
	if _, err = js.AddStream(&nats.StreamConfig{Name: "STALL"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	acc, _ := s.LookupAccount("JS")
	mset, err := acc.LookupStream("STALL")
	if err != nil {
		t.Fatalf("stream lookup failed: %v", err)
	}

	msg := []byte("Hello JS!")
	toSend := 100_000
	for i := 0; i < toSend; i++ {
		// Use plain NATS here for speed.
		nc.Publish("STALL", msg)
	}
	nc.Flush()

	if state := mset.State(); state.Msgs != uint64(toSend) {
		t.Fatalf("Expected %d messages, got %d", toSend, state.Msgs)
	}

	batch := 100
	msgs := make(chan *nats.Msg, batch-2)
	sub, err := js.ChanSubscribe("STALL", msgs, nats.Durable("dlc"), nats.Pull(batch))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	for received := 0; received < toSend; {
		select {
		case m := <-msgs:
			received++
			meta, _ := m.MetaData()
			if meta.Consumer != uint64(received) {
				t.Fatalf("Missed something, wanted %d but got %d", received, meta.Consumer)
			}
			m.Ack()
		case <-time.After(time.Second):
			t.Fatalf("Timeout waiting for messages, last received was %d", received)
		}
	}
}

func TestJetStreamSubscribe_DeliverPolicy(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var publishTime time.Time

	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		if i == 5 {
			publishTime = time.Now()
		}
		js.Publish("foo", []byte(payload))
	}

	for _, test := range []struct {
		name     string
		subopt   nats.SubOpt
		expected int
	}{
		{
			"deliver.all", nats.DeliverAll(), 10,
		},
		{
			"deliver.last", nats.DeliverLast(), 1,
		},
		{
			"deliver.new", nats.DeliverNew(), 0,
		},
		{
			"deliver.starttime", nats.StartTime(publishTime), 5,
		},
		{
			"deliver.startseq", nats.StartSequence(6), 5,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			got := 0
			sub, err := js.Subscribe("foo", func(m *nats.Msg) {
				got++
				if got == test.expected {
					cancel()
				}
			}, test.subopt)

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			<-ctx.Done()
			sub.Drain()

			if got != test.expected {
				t.Fatalf("Expected %d, got %d", test.expected, got)
			}
		})
	}
}

func TestJetStreamSubscribe_AckPolicy(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		js.Publish("foo", []byte(payload))
	}

	for _, test := range []struct {
		name     string
		subopt   nats.SubOpt
		expected nats.AckPolicy
	}{
		{
			"ack-none", nats.AckNone(), nats.AckNonePolicy,
		},
		{
			"ack-all", nats.AckAll(), nats.AckAllPolicy,
		},
		{
			"ack-explicit", nats.AckExplicit(), nats.AckExplicitPolicy,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			got := 0
			totalMsgs := 10
			sub, err := js.Subscribe("foo", func(m *nats.Msg) {
				got++
				if got == totalMsgs {
					cancel()
				}
			}, test.subopt, nats.Durable(test.name))

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			<-ctx.Done()
			sub.Drain()

			if got != totalMsgs {
				t.Fatalf("Expected %d, got %d", totalMsgs, got)
			}

			ci, err := js.ConsumerInfo("TEST", test.name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ci.Config.AckPolicy != test.expected {
				t.Fatalf("Expected %v, got %v", test.expected, ci.Config.AckPolicy)
			}
		})
	}
}

func TestJetStreamSubscribe_AckDup(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("hello"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	pings := make(chan struct{}, 6)
	nc.Subscribe("$JS.ACK.TEST.>", func(msg *nats.Msg) {
		pings <- struct{}{}
	})
	nc.Flush()

	ch := make(chan error, 6)
	_, err = js.Subscribe("foo", func(m *nats.Msg) {
		// Only first ack will be sent, auto ack that will occur after
		// this won't be sent either.
		ch <- m.Ack()

		// Any following acks should fail.
		ch <- m.Ack()
		ch <- m.Nak()
		ch <- m.AckSync()
		ch <- m.Term()
		ch <- m.InProgress()
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	<-ctx.Done()
	ackErr1 := <-ch
	if ackErr1 != nil {
		t.Errorf("Unexpected error: %v", ackErr1)
	}

	for i := 0; i < 5; i++ {
		e := <-ch
		if e != nats.ErrInvalidJSAck {
			t.Errorf("Expected error: %v", e)
		}
	}
	if len(pings) != 1 {
		t.Logf("Expected to receive a single ack, got: %v", len(pings))
	}
}

func TestJetStreamSubscribe_AckDupInProgress(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("hello"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	pings := make(chan struct{}, 3)
	nc.Subscribe("$JS.ACK.TEST.>", func(msg *nats.Msg) {
		pings <- struct{}{}
	})
	nc.Flush()

	ch := make(chan error, 3)
	_, err = js.Subscribe("foo", func(m *nats.Msg) {
		// InProgress ACK can be sent any number of times.
		ch <- m.InProgress()
		ch <- m.InProgress()
		ch <- m.Ack()
	}, nats.Durable("WQ"), nats.ManualAck())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	<-ctx.Done()
	ackErr1 := <-ch
	ackErr2 := <-ch
	ackErr3 := <-ch
	if ackErr1 != nil {
		t.Errorf("Unexpected error: %v", ackErr1)
	}
	if ackErr2 != nil {
		t.Errorf("Unexpected error: %v", ackErr2)
	}
	if ackErr3 != nil {
		t.Errorf("Unexpected error: %v", ackErr3)
	}
	if len(pings) != 3 {
		t.Logf("Expected to receive multiple acks, got: %v", len(pings))
	}
}

func TestJetStream_UnsubscribeDeletesEphemeralConsumer(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	js.Publish("foo.A", []byte("A"))
	js.Publish("foo.B", []byte("B"))
	js.Publish("foo.C", []byte("C"))

	subA, err := js.SubscribeSync("foo.A")
	if err != nil {
		t.Fatal(err)
	}
	subB, err := js.SubscribeSync("foo.B", nats.Durable("B"))
	if err != nil {
		t.Fatal(err)
	}

	// There should be two consumers.
	fetchConsumers := func(t *testing.T, expected int) []*nats.ConsumerInfo {
		t.Helper()
		cl := js.NewConsumerLister("foo")
		if !cl.Next() {
			if err := cl.Err(); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			t.Fatalf("Unexpected consumer lister next")
		}
		p := cl.Page()
		if len(p) != expected {
			t.Fatalf("Expected %d consumers, got: %d", expected, len(p))
		}
		if err := cl.Err(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		return p
	}
	fetchConsumers(t, 2)

	t.Run("ephemeral consumer is deleted", func(t *testing.T) {
		err = subA.Unsubscribe()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		// Only subB should remain.
		for _, ci := range fetchConsumers(t, 1) {
			ciB, err := subB.ConsumerInfo()
			if err != nil {
				t.Fatal(err)
			}
			if ci.Name != ciB.Name {
				t.Fatalf("Expected %v, got: %v", ciB.Name, ci.Name)
			}
		}
	})

	t.Run("attached consumer not deleted", func(t *testing.T) {
		if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{
			Durable:   "wq",
			AckPolicy: nats.AckExplicitPolicy,
			// Need to specify filter subject here otherwise
			// would get messages from foo.A as well.
			FilterSubject: "foo.C",
		}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Now test that we can attach to an existing durable.
		subC, err := js.SubscribeSync("foo.C", nats.Attach("foo", "wq"), nats.Pull(1))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 2)

		msg, err := subC.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error getting message: %v", err)
		}
		got := string(msg.Data)
		expected := "C"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}

		// On unsubscribe there should still be 2 consumers.
		subC.Unsubscribe()
		fetchConsumers(t, 2)
	})

	t.Run("durable consumer not deleted", func(t *testing.T) {
		err = subB.Unsubscribe()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 2)
	})
}

func TestJetStream_UnsubscribeDeleteNoPermissions(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		no_auth_user: guest
		accounts: {
			JS: {   # User should not be able to delete consumer.
				jetstream: enabled
				users: [ {user: guest, password: "", permissions: {
					publish: { deny: "$JS.API.CONSUMER.DELETE.>" }
				}}]
			}
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	errCh := make(chan error, 2)
	nc, err := nats.Connect(s.ClientURL(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		errCh <- err
	}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	js.AddStream(&nats.StreamConfig{
		Name: "foo",
	})
	js.Publish("foo", []byte("test"))

	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}

	_, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Should fail due to lack of permissions.
	err = sub.Unsubscribe()
	if err == nil {
		t.Errorf("Unexpected success attempting to delete consumer without permissions")
	}

	select {
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for permissions error")
	case err = <-errCh:
		if !strings.Contains(err.Error(), `Permissions Violation for Publish to "$JS.API.CONSUMER.DELETE`) {
			t.Error("Expected permissionns violation error")
		}
	}
}

func TestJetStream_UnsubscribeAfterAttach(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	mc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	jsm, err := mc.JetStream()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	_, err = jsm.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fetchConsumers := func(t *testing.T, expected int) []*nats.ConsumerInfo {
		t.Helper()
		cl := jsm.NewConsumerLister("foo")
		if !cl.Next() {
			if err := cl.Err(); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			t.Fatalf("Unexpected consumer lister next")
		}
		p := cl.Page()
		if len(p) != expected {
			t.Fatalf("Expected %d consumers, got: %d", expected, len(p))
		}
		if err := cl.Err(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		return p
	}

	t.Run("with close", func(t *testing.T) {
		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.SubscribeSync("foo.A")
		if err != nil {
			t.Fatal(err)
		}
		_, err = js.SubscribeSync("foo.B", nats.Durable("B"))
		if err != nil {
			t.Fatal(err)
		}
		fetchConsumers(t, 2)

		// There will be still two consumers since Close do not unsubscribe
		// the client.  The server will have to detect this type of consumers
		// in case they do not come back.
		nc.Close()
		fetchConsumers(t, 2)
	})

	t.Run("with drain", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		nc, err := nats.Connect(s.ClientURL(), nats.ClosedHandler(func(_ *nats.Conn) {
			t.Logf("closed!")
			cancel()
		}))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		sub, err := js.SubscribeSync("foo.C")
		if err != nil {
			t.Fatal(err)
		}

		// Calls unsubscribe so there will be two subscribers again.
		sub.Drain()
		<-ctx.Done()

		time.Sleep(2 * time.Second)
		fetchConsumers(t, 2)
	})
}
