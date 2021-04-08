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
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func TestBasicHeaders(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer nc.Close()

	subject := "headers.test"
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("Could not subscribe to %q: %v", subject, err)
	}
	defer sub.Unsubscribe()

	m := nats.NewMsg(subject)
	m.Header.Add("Accept-Encoding", "json")
	m.Header.Add("Authorization", "s3cr3t")
	m.Data = []byte("Hello Headers!")

	nc.PublishMsg(m)
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Did not receive response: %v", err)
	}

	// Blank out the sub since its not present in the original.
	msg.Sub = nil
	if !reflect.DeepEqual(m, msg) {
		t.Fatalf("Messages did not match! \n%+v\n%+v\n", m, msg)
	}
}

func TestRequestMsg(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer nc.Close()

	subject := "headers.test"
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		if m.Header.Get("Hdr-Test") != "1" {
			m.Respond([]byte("-ERR"))
		}

		r := nats.NewMsg(m.Reply)
		r.Header = m.Header
		r.Data = []byte("+OK")
		m.RespondMsg(r)
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Unsubscribe()

	msg := nats.NewMsg(subject)
	msg.Header.Add("Hdr-Test", "1")
	resp, err := nc.RequestMsg(msg, time.Second)
	if err != nil {
		t.Fatalf("Expected request to be published: %v", err)
	}
	if string(resp.Data) != "+OK" {
		t.Fatalf("Headers were not published to the requestor")
	}
	if resp.Header.Get("Hdr-Test") != "1" {
		t.Fatalf("Did not receive header in response")
	}
}

func TestNoHeaderSupport(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.NoHeaderSupport = true
	s := RunServerWithOptions(opts)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer nc.Close()

	m := nats.NewMsg("foo")
	m.Header.Add("Authorization", "s3cr3t")
	m.Data = []byte("Hello Headers!")

	if err := nc.PublishMsg(m); err != nats.ErrHeadersNotSupported {
		t.Fatalf("Expected an error, got %v", err)
	}

	if _, err := nc.RequestMsg(m, time.Second); err != nats.ErrHeadersNotSupported {
		t.Fatalf("Expected an error, got %v", err)
	}
}

func TestMsgHeadersAcrossHandlers(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer nc.Close()

	// Validate that headers are preserved across hops.
	errCh := make(chan error, 2)
	msgCh := make(chan *nats.Msg, 1)
	sub, err := nc.Subscribe("nats.svc.A", func(m *nats.Msg) {
		// A -> B
		m.Subject = "nats.svc.B"
		m.Header.Add("X-Result-A", "A")
		resp, err := nc.RequestMsg(m, 2*time.Second)
		if err != nil {
			errCh <- err
			return
		}

		// Respond to request from HTTP Handler.
		resp.Subject = m.Reply
		err = nc.PublishMsg(resp)
		if err != nil {
			errCh <- err
			return
		}
	})
	defer sub.Unsubscribe()

	sub, err = nc.Subscribe("nats.svc.B", func(m *nats.Msg) {
		m.Header.Add("X-Result-B", "B")
		m.Subject = m.Reply

		// B -> A
		err := nc.PublishMsg(m)
		if err != nil {
			errCh <- err
			return
		}
	})
	defer sub.Unsubscribe()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := nats.NewMsg("nats.svc.A")
		msg.Header = r.Header.Clone()
		resp, err := nc.RequestMsg(msg, 2*time.Second)
		if err != nil {
			errCh <- err
			return
		}
		msgCh <- resp

		// Add headers to HTTP handler.
		for k, v := range resp.Header {
			w.Header()[k] = v
		}

		// Remove Date for testing.
		w.Header()["Date"] = nil

		w.WriteHeader(200)
		fmt.Fprintln(w, string(resp.Data))
	}))
	defer ts.Close()

	req, err := http.NewRequest("GET", ts.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("X-Begin-Request", "start")

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var msg *nats.Msg
	select {
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message.")
	case err = <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case msg = <-msgCh:
	}

	result := resp.Header.Get("X-Result-A")
	if result != "A" {
		t.Errorf("Unexpected header value, got: %+v", result)
	}
	result = resp.Header.Get("X-Result-B")
	if result != "B" {
		t.Errorf("Unexpected header value, got: %+v", result)
	}

	if len(msg.Header) != 5 {
		t.Errorf("Wrong number of headers in NATS message, got: %v", len(msg.Header))
	}
}
