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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// JetStream enhances the client with JetStream functionality.
func (nc *Conn) JetStream(fopts ...jetstream.Option) (*jsContext, error) {
	var err error
	opts := &jetstream.Options{
		PublishStreamTimeout: 2 * time.Second,
	}
	for _, f := range fopts {
		if err = f(opts); err != nil {
			return nil, err
		}
	}

	return &jsContext{
		opts: opts,
		nc:   nc,
	}, nil
}

// jsContext provides JetStream behaviors.
type jsContext struct {
	nc   *Conn
	opts *jetstream.Options
	mu   sync.Mutex
}

// Publish makes a publish to JetStream returning the ack response.
func (js *jsContext) Publish(subj string, data []byte, fopts ...jetstream.Option) (*JetStreamPublishAck, error) {
	js.mu.Lock()
	timeout := js.opts.PublishStreamTimeout
	streamName := js.opts.StreamName
	js.mu.Unlock()

	if len(fopts) > 0 {
		o := &jetstream.Options{
			PublishStreamTimeout: timeout,
			StreamName:           streamName,
		}
		for _, f := range fopts {
			if err := f(o); err != nil {
				return nil, err
			}
		}
		timeout = o.PublishStreamTimeout
		streamName = o.StreamName
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := js.nc.RequestWithContext(ctx, subj, data)
	if err != nil {
		return nil, err
	}

	ack, err := ParsePublishAck(resp.Data)
	if err != nil {
		return nil, err
	}

	if ack.Stream == "" || ack.Sequence == 0 {
		return nil, ErrInvalidJSAck
	}

	// If we explicitly set a stream name, error in case
	// we get an ACK from a different one.
	if streamName != "" && ack.Stream != streamName {
		return nil, fmt.Errorf("received ack from stream %q", ack.Stream)
	}

	return ack, nil
}

const JSApiStreamLookup = "$JS.API.STREAM.LOOKUP"

type jSApiStreamLookupRequest struct {
	// Subject finds any stream that matches this subject
	// including those where wildcards intercepts
	Subject string `json:"subject"`
}

type jSApiStreamLookupResponse struct {
	jSApiResponse
	Stream   string `json:"stream"`
	Filtered bool   `json:"is_filtered"`
}

// Subscribe creates an ephemeral push based consumer.
func (js *jsContext) Subscribe(subj string, cb MsgHandler, fopts ...jetstream.Option) (*Subscription, error) {
	js.mu.Lock()
	jsconf := js.opts.ConsumerConfig
	streamName := js.opts.StreamName
	js.mu.Unlock()

	if jsconf == nil {
		// Default config for an ephemeral push based config
		// in case there is none already in the JetStream context.
		jsconf = &jetstream.ConsumerConfig{
			DeliverPolicy: jetstream.DeliverAll,
			AckPolicy:     jetstream.AckExplicit,
			AckWait:       5 * time.Second,
			ReplayPolicy:  jetstream.ReplayInstant,
		}
	}

	// Allow customizing/overriding via options as well.
	var deliverySubject string
	if len(fopts) > 0 {
		opts := &jetstream.Options{
			ConsumerConfig: jsconf,
			StreamName:     streamName,
		}
		for _, f := range fopts {
			if err := f(opts); err != nil {
				return nil, err
			}
		}
		jsconf = opts.ConsumerConfig
		streamName = opts.StreamName
		deliverySubject = opts.DeliverySubject
	}

	// In case of no explicit stream, then make a lookup to find
	// the subject since there ought not be any overlap.
	if streamName == "" {
		crj, err := json.Marshal(&jSApiStreamLookupRequest{subj})
		if err != nil {
			return nil, err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		resp, err := js.nc.RequestWithContext(ctx, JSApiStreamLookup, crj)
		if err != nil {
			return nil, err
		}

		cresp := &jSApiStreamLookupResponse{}
		err = json.Unmarshal(resp.Data, cresp)
		if err != nil {
			return nil, err
		}
		if cresp.Error != nil {
			return nil, cresp.Error
		}
		streamName = cresp.Stream
	}

	// We need a delivery subject for push based consumers, so we create
	// an inbox to represent that interest and bind the callback in the client.
	if deliverySubject == "" {
		deliverySubject = NewInbox()
	}
	sub, err := js.nc.subscribe(deliverySubject, _EMPTY_, cb, nil, false)
	if err != nil {
		return nil, err
	}

	// If we get a consumer config, then make API request to create or update.
	if jsconf != nil {
		jsconf.FilterSubject = subj
		cresp, err := js.createConsumer(streamName, deliverySubject, jsconf)
		if err != nil {
			sub.Unsubscribe()
			return nil, err
		}

		// Fill in the response about the consumer info config.
		sub.ConsumerConfig = &cresp.ConsumerInfo.Config
	}
	return sub, nil
}

func (js *jsContext) createConsumer(streamName string, deliverySubject string, jsconf *jetstream.ConsumerConfig) (*jSApiConsumerCreateResponse, error) {
	consumer := &ConsumerConfig{
		Durable:         jsconf.Durable,
		DeliverPolicy:   DeliverPolicy(jsconf.DeliverPolicy),
		OptStartSeq:     jsconf.OptStartSeq,
		OptStartTime:    jsconf.OptStartTime,
		AckPolicy:       AckPolicy(jsconf.AckPolicy),
		AckWait:         jsconf.AckWait,
		MaxDeliver:      jsconf.MaxDeliver,
		FilterSubject:   jsconf.FilterSubject,
		ReplayPolicy:    ReplayPolicy(jsconf.ReplayPolicy),
		SampleFrequency: jsconf.SampleFrequency,
		RateLimit:       jsconf.RateLimit,
		MaxAckPending:   jsconf.MaxAckPending,
	}
	crj, err := json.Marshal(&jSApiConsumerCreateRequest{
		Stream: streamName,
		Config: consumerConfig{
			DeliverSubject: deliverySubject,
			ConsumerConfig: consumer,
		},
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var subj string
	switch len(consumer.Durable) {
	case 0:
		subj = fmt.Sprintf(jSApiConsumerCreateT, streamName)
	default:
		subj = fmt.Sprintf(jSApiDurableCreateT, streamName, consumer.Durable)
	}

	resp, err := js.nc.RequestWithContext(ctx, subj, crj)
	if err != nil {
		return nil, err
	}

	cresp := &jSApiConsumerCreateResponse{}
	err = json.Unmarshal(resp.Data, cresp)
	if err != nil {
		return nil, err
	}

	if cresp.Error != nil {
		return nil, cresp.Error
	}

	return cresp, nil
}

const JSApiRequestNext = "$JS.API.CONSUMER.MSG.NEXT.%s.%s"

// NextMsg retrieves the next message for a pull based consumer.
func (js *jsContext) NextMsg(streamSubj string, duration time.Duration) (*Msg, error) {
	if js.opts.ConsumerConfig == nil || js.opts.ConsumerConfig.Durable == "" {
		return nil, errors.New("nats: missing durable name in ConsumerConfig")
	}
	streamName := js.opts.StreamName

	// In case of no explicit stream, then make a lookup to find
	// the subject since there ought not be any overlap.
	if streamName == "" {
		crj, err := json.Marshal(&jSApiStreamLookupRequest{streamSubj})
		if err != nil {
			return nil, err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		resp, err := js.nc.RequestWithContext(ctx, JSApiStreamLookup, crj)
		if err != nil {
			return nil, err
		}

		cresp := &jSApiStreamLookupResponse{}
		err = json.Unmarshal(resp.Data, cresp)
		if err != nil {
			return nil, err
		}
		if cresp.Error != nil {
			return nil, cresp.Error
		}
		streamName = cresp.Stream
	}

	sub, err := js.nc.SubscribeSync(NewInbox())
	if err != nil {
		return nil, err
	}
	sub.AutoUnsubscribe(1)
	subj := fmt.Sprintf(JSApiRequestNext, streamName, js.opts.ConsumerConfig.Durable)
	err = js.nc.PublishRequest(subj, sub.Subject, nil)
	if err != nil {
		return nil, err
	}
	msg, err := sub.NextMsg(duration)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// JetStreamMsgMetaData is metadata related to a JetStream originated message
type JetStreamMsgMetaData struct {
	Stream      string
	Consumer    string
	Parsed      bool
	Delivered   int
	StreamSeq   int
	ConsumerSeq int
	Pending     int
	TimeStamp   time.Time
}

func (m *Msg) JetStreamMetaData() (*JetStreamMsgMetaData, error) {
	var err error

	if m.jsMeta != nil && m.jsMeta.Parsed {
		return m.jsMeta, nil
	}

	m.jsMeta, err = m.parseJSMsgMetadata()

	return m.jsMeta, err
}

func (m *Msg) parseJSMsgMetadata() (*JetStreamMsgMetaData, error) {
	if m.jsMeta != nil {
		return m.jsMeta, nil
	}

	if len(m.Reply) == 0 {
		return nil, ErrNotJSMessage
	}

	meta := &JetStreamMsgMetaData{}

	tsa := [32]string{}
	parts := tsa[:0]
	start := 0
	btsep := byte('.')
	for i := 0; i < len(m.Reply); i++ {
		if m.Reply[i] == btsep {
			parts = append(parts, m.Reply[start:i])
			start = i + 1
		}
	}
	parts = append(parts, m.Reply[start:])
	c := len(parts)

	if (c != 8 && c != 9) || parts[0] != "$JS" || parts[1] != "ACK" {
		return nil, ErrNotJSMessage
	}

	var err error

	meta.Stream = parts[2]
	meta.Consumer = parts[3]
	meta.Delivered, err = strconv.Atoi(parts[4])
	if err != nil {
		return nil, ErrNotJSMessage
	}

	meta.StreamSeq, err = strconv.Atoi(parts[5])
	if err != nil {
		return nil, ErrNotJSMessage
	}

	meta.ConsumerSeq, err = strconv.Atoi(parts[6])
	if err != nil {
		return nil, ErrNotJSMessage
	}

	tsi, err := strconv.Atoi(parts[7])
	if err != nil {
		return nil, ErrNotJSMessage
	}
	meta.TimeStamp = time.Unix(0, int64(tsi))

	meta.Pending = -1
	if c == 9 {
		meta.Pending, err = strconv.Atoi(parts[8])
		if err != nil {
			return nil, ErrNotJSMessage
		}
	}

	meta.Parsed = true

	return meta, nil
}

const jsStreamUnspecified = "not.set"

type jsOpts struct {
	timeout time.Duration
	ctx     context.Context

	ackstr     string
	consumer   *ConsumerConfig
	streamName string
}

func newJsOpts() *jsOpts {
	return &jsOpts{ackstr: jsStreamUnspecified, consumer: &ConsumerConfig{}}
}

func (j *jsOpts) context(dftl time.Duration) (context.Context, context.CancelFunc) {
	if j.ctx != nil {
		return context.WithCancel(j.ctx)
	}

	if j.timeout == 0 {
		j.timeout = dftl
	}

	return context.WithTimeout(context.Background(), j.timeout)
}

// AckOption configures the various JetStream message acknowledgement helpers
type AckOption func(opts *jsOpts) error

// PublishOption configures publishing messages
type PublishOption func(opts *jsOpts) error

// SubscribeOption configures JetStream consumer behavior
type SubscribeOption func(opts *jsOpts) error

// Consumer creates a JetStream Consumer on a Stream
func Consumer(stream string, cfg ConsumerConfig) SubscribeOption {
	return func(jopts *jsOpts) error {
		jopts.consumer = &cfg
		jopts.streamName = stream
		return nil
	}
}

// PublishExpectsStream waits for an ack after publishing and ensure it's from a specific stream, empty arguments waits for any valid acknowledgement
func PublishExpectsStream(stream ...string) PublishOption {
	return func(opts *jsOpts) error {
		switch len(stream) {
		case 0:
			opts.ackstr = ""
		case 1:
			opts.ackstr = stream[0]
			if !isValidJSName(opts.ackstr) {
				return ErrInvalidStreamName
			}
		default:
			return ErrMultiStreamUnsupported
		}

		return nil
	}
}

// PublishStreamTimeout sets the period of time to wait for JetStream to acknowledge receipt, defaults to JetStreamTimeout option
func PublishStreamTimeout(t time.Duration) PublishOption {
	return func(opts *jsOpts) error {
		opts.timeout = t
		return nil
	}
}

// PublishCtx sets an interrupt context for waiting on a stream to reply
func PublishCtx(ctx context.Context) PublishOption {
	return func(opts *jsOpts) error {
		opts.ctx = ctx
		return nil
	}
}

// AckWaitDuration waits for confirmation from the JetStream server
func AckWaitDuration(d time.Duration) AckOption {
	return func(opts *jsOpts) error {
		opts.timeout = d
		return nil
	}
}

func (m *Msg) jsAck(body []byte, opts ...AckOption) error {
	if m.Reply == "" {
		return ErrMsgNoReply
	}

	if m == nil || m.Sub == nil {
		return ErrMsgNotBound
	}

	m.Sub.mu.Lock()
	nc := m.Sub.conn
	m.Sub.mu.Unlock()

	var err error
	var aopts *jsOpts

	if len(opts) > 0 {
		aopts = newJsOpts()
		for _, f := range opts {
			if err = f(aopts); err != nil {
				return err
			}
		}
	}

	if aopts == nil || aopts.timeout == 0 {
		return m.Respond(body)
	}

	_, err = nc.Request(m.Reply, body, aopts.timeout)

	return err
}

// Ack acknowledges a JetStream messages received from a Consumer, indicating the message
// should not be received again later
func (m *Msg) Ack(opts ...AckOption) error {
	return m.jsAck(AckAck, opts...)
}

// Nak acknowledges a JetStream message received from a Consumer, indicating that the message
// is not completely processed and should be sent again later
func (m *Msg) Nak(opts ...AckOption) error {
	return m.jsAck(AckNak, opts...)
}

// AckProgress acknowledges a Jetstream message received from a Consumer, indicating that work is
// ongoing and further processing time is required equal to the configured AckWait of the Consumer
func (m *Msg) AckProgress(opts ...AckOption) error {
	return m.jsAck(AckProgress, opts...)
}

// AckNextRequest is parameters used to request the next message while Acknowledging a message
type AckNextRequest struct {
	// Expires is the time when the server will stop honoring this request
	Expires time.Time `json:"expires,omitempty"`
	// Batch is how many messages to request
	Batch int `json:"batch,omitempty"`
	// NoWait indicates that if the Consumer has consumed all messages an Msg with Status header set to 404
	NoWait bool `json:"no_wait,omitempty"`
}

// AckNextRequest performs an acknowledgement of a message and request the next messages based on req
func (m *Msg) AckNextRequest(req *AckNextRequest) error {
	if req == nil {
		return m.AckNext()
	}

	if m == nil || m.Sub == nil {
		return ErrMsgNotBound
	}

	rj, err := json.Marshal(req)
	if err != nil {
		return err
	}

	return m.RespondMsg(&Msg{Subject: m.Reply, Reply: m.Sub.Subject, Data: append(AckNext, append([]byte{' '}, rj...)...)})
}

// AckNext performs an Ack() and request the next message, to request multiple messages use AckNextRequest()
func (m *Msg) AckNext() error {
	if m == nil || m.Sub == nil {
		return ErrMsgNotBound
	}

	return m.RespondMsg(&Msg{Subject: m.Reply, Reply: m.Sub.Subject, Data: AckNext})
}

// AckAndFetch performs an AckNext() and returns the next message from the stream
func (m *Msg) AckAndFetch(opts ...AckOption) (*Msg, error) {
	if m.Reply == "" {
		return nil, ErrMsgNoReply
	}

	if m == nil || m.Sub == nil {
		return nil, ErrMsgNotBound
	}

	m.Sub.mu.Lock()
	nc := m.Sub.conn
	m.Sub.mu.Unlock()

	var err error

	aopts := newJsOpts()
	for _, f := range opts {
		if err = f(aopts); err != nil {
			return nil, err
		}
	}

	ctx, cancel := aopts.context(nc.Opts.JetStreamTimeout)
	defer cancel()

	sub, err := nc.SubscribeSync(NewInbox())
	if err != nil {
		return nil, err
	}
	sub.AutoUnsubscribe(1)
	defer sub.Unsubscribe()

	err = m.RespondMsg(&Msg{Reply: sub.Subject, Data: AckNext, Subject: m.Reply})
	if err != nil {
		return nil, err
	}
	nc.Flush()

	return sub.NextMsgWithContext(ctx)
}

// AckTerm acknowledges a message received from JetStream indicating the message will not be processed
// and should not be sent to another consumer
func (m *Msg) AckTerm(opts ...AckOption) error {
	return m.jsAck(AckTerm, opts...)
}

// JetStreamPublishAck metadata received from JetStream when publishing messages
type JetStreamPublishAck struct {
	Stream   string `json:"stream"`
	Sequence int    `json:"seq"`
}

// ParsePublishAck parses the publish acknowledgement sent by JetStream
func ParsePublishAck(m []byte) (*JetStreamPublishAck, error) {
	if bytes.HasPrefix([]byte("-ERR"), m) {
		if len(m) > 7 {
			return nil, fmt.Errorf(string(m[6 : len(m)-1]))
		}

		return nil, fmt.Errorf(string(m))
	}

	if !bytes.HasPrefix(m, []byte("+OK {")) {
		return nil, fmt.Errorf("invalid JetStream Ack: %v", string(m))
	}

	ack := &JetStreamPublishAck{}
	err := json.Unmarshal(m[3:], ack)
	return ack, err
}

func (nc *Conn) jsPublish(subj string, data []byte, opts []PublishOption) error {
	var err error
	var aopts *jsOpts

	if len(opts) > 0 {
		aopts = newJsOpts()
		for _, f := range opts {
			if err = f(aopts); err != nil {
				return err
			}
		}
	}

	if aopts == nil || aopts.timeout == 0 && aopts.ctx == nil && aopts.ackstr == jsStreamUnspecified {
		return nc.publish(subj, _EMPTY_, nil, data)
	}

	ctx, cancel := aopts.context(nc.Opts.JetStreamTimeout)
	defer cancel()

	resp, err := nc.RequestWithContext(ctx, subj, data)
	if err != nil {
		return err
	}

	ack, err := ParsePublishAck(resp.Data)
	if err != nil {
		return err
	}

	if ack.Stream == "" || ack.Sequence == 0 {
		return ErrInvalidJSAck
	}

	if aopts.ackstr == jsStreamUnspecified || aopts.ackstr == "" {
		return nil
	}

	if ack.Stream == aopts.ackstr {
		return nil
	}

	return fmt.Errorf("received ack from stream %q", ack.Stream)
}
