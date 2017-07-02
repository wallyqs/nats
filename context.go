// Copyright 2012-2017 Apcera Inc. All rights reserved.

// +build go1.7

// A Go client for the NATS messaging system (https://nats.io).
package nats

import (
	"context"
	"fmt"
	"reflect"
)

// RequestWithContext takes a context, a subject and payload
// in bytes and request expecting a single response.
func (nc *Conn) RequestWithContext(ctx context.Context, subj string, data []byte) (*Msg, error) {
	if ctx == nil {
		return nil, ErrInvalidContext
	}
	if nc == nil {
		return nil, ErrInvalidConnection
	}

	nc.mu.Lock()
	// If user wants the old style.
	if nc.Opts.UseOldRequestStyle {
		nc.mu.Unlock()
		return nc.oldRequestWithContext(ctx, subj, data)
	}

	// Do setup for the new style.
	if nc.respMap == nil {
		// _INBOX wildcard
		nc.respSub = fmt.Sprintf("%s.*", NewInbox())
		nc.respMap = make(map[string]chan *Msg)
	}
	// Create literal Inbox and map to a chan msg.
	mch := make(chan *Msg, RequestChanLen)
	respInbox := nc.newRespInbox()
	token := respToken(respInbox)
	nc.respMap[token] = mch
	createSub := nc.respMux == nil
	ginbox := nc.respSub
	nc.mu.Unlock()

	if createSub {
		// Make sure scoped subscription is setup only once.
		var err error
		nc.respSetup.Do(func() { err = nc.createRespMux(ginbox) })
		if err != nil {
			return nil, err
		}
	}

	err := nc.PublishRequest(subj, respInbox, data)
	if err != nil {
		return nil, err
	}

	var ok bool
	var msg *Msg

	select {
	case msg, ok = <-mch:
		if !ok {
			return nil, ErrConnectionClosed
		}
	case <-ctx.Done():
		nc.mu.Lock()
		delete(nc.respMap, token)
		nc.mu.Unlock()
		return nil, ctx.Err()
	}

	return msg, nil
}

// oldRequestWithContext utilizes inbox and subscription per request.
func (nc *Conn) oldRequestWithContext(ctx context.Context, subj string, data []byte) (*Msg, error) {
	inbox := NewInbox()
	ch := make(chan *Msg, RequestChanLen)

	s, err := nc.subscribe(inbox, _EMPTY_, nil, ch)
	if err != nil {
		return nil, err
	}
	s.AutoUnsubscribe(1)
	defer s.Unsubscribe()

	err = nc.PublishRequest(subj, inbox, data)
	if err != nil {
		return nil, err
	}

	return s.NextMsgWithContext(ctx)
}

// NextMsgWithContext takes a context and returns the next message
// available to a synchronous subscriber, blocking until it is delivered
// or context gets canceled.
func (s *Subscription) NextMsgWithContext(ctx context.Context) (*Msg, error) {
	if ctx == nil {
		return nil, ErrInvalidContext
	}
	if s == nil {
		return nil, ErrBadSubscription
	}

	s.mu.Lock()
	err := s.validateNextMsgState()
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}

	// snapshot
	mch := s.mch
	s.mu.Unlock()

	var ok bool
	var msg *Msg

	select {
	case msg, ok = <-mch:
		if !ok {
			return nil, ErrConnectionClosed
		}
		err := s.processNextMsgDelivered(msg)
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return msg, nil
}

// RequestWithContext will create an Inbox and perform a Request
// using the provided cancellation context with the Inbox reply
// for the data v. A response will be decoded into the vPtrResponse.
func (c *EncodedConn) RequestWithContext(ctx context.Context, subject string, v interface{}, vPtr interface{}) error {
	if ctx == nil {
		return ErrInvalidContext
	}

	b, err := c.Enc.Encode(subject, v)
	if err != nil {
		return err
	}
	m, err := c.Conn.RequestWithContext(ctx, subject, b)
	if err != nil {
		return err
	}
	if reflect.TypeOf(vPtr) == emptyMsgType {
		mPtr := vPtr.(*Msg)
		*mPtr = *m
	} else {
		err := c.Enc.Decode(m.Subject, m.Data, vPtr)
		if err != nil {
			return err
		}
	}

	return nil
}

var emptyMsg = []byte("")

// ActiveRequest takes a context, a subject and payload
// in bytes, and makes a request expecting a single response.
func (nc *Conn) ActiveRequest(
	ctx context.Context,
	subj string,
	data []byte,
) (*Msg, error) {
	if ctx == nil {
		return nil, ErrInvalidContext
	}
	if nc == nil {
		return nil, ErrInvalidConnection
	}

	// Special inbox that the remote can subscribe to
	// for cancellation propagation.
	respInbox := nc.newRespInbox()
	cInbox := fmt.Sprintf("_CANCEL.%s", respInbox)

	// Requestor borrows the cancellation inbox during
	// the setup stage of the request to receive an ack
	// from the subscriber handling the request that it
	// can proceed to handle the request with cancellation
	// propagation enabled.
	cs, err := nc.SubscribeSync(cInbox + ".ack")
	if err != nil {
		return nil, err
	}
	cs.AutoUnsubscribe(1)
	defer cs.Unsubscribe()

	// Regular inbox for the request is tagged with
	// cancelation inbox as part of subject.
	inbox := fmt.Sprintf("%s.%s", NewInbox(), respInbox)
	s, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	s.AutoUnsubscribe(1)
	defer s.Unsubscribe()

	// Server roundtrip at this point to ensure ordering.
	nc.Flush()

	// Make the regular request and wait for reply
	// from server telling us that we can continue.
	err = nc.PublishRequest(subj, inbox, data)
	if err != nil {
		return nil, err
	}

	// Wait for the remote to reply back telling us
	// that its cancellation context is ready.
	ack, err := cs.NextMsgWithContext(ctx)
	if err != nil {
		return nil, err
	}
	// Tell active requestor that can proceed to wait
	// for the message and that cancellation would propagate.
	nc.Publish(ack.Reply, emptyMsg)

	// Once cancellation context is setup, then wait
	// for the reply from the original request.
	msg, err := s.NextMsgWithContext(ctx)
	if err != nil {
		// At this point we have given up waiting for the message,
		// for example by cancellation propagation from parent context
		// being cancelled.  We publish into cancellation inbox to signal
		// the intereste subscribers which might be working that the
		// consumer will be going away so that they cancel as well.
		nc.Publish(cInbox, emptyMsg)

		return nil, err
	}

	return msg, nil
}

// ActiveSubscribe wraps a regular subscription with a context
// and cancellation propagation semantics by using a special inbox
// which the requestor can use to signal that is going away
// so do not need the rest of the processing being done.
func (nc *Conn) ActiveSubscribe(
	ctx context.Context,
	subj string,
	cb func(context.Context, context.CancelFunc, *Msg),
) (*Subscription, error) {
	return nc.Subscribe(subj, func(m *Msg) {
		// Chomp reply inbox to craft one for cancellation of this request.
		inbox := m.Reply[respInboxPrefixLen:]
		cInbox := fmt.Sprintf("_CANCEL.%s", inbox)

		// Cancellation subscription which aborts further processing.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		as, err := nc.Subscribe(cInbox, func(cm *Msg) {
			// Propagate cancellation via context.
			cancel()
		})
		if err != nil {
			return
		}
		defer as.Unsubscribe()

		// Ensure that cancellation subscription has been
		// processed by server before continuing...
		nc.Flush()

		// We need to reply back once to client to signal
		// that we are ready to conform to the cancellation protocol.
		nc.Publish(cInbox+".ack", emptyMsg)

		// Start working under new child context which
		// can be triggered to be cancelled by the remote.
		cb(ctx, cancel, m)
	})
}
