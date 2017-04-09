// Copyright 2012-2017 Apcera Inc. All rights reserved.

// +build go1.7

// A Go client for the NATS messaging system (https://nats.io).
package nats

import "context"

// RequestWithContext takes a context, a subject and payload in bytes
// and request expecting a single response.
func (nc *Conn) RequestWithContext(
	ctx context.Context,
	subj string,
	data []byte,
) (*Msg, error) {
	inbox := NewInbox()
	ch := make(chan *Msg, RequestChanLen)
	recvCh := make(chan *Msg, 1)
	var recvMsg *Msg

	s, err := nc.subscribe(inbox, _EMPTY_, func(msg *Msg) {
		recvCh <- msg
	}, ch)

	if err != nil {
		// If we errored here but context has been canceled
		// then still return the error from context.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		return nil, err
	}
	s.AutoUnsubscribe(1)
	defer s.Unsubscribe()
	defer close(ch)

	err = nc.PublishRequest(subj, inbox, data)
	if err != nil {
		// Still prefer error from context
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		return nil, err
	}

	select {
	case <-ctx.Done():
		// Context has been canceled so return opaque error
		return nil, ctx.Err()
	case recvMsg = <-recvCh:
		break
	}
	return recvMsg, nil
}

// NextMsgWithContext takes a context and returns the next message available
// to a synchronous subscriber, blocking until either one is available or
// context gets canceled.
func (s *Subscription) NextMsgWithContext(ctx context.Context) (*Msg, error) {
	if ctx == nil {
		panic("nil context")
	}
	if s == nil {
		return nil, ErrBadSubscription
	}

	s.mu.Lock()
	if s.connClosed {
		s.mu.Unlock()
		return nil, ErrConnectionClosed
	}
	if s.mch == nil {
		if s.max > 0 && s.delivered >= s.max {
			s.mu.Unlock()
			return nil, ErrMaxMessages
		} else if s.closed {
			s.mu.Unlock()
			return nil, ErrBadSubscription
		}
	}
	if s.mcb != nil {
		s.mu.Unlock()
		return nil, ErrSyncSubRequired
	}
	if s.sc {
		s.sc = false
		s.mu.Unlock()
		return nil, ErrSlowConsumer
	}

	// snapshot
	nc := s.conn
	mch := s.mch
	max := s.max
	s.mu.Unlock()

	var ok bool
	var msg *Msg

	select {
	case msg, ok = <-mch:
		if !ok {
			return nil, ErrConnectionClosed
		}
		// Update some stats.
		s.mu.Lock()
		s.delivered++
		delivered := s.delivered
		if s.typ == SyncSubscription {
			s.pMsgs--
			s.pBytes -= len(msg.Data)
		}
		s.mu.Unlock()

		if max > 0 {
			if delivered > max {
				return nil, ErrMaxMessages
			}
			// Remove subscription if we have reached max.
			if delivered == max {
				nc.mu.Lock()
				nc.removeSub(s)
				nc.mu.Unlock()
			}
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return msg, nil
}
