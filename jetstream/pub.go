package jetstream

import (
	"context"
	"time"
)

// PubOpt configures options for publishing jetstream messages.
type PubOpt func(opts *PubOpts) error

type PubOpts struct {
	Context   context.Context
	Ttl       time.Duration
	MsgId     string
	LastMsgId string // Expected last msgId
	Stream    string // Expected stream name
	Seq       uint64 // Expected last sequence
}

// MsgId sets the message ID used for de-duplication.
func MsgId(id string) PubOpt {
	return func(opts *PubOpts) error {
		opts.MsgId = id
		return nil
	}
}

// ExpectStream sets the expected stream to respond from the publish.
func ExpectStream(stream string) PubOpt {
	return func(opts *PubOpts) error {
		opts.Stream = stream
		return nil
	}
}

// ExpectLastSequence sets the expected sequence in the response from the publish.
func ExpectLastSequence(seq uint64) PubOpt {
	return func(opts *PubOpts) error {
		opts.Seq = seq
		return nil
	}
}

// ExpectLastSequence sets the expected sequence in the response from the publish.
func ExpectLastMsgId(id string) PubOpt {
	return func(opts *PubOpts) error {
		opts.LastMsgId = id
		return nil
	}
}

// MaxWait sets the maximum amount of time we will wait for a response from JetStream.
func MaxWait(ttl time.Duration) PubOpt {
	return func(opts *PubOpts) error {
		opts.Ttl = ttl
		return nil
	}
}

// Context sets the contect to make the call to JetStream.
func Context(ctx context.Context) PubOpt {
	return func(opts *PubOpts) error {
		opts.Context = ctx
		return nil
	}
}
