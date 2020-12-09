package jetstream

import "errors"

// SubOpt configures options for subscribing to JetStream consumers.
type SubOpt func(opts *SubOpts) error

type SubOpts struct {
	// For attaching.
	Stream, Consumer string

	// For pull based consumers, batch size for pull
	Pull int

	// For manual ack
	ManualAck bool

	// For creating or updating.
	DeliverSubject string

	// Durable is the name of a
	Durable string
}

func Durable(name string) SubOpt {
	return func(opts *SubOpts) error {
		opts.Durable = name
		return nil
	}
}

func Attach(stream, consumer string) SubOpt {
	return func(opts *SubOpts) error {
		opts.Stream = stream
		opts.Consumer = consumer
		return nil
	}
}

func Pull(batchSize int) SubOpt {
	return func(opts *SubOpts) error {
		if batchSize == 0 {
			return errors.New("nats: batch size of 0 not valid")
		}
		opts.Pull = batchSize
		return nil
	}
}

func PullDirect(stream, consumer string, batchSize int) SubOpt {
	return func(opts *SubOpts) error {
		if batchSize == 0 {
			return errors.New("nats: batch size of 0 not valid")
		}
		opts.Stream = stream
		opts.Consumer = consumer
		opts.Pull = batchSize
		return nil
	}
}

func PushDirect(deliverSubject string) SubOpt {
	return func(opts *SubOpts) error {
		opts.DeliverSubject = deliverSubject
		return nil
	}
}

func ManualAck() SubOpt {
	return func(opts *SubOpts) error {
		opts.ManualAck = true
		return nil
	}
}
