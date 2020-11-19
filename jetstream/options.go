package jetstream

import (
	"time"
)

type Option func(opts *Options) error

type Options struct {
	StreamName           string
	PublishStreamTimeout time.Duration
	ConsumerConfig       *ConsumerConfig
	DeliverySubject      string
}

func Stream(name string) func(opts *Options) error {
	return func(opts *Options) error {
		opts.StreamName = name
		return nil
	}
}

func PublishStreamTimeout(timeout time.Duration) func(opts *Options) error {
	return func(opts *Options) error {
		opts.PublishStreamTimeout = timeout
		return nil
	}
}

func Consumer(cfg *ConsumerConfig) func(opts *Options) error {
	return func(opts *Options) error {
		opts.ConsumerConfig = cfg
		return nil
	}
}

func DeliverySubject(name string) func(opts *Options) error {
	return func(opts *Options) error {
		opts.DeliverySubject = name
		return nil
	}
}
