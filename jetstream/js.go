package jetstream

import (
	"strings"
	"time"
)

// Option configures options for the JetStream context.
type Option func(opts *Options) error

type Options struct {
	// For importing JetStream from other accounts.
	Prefix string

	// Amount of time to wait for API requests.
	Wait time.Duration

	// Signals only direct access and no API access.
	Direct bool
}

func ApiPrefix(pre string) Option {
	return func(js *Options) error {
		js.Prefix = pre
		if !strings.HasSuffix(js.Prefix, ".") {
			js.Prefix = js.Prefix + "."
		}
		return nil
	}
}

func ApiRequestWait(wait time.Duration) Option {
	return func(js *Options) error {
		js.Wait = wait
		return nil
	}
}

func DirectOnly() Option {
	return func(js *Options) error {
		js.Direct = true
		return nil
	}
}
