package broker

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	defaultMaxAttempts = 5
	defaultBackoffMin  = 200 * time.Millisecond
	defaultBackoffMax  = 5 * time.Second
	defaultDialTimeout = 10 * time.Second
)

type Credentials struct {
	Brokers  []string
	Username string
	Password string
}

func NewDialer(creds Credentials) (*kafka.Dialer, error) {
	mechanism, err := scram.Mechanism(scram.SHA256, creds.Username, creds.Password)
	if err != nil {
		return nil, fmt.Errorf("broker: build SCRAM mechanism: %w", err)
	}
	return &kafka.Dialer{
		Timeout:       defaultDialTimeout,
		DualStack:     true,
		TLS:           &tls.Config{},
		SASLMechanism: mechanism,
	}, nil
}

func NewTransport(creds Credentials) (*kafka.Transport, error) {
	mechanism, err := scram.Mechanism(scram.SHA256, creds.Username, creds.Password)
	if err != nil {
		return nil, fmt.Errorf("broker: build SCRAM mechanism: %w", err)
	}
	return &kafka.Transport{
		TLS:  &tls.Config{},
		SASL: mechanism,
	}, nil
}

func GroupID(base, suffix string) string {
	return base + "." + suffix
}

func backoffDuration(attempt int, min, max time.Duration) time.Duration {
	if min <= 0 {
		min = defaultBackoffMin
	}
	if max <= 0 {
		max = defaultBackoffMax
	}
	d := min
	for i := 1; i < attempt; i++ {
		d *= 2
		if d > max {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}

func sleepOrDone(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
