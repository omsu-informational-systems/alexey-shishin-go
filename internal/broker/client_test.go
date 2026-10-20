package broker

import (
	"context"
	"testing"
	"time"
)

func TestNewDialerBuildsSCRAMSHA256Mechanism(t *testing.T) {
	dialer, err := NewDialer(Credentials{Username: "student", Password: "pwd"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dialer.SASLMechanism == nil {
		t.Fatalf("expected SASL mechanism to be set")
	}
	if dialer.SASLMechanism.Name() != "SCRAM-SHA-256" {
		t.Fatalf("expected SCRAM-SHA-256, got %q", dialer.SASLMechanism.Name())
	}
	if dialer.TLS == nil {
		t.Fatalf("expected TLS to be enabled for the dialer")
	}
}

func TestNewTransportBuildsSCRAMSHA256Mechanism(t *testing.T) {
	transport, err := NewTransport(Credentials{Username: "student", Password: "pwd"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if transport.SASL == nil {
		t.Fatalf("expected SASL mechanism to be set")
	}
	if transport.SASL.Name() != "SCRAM-SHA-256" {
		t.Fatalf("expected SCRAM-SHA-256, got %q", transport.SASL.Name())
	}
	if transport.TLS == nil {
		t.Fatalf("expected TLS to be enabled for the transport")
	}
}

func TestGroupIDAddsSuffixKeepingBaseUnique(t *testing.T) {
	base := "student-vwap"
	registration := GroupID(base, "registration")
	candles := GroupID(base, "candles")

	if registration == candles {
		t.Fatalf("expected distinct group ids, got the same value %q", registration)
	}
	if registration != "student-vwap.registration" {
		t.Fatalf("unexpected registration group id: %q", registration)
	}
	if candles != "student-vwap.candles" {
		t.Fatalf("unexpected candles group id: %q", candles)
	}
}

func TestBackoffDurationGrowsExponentiallyAndCaps(t *testing.T) {
	min := 100 * time.Millisecond
	max := 1 * time.Second

	if got := backoffDuration(1, min, max); got != min {
		t.Fatalf("attempt 1: expected %v, got %v", min, got)
	}
	if got := backoffDuration(2, min, max); got != 2*min {
		t.Fatalf("attempt 2: expected %v, got %v", 2*min, got)
	}
	if got := backoffDuration(3, min, max); got != 4*min {
		t.Fatalf("attempt 3: expected %v, got %v", 4*min, got)
	}
	if got := backoffDuration(10, min, max); got != max {
		t.Fatalf("attempt 10: expected capped at %v, got %v", max, got)
	}
}

func TestSleepOrDoneReturnsContextErrorWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := sleepOrDone(ctx, time.Second); err == nil {
		t.Fatalf("expected context error, got nil")
	}
}
