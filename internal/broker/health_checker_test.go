package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/omsu-informational-systems/alexey-shishin/internal/health"
)

func TestLivenessCheckerStartsUnknown(t *testing.T) {
	c := NewLivenessChecker("candle-reader")

	status, _ := c.Check(context.Background())
	if status != health.StatusUNKNOWN {
		t.Fatalf("expected UNKNOWN before any Record call, got %s", status)
	}
}

func TestLivenessCheckerUpAfterSuccess(t *testing.T) {
	c := NewLivenessChecker("candle-reader")
	c.Record(nil)

	status, details := c.Check(context.Background())
	if status != health.StatusUP {
		t.Fatalf("expected UP, got %s", status)
	}
	if details != "" {
		t.Fatalf("expected empty details on UP, got %q", details)
	}
}

func TestLivenessCheckerDownAfterFailureDoesNotMaskOthers(t *testing.T) {
	heartbeat := NewLivenessChecker("heartbeat-producer")
	registration := NewLivenessChecker("registration-reader")
	candles := NewLivenessChecker("candle-reader")

	heartbeat.Record(nil)
	registration.Record(nil)
	candles.Record(errors.New("connection refused"))

	registry := health.NewRegistry(heartbeat, registration, candles)
	report := registry.Aggregate(context.Background())

	if report.Components["candle-reader"].Status != health.StatusDOWN {
		t.Fatalf("expected candle-reader DOWN, got %v", report.Components["candle-reader"])
	}
	if report.Components["heartbeat-producer"].Status != health.StatusUP {
		t.Fatalf("expected heartbeat-producer to remain UP despite candle-reader failure, got %v", report.Components["heartbeat-producer"])
	}
	if report.Status != health.StatusDEGRADED {
		t.Fatalf("expected overall DEGRADED, got %s", report.Status)
	}
}
