package broker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/omsu-informational-systems/alexey-shishin/internal/events"
)

type fakeWriter struct {
	failTimes int
	calls     int
	closed    bool
	lastMsgs  []kafka.Message
}

func (f *fakeWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	f.calls++
	f.lastMsgs = msgs
	if f.calls <= f.failTimes {
		return errors.New("transient write failure")
	}
	return nil
}

func (f *fakeWriter) Close() error {
	f.closed = true
	return nil
}

func TestProduceSucceedsAfterTransientRetries(t *testing.T) {
	w := &fakeWriter{failTimes: 2}
	liveness := NewLivenessChecker("heartbeat-producer")
	p := newKafkaProducer(w, nil, liveness, 5, time.Millisecond, 5*time.Millisecond)

	event := events.NewEnvelope("account.service.heartbeat", "corr-1", "vwap-reversion", nil, events.HeartbeatPayload{})

	if err := p.Produce(context.Background(), "account.service.heartbeat", "acc-1", event); err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}
	if w.calls != 3 {
		t.Fatalf("expected 3 attempts (2 failures + 1 success), got %d", w.calls)
	}

	status, _ := liveness.Check(context.Background())
	if status != "UP" {
		t.Fatalf("expected liveness UP after success, got %s", status)
	}
}

func TestProduceGivesUpAfterMaxAttemptsAndRecordsDown(t *testing.T) {
	w := &fakeWriter{failTimes: 100}
	liveness := NewLivenessChecker("heartbeat-producer")
	p := newKafkaProducer(w, nil, liveness, 3, time.Millisecond, time.Millisecond)

	event := events.NewEnvelope("account.service.heartbeat", "corr-1", "vwap-reversion", nil, events.HeartbeatPayload{})

	err := p.Produce(context.Background(), "account.service.heartbeat", "acc-1", event)
	if err == nil {
		t.Fatalf("expected error after exhausting attempts")
	}
	if w.calls != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", w.calls)
	}

	status, details := liveness.Check(context.Background())
	if status != "DOWN" {
		t.Fatalf("expected liveness DOWN after exhausted retries, got %s", status)
	}
	if details == "" {
		t.Fatalf("expected non-empty details on DOWN status")
	}
}

func TestProduceNeverLogsSecretPayload(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	w := &fakeWriter{failTimes: 2}
	p := newKafkaProducer(w, logger, nil, 5, time.Millisecond, time.Millisecond)

	const secret = "top-secret-kafka-password"
	event := events.NewEnvelope("account.registration.request", "corr-1", "vwap-reversion", nil,
		events.RegistrationRequestPayload{SecretKey: secret, AccountID: "acc-1"})

	if err := p.Produce(context.Background(), "account.registration.request", "acc-1", event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(w.lastMsgs) != 1 {
		t.Fatalf("expected exactly one message written")
	}
	if w.lastMsgs[0].Topic != "account.registration.request" {
		t.Fatalf("unexpected topic: %q", w.lastMsgs[0].Topic)
	}

	if strings.Contains(logBuf.String(), secret) {
		t.Fatalf("expected secretKey to never appear in logs, got: %s", logBuf.String())
	}
	if !strings.Contains(logBuf.String(), event.EventID) {
		t.Fatalf("expected eventId to appear in retry logs for diagnostics, got: %s", logBuf.String())
	}
}

func TestProducerCloseClosesUnderlyingWriter(t *testing.T) {
	w := &fakeWriter{}
	p := newKafkaProducer(w, nil, nil, 5, time.Millisecond, time.Millisecond)

	if err := p.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !w.closed {
		t.Fatalf("expected underlying writer to be closed")
	}
}
