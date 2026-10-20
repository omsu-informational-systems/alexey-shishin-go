package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type fakeReader struct {
	fetchFailTimes  int
	fetchCalls      int
	fetchErr        error
	commitFailTimes int
	commitCalls     int
	rebalances      int64
	closed          bool
	toReturn        kafka.Message
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	f.fetchCalls++
	if f.fetchCalls <= f.fetchFailTimes {
		if f.fetchErr != nil {
			return kafka.Message{}, f.fetchErr
		}
		return kafka.Message{}, errors.New("transient fetch failure")
	}
	return f.toReturn, nil
}

func (f *fakeReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	f.commitCalls++
	if f.commitCalls <= f.commitFailTimes {
		return errors.New("transient commit failure")
	}
	return nil
}

func (f *fakeReader) Stats() kafka.ReaderStats {
	return kafka.ReaderStats{Rebalances: f.rebalances}
}

func (f *fakeReader) Close() error {
	f.closed = true
	return nil
}

func TestFetchSucceedsAfterTransientRetries(t *testing.T) {
	r := &fakeReader{fetchFailTimes: 2, toReturn: kafka.Message{Topic: "market.candle.closed", Value: []byte("{}")}}
	liveness := NewLivenessChecker("candle-reader")
	c := newKafkaConsumer(r, "market.candle.closed", "group.candles", nil, liveness, 5, time.Millisecond, time.Millisecond)

	msg, err := c.Fetch(context.Background())
	if err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}
	if r.fetchCalls != 3 {
		t.Fatalf("expected 3 attempts, got %d", r.fetchCalls)
	}
	if msg.Topic != "market.candle.closed" {
		t.Fatalf("unexpected topic: %q", msg.Topic)
	}

	status, _ := liveness.Check(context.Background())
	if status != "UP" {
		t.Fatalf("expected liveness UP, got %s", status)
	}
}

func TestPrimeTriggersExactlyOneUnderlyingFetchCallEvenOnFailure(t *testing.T) {
	r := &fakeReader{fetchFailTimes: 100}
	c := newKafkaConsumer(r, "topic", "group", nil, nil, 5, time.Millisecond, time.Millisecond)

	_, ok := c.Prime(context.Background())
	if ok {
		t.Fatalf("expected Prime to report no message on failure")
	}
	if r.fetchCalls != 1 {
		t.Fatalf("expected Prime to call the underlying reader exactly once (to trigger lazy start), got %d calls", r.fetchCalls)
	}
}

func TestPrimeReturnsMessageAndRecordsLivenessOnSuccess(t *testing.T) {
	r := &fakeReader{toReturn: kafka.Message{Topic: "account.registration.result", Value: []byte("{}")}}
	liveness := NewLivenessChecker("registration-reader")
	c := newKafkaConsumer(r, "account.registration.result", "group.registration", nil, liveness, 5, time.Millisecond, time.Millisecond)

	msg, ok := c.Prime(context.Background())
	if !ok {
		t.Fatalf("expected Prime to succeed")
	}
	if msg.Topic != "account.registration.result" {
		t.Fatalf("unexpected topic: %q", msg.Topic)
	}
	if r.fetchCalls != 1 {
		t.Fatalf("expected exactly 1 underlying fetch call, got %d", r.fetchCalls)
	}

	status, _ := liveness.Check(context.Background())
	if status != "UP" {
		t.Fatalf("expected liveness UP after a successful prime, got %s", status)
	}
}

func TestFetchGivesUpAfterMaxAttemptsAndRecordsDown(t *testing.T) {
	r := &fakeReader{fetchFailTimes: 100}
	liveness := NewLivenessChecker("candle-reader")
	c := newKafkaConsumer(r, "market.candle.closed", "group.candles", nil, liveness, 3, time.Millisecond, time.Millisecond)

	_, err := c.Fetch(context.Background())
	if err == nil {
		t.Fatalf("expected error after exhausting attempts")
	}
	if r.fetchCalls != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", r.fetchCalls)
	}

	status, _ := liveness.Check(context.Background())
	if status != "DOWN" {
		t.Fatalf("expected liveness DOWN, got %s", status)
	}
}

func TestFetchDoesNotRetryOnContextCancelled(t *testing.T) {
	r := &fakeReader{fetchFailTimes: 100, fetchErr: context.Canceled}
	c := newKafkaConsumer(r, "topic", "group", nil, nil, 5, time.Millisecond, time.Millisecond)

	_, err := c.Fetch(context.Background())
	if err == nil {
		t.Fatalf("expected error")
	}
	if r.fetchCalls != 1 {
		t.Fatalf("expected a single attempt on context cancellation, got %d", r.fetchCalls)
	}
}

func TestCommitRetriesOnTransientFailureThenSucceeds(t *testing.T) {
	r := &fakeReader{commitFailTimes: 2}
	c := newKafkaConsumer(r, "topic", "group", nil, nil, 5, time.Millisecond, time.Millisecond)

	if err := c.Commit(context.Background(), kafka.Message{}); err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if r.commitCalls != 3 {
		t.Fatalf("expected 3 attempts, got %d", r.commitCalls)
	}
}

func TestReadyReflectsRebalanceCount(t *testing.T) {
	r := &fakeReader{rebalances: 0}
	c := newKafkaConsumer(r, "topic", "group", nil, nil, 5, time.Millisecond, time.Millisecond)

	if c.Ready() {
		t.Fatalf("expected not ready before any rebalance")
	}

	r.rebalances = 1
	if !c.Ready() {
		t.Fatalf("expected ready after a rebalance (partition assignment) happened")
	}
}

func TestConsumerCloseClosesUnderlyingReader(t *testing.T) {
	r := &fakeReader{}
	c := newKafkaConsumer(r, "topic", "group", nil, nil, 5, time.Millisecond, time.Millisecond)

	if err := c.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !r.closed {
		t.Fatalf("expected underlying reader to be closed")
	}
}
