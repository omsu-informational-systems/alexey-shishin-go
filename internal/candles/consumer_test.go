package candles

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/omsu-informational-systems/alexey-shishin/internal/events"
)

type fakeConsumer struct {
	mu         sync.Mutex
	queue      []kafka.Message
	fetchErr   error
	commits    []kafka.Message
	commitErr  error
	readyValue bool
}

func (c *fakeConsumer) Fetch(ctx context.Context) (kafka.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fetchErr != nil {
		err := c.fetchErr
		c.fetchErr = nil
		return kafka.Message{}, err
	}
	if len(c.queue) == 0 {
		return kafka.Message{}, errors.New("no more fake messages queued")
	}
	msg := c.queue[0]
	c.queue = c.queue[1:]
	return msg, nil
}

func (c *fakeConsumer) Commit(ctx context.Context, msg kafka.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commits = append(c.commits, msg)
	return c.commitErr
}

func (c *fakeConsumer) Prime(ctx context.Context) (kafka.Message, bool) {
	return kafka.Message{}, false
}

func (c *fakeConsumer) Ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readyValue
}

func (c *fakeConsumer) Close() error { return nil }

func candleMessage(t *testing.T, runID, ticker string, seq int64) kafka.Message {
	t.Helper()
	payload := events.CandleClosedPayload{
		RunID:          runID,
		Instrument:     events.Instrument{Ticker: ticker},
		SequenceNumber: seq,
		Open:           "100.50",
		High:           "101.00",
		Low:            "99.90",
		Close:          "100.80",
		Volume:         1000,
	}
	env := events.NewEnvelope("market.candle.closed", "corr-1", "platform", nil, payload)
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return kafka.Message{Topic: "market.candle.closed", Value: raw}
}

func TestHandleParsesPayloadKeepingDecimalFieldsAsStrings(t *testing.T) {
	fc := &fakeConsumer{}
	c := New(fc, nil)

	msg := candleMessage(t, "run-1", "SBER", 1)
	if err := c.handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if c.ProcessedCandles() != 1 {
		t.Fatalf("expected 1 processed candle, got %d", c.ProcessedCandles())
	}
}

func TestHandleSeparatesStateByRunIDAndTicker(t *testing.T) {
	fc := &fakeConsumer{}
	c := New(fc, nil)

	must := func(msg kafka.Message) {
		if err := c.handle(context.Background(), msg); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	must(candleMessage(t, "run-1", "SBER", 1))
	must(candleMessage(t, "run-1", "GAZP", 1))
	must(candleMessage(t, "run-2", "SBER", 1))

	if got := c.TrackedInstruments(); got != 3 {
		t.Fatalf("expected 3 distinct (runId, ticker) states, got %d", got)
	}
	if c.ProcessedCandles() != 3 {
		t.Fatalf("expected 3 processed candles, got %d", c.ProcessedCandles())
	}
}

func TestHandleDoesNotCommitOnDecodeFailure(t *testing.T) {
	fc := &fakeConsumer{}
	c := New(fc, nil)

	badMsg := kafka.Message{Topic: "market.candle.closed", Value: []byte("not-json")}
	if err := c.handle(context.Background(), badMsg); err == nil {
		t.Fatalf("expected a decode error")
	}

	if len(fc.commits) != 0 {
		t.Fatalf("expected no commit on decode failure, got %d commits", len(fc.commits))
	}
	if c.ProcessedCandles() != 0 {
		t.Fatalf("expected no candle recorded on decode failure, got %d", c.ProcessedCandles())
	}
}

func TestHandleLogsOnlyTechnicalFieldsNeverFullPayload(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	fc := &fakeConsumer{}
	c := New(fc, logger)

	const openPrice = "100.50"
	msg := candleMessage(t, "run-1", "SBER", 42)
	if err := c.handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	logged := logBuf.String()
	if !strings.Contains(logged, "run-1") || !strings.Contains(logged, "SBER") || !strings.Contains(logged, "42") {
		t.Fatalf("expected structured fields (runId, ticker, sequenceNumber) in log, got: %s", logged)
	}
	if strings.Contains(logged, openPrice) {
		t.Fatalf("expected full candle payload (e.g. open price) to never be logged, got: %s", logged)
	}
}

func TestHandleCommitsOnlyAfterSuccessfulLog(t *testing.T) {
	fc := &fakeConsumer{}
	c := New(fc, nil)

	msg := candleMessage(t, "run-1", "SBER", 5)
	if err := c.handle(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(fc.commits) != 1 {
		t.Fatalf("expected exactly 1 commit after successful processing, got %d", len(fc.commits))
	}
}

func TestLastProcessedSequenceNumberTracksMostRecentCandle(t *testing.T) {
	fc := &fakeConsumer{}
	c := New(fc, nil)

	if seq := c.LastProcessedSequenceNumber(); seq != nil {
		t.Fatalf("expected nil before any candle processed, got %v", *seq)
	}

	if err := c.handle(context.Background(), candleMessage(t, "run-1", "SBER", 7)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	seq := c.LastProcessedSequenceNumber()
	if seq == nil || *seq != 7 {
		t.Fatalf("expected last sequence number 7, got %v", seq)
	}
}

func TestRunRetriesOnTransientFetchErrorWithoutCrashing(t *testing.T) {
	fc := &fakeConsumer{fetchErr: errors.New("broker unavailable")}
	c := New(fc, nil)

	fc.mu.Lock()
	fc.queue = append(fc.queue, candleMessage(t, "run-1", "SBER", 1))
	fc.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.ProcessedCandles() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if c.ProcessedCandles() != 1 {
		t.Fatalf("expected Run to recover from a transient fetch error and process the queued candle")
	}

	cancel()
	<-done
}
