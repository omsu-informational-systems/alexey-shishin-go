package candles

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/omsu-informational-systems/alexey-shishin/internal/broker"
	"github.com/omsu-informational-systems/alexey-shishin/internal/events"
)

const defaultFetchRetryBackoff = time.Second

type instrumentKey struct {
	runID  string
	ticker string
}

type instrumentState struct {
	lastSequenceNumber int64
	lastCandleTime     time.Time
}

type Consumer struct {
	Consumer broker.Consumer
	Logger   *slog.Logger

	mu        sync.Mutex
	states    map[instrumentKey]*instrumentState
	processed int64
	lastSeq   *int64
}

func New(consumer broker.Consumer, logger *slog.Logger) *Consumer {
	return &Consumer{
		Consumer: consumer,
		Logger:   logger,
		states:   make(map[instrumentKey]*instrumentState),
	}
}

func (c *Consumer) log() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func (c *Consumer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := c.Consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log().Warn("candle fetch failed, retrying", "error", err.Error())
			select {
			case <-time.After(defaultFetchRetryBackoff):
				continue
			case <-ctx.Done():
				return
			}
		}

		if err := c.handle(ctx, msg); err != nil {
			c.log().Warn("candle handling failed, skipping", "error", err.Error())
			continue
		}
	}
}

func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var envelope events.Envelope
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		return fmt.Errorf("candles: decode envelope: %w", err)
	}

	var payload events.CandleClosedPayload
	if err := envelope.DecodePayload(&payload); err != nil {
		return fmt.Errorf("candles: decode payload: %w", err)
	}

	c.record(payload)

	c.log().Info("candle closed",
		"eventId", envelope.EventID,
		"correlationId", envelope.CorrelationID,
		"runId", payload.RunID,
		"ticker", payload.Instrument.Ticker,
		"sequenceNumber", payload.SequenceNumber,
	)

	if err := c.Consumer.Commit(ctx, msg); err != nil {
		return fmt.Errorf("candles: commit: %w", err)
	}
	return nil
}

func (c *Consumer) record(payload events.CandleClosedPayload) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := instrumentKey{runID: payload.RunID, ticker: payload.Instrument.Ticker}
	state, ok := c.states[key]
	if !ok {
		state = &instrumentState{}
		c.states[key] = state
	}
	state.lastSequenceNumber = payload.SequenceNumber
	state.lastCandleTime = payload.CandleTime

	c.processed++
	seq := payload.SequenceNumber
	c.lastSeq = &seq
}

func (c *Consumer) ProcessedCandles() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int(c.processed)
}

func (c *Consumer) LastProcessedSequenceNumber() *int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastSeq == nil {
		return nil
	}
	v := int(*c.lastSeq)
	return &v
}

func (c *Consumer) TrackedInstruments() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.states)
}
