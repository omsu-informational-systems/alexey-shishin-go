package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/omsu-informational-systems/alexey-shishin/internal/events"
)

type Producer interface {
	Produce(ctx context.Context, topic, key string, event events.Envelope) error
	Close() error
}

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type ProducerParams struct {
	Brokers     []string
	Transport   *kafka.Transport
	Logger      *slog.Logger
	Liveness    *LivenessChecker
	MaxAttempts int
	BackoffMin  time.Duration
	BackoffMax  time.Duration
}

type KafkaProducer struct {
	writer      messageWriter
	maxAttempts int
	backoffMin  time.Duration
	backoffMax  time.Duration
	logger      *slog.Logger
	liveness    *LivenessChecker
}

func NewKafkaProducer(params ProducerParams) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(params.Brokers...),
		Transport:              params.Transport,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
	}
	return newKafkaProducer(writer, params.Logger, params.Liveness, params.MaxAttempts, params.BackoffMin, params.BackoffMax)
}

func newKafkaProducer(writer messageWriter, logger *slog.Logger, liveness *LivenessChecker, maxAttempts int, backoffMin, backoffMax time.Duration) *KafkaProducer {
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}
	return &KafkaProducer{
		writer:      writer,
		maxAttempts: maxAttempts,
		backoffMin:  backoffMin,
		backoffMax:  backoffMax,
		logger:      logger,
		liveness:    liveness,
	}
}

func (p *KafkaProducer) log() *slog.Logger {
	if p.logger != nil {
		return p.logger
	}
	return slog.Default()
}

func (p *KafkaProducer) Produce(ctx context.Context, topic, key string, event events.Envelope) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("broker: marshal event %s: %w", event.EventType, err)
	}

	msg := kafka.Message{Topic: topic, Key: []byte(key), Value: value}

	var lastErr error
	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		lastErr = p.writer.WriteMessages(ctx, msg)
		if lastErr == nil {
			if p.liveness != nil {
				p.liveness.Record(nil)
			}
			return nil
		}

		p.log().Warn("kafka produce failed, retrying",
			"eventId", event.EventID,
			"eventType", event.EventType,
			"correlationId", event.CorrelationID,
			"attempt", attempt,
			"error", lastErr.Error(),
		)

		if attempt == p.maxAttempts {
			break
		}
		if sleepErr := sleepOrDone(ctx, backoffDuration(attempt, p.backoffMin, p.backoffMax)); sleepErr != nil {
			lastErr = sleepErr
			break
		}
	}

	if p.liveness != nil {
		p.liveness.Record(lastErr)
	}
	return fmt.Errorf("broker: produce to %s after %d attempts: %w", topic, p.maxAttempts, lastErr)
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}
