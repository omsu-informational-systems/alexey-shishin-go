package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type Consumer interface {
	Fetch(ctx context.Context) (kafka.Message, error)
	Commit(ctx context.Context, msg kafka.Message) error
	Prime(ctx context.Context) (kafka.Message, bool)
	Ready() bool
	Close() error
}

type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Stats() kafka.ReaderStats
	Close() error
}

type ConsumerParams struct {
	Brokers     []string
	Topic       string
	GroupID     string
	Dialer      *kafka.Dialer
	Logger      *slog.Logger
	Liveness    *LivenessChecker
	MaxAttempts int
	BackoffMin  time.Duration
	BackoffMax  time.Duration
}

type KafkaConsumer struct {
	reader      messageReader
	topic       string
	groupID     string
	maxAttempts int
	backoffMin  time.Duration
	backoffMax  time.Duration
	logger      *slog.Logger
	liveness    *LivenessChecker
}

func NewKafkaConsumer(ctx context.Context, params ConsumerParams) (*KafkaConsumer, error) {
	if err := verifyTopicExists(ctx, params.Dialer, params.Brokers, params.Topic); err != nil {
		return nil, fmt.Errorf("broker: verify topic %q: %w", params.Topic, err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     params.Brokers,
		Topic:       params.Topic,
		GroupID:     params.GroupID,
		Dialer:      params.Dialer,
		StartOffset: kafka.FirstOffset,
	})

	return newKafkaConsumer(reader, params.Topic, params.GroupID, params.Logger, params.Liveness, params.MaxAttempts, params.BackoffMin, params.BackoffMax), nil
}

func newKafkaConsumer(reader messageReader, topic, groupID string, logger *slog.Logger, liveness *LivenessChecker, maxAttempts int, backoffMin, backoffMax time.Duration) *KafkaConsumer {
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}
	return &KafkaConsumer{
		reader:      reader,
		topic:       topic,
		groupID:     groupID,
		maxAttempts: maxAttempts,
		backoffMin:  backoffMin,
		backoffMax:  backoffMax,
		logger:      logger,
		liveness:    liveness,
	}
}

func (c *KafkaConsumer) log() *slog.Logger {
	if c.logger != nil {
		return c.logger
	}
	return slog.Default()
}

func (c *KafkaConsumer) Prime(ctx context.Context) (kafka.Message, bool) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return kafka.Message{}, false
	}
	if c.liveness != nil {
		c.liveness.Record(nil)
	}
	return msg, true
}

func (c *KafkaConsumer) Fetch(ctx context.Context) (kafka.Message, error) {
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		msg, err := c.reader.FetchMessage(ctx)
		if err == nil {
			if c.liveness != nil {
				c.liveness.Record(nil)
			}
			return msg, nil
		}
		lastErr = err

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
			break
		}

		c.log().Warn("kafka fetch failed, retrying",
			"topic", c.topic, "groupId", c.groupID, "attempt", attempt, "error", err.Error())

		if attempt == c.maxAttempts {
			break
		}
		if sleepErr := sleepOrDone(ctx, backoffDuration(attempt, c.backoffMin, c.backoffMax)); sleepErr != nil {
			lastErr = sleepErr
			break
		}
	}

	if c.liveness != nil {
		c.liveness.Record(lastErr)
	}
	return kafka.Message{}, fmt.Errorf("broker: fetch from %s: %w", c.topic, lastErr)
}

func (c *KafkaConsumer) Commit(ctx context.Context, msg kafka.Message) error {
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		lastErr = c.reader.CommitMessages(ctx, msg)
		if lastErr == nil {
			return nil
		}

		if errors.Is(lastErr, context.Canceled) || errors.Is(lastErr, context.DeadlineExceeded) {
			break
		}

		c.log().Warn("kafka commit failed, retrying",
			"topic", c.topic, "groupId", c.groupID, "attempt", attempt, "error", lastErr.Error())

		if attempt == c.maxAttempts {
			break
		}
		if sleepErr := sleepOrDone(ctx, backoffDuration(attempt, c.backoffMin, c.backoffMax)); sleepErr != nil {
			lastErr = sleepErr
			break
		}
	}

	return fmt.Errorf("broker: commit to %s: %w", c.topic, lastErr)
}

func (c *KafkaConsumer) Ready() bool {
	return c.reader.Stats().Rebalances > 0
}

func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
}

func verifyTopicExists(ctx context.Context, dialer *kafka.Dialer, brokers []string, topic string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}
	if dialer == nil {
		dialer = kafka.DefaultDialer
	}

	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial %s: %w", brokers[0], err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return fmt.Errorf("read partitions: %w", err)
	}
	if len(partitions) == 0 {
		return fmt.Errorf("topic has no partitions")
	}
	return nil
}
