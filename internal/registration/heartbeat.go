package registration

import (
	"context"
	"log/slog"
	"time"

	"github.com/omsu-informational-systems/alexey-shishin/internal/broker"
	"github.com/omsu-informational-systems/alexey-shishin/internal/events"
)

const (
	eventTypeHeartbeat    = "account.service.heartbeat"
	defaultHeartbeatEvery = 5 * time.Second
)

type CandleCounters interface {
	ProcessedCandles() int
	LastProcessedSequenceNumber() *int
}

type Heartbeat struct {
	Producer   broker.Producer
	Topic      string
	Identity   Identity
	InstanceID string
	StartedAt  time.Time
	Interval   time.Duration
	Counters   CandleCounters
	Logger     *slog.Logger
}

func (h *Heartbeat) log() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

func (h *Heartbeat) Run(ctx context.Context) {
	interval := h.Interval
	if interval <= 0 {
		interval = defaultHeartbeatEvery
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.send(ctx)
		}
	}
}

func (h *Heartbeat) send(ctx context.Context) {
	payload := events.HeartbeatPayload{
		SecretKey:                h.Identity.SecretKey,
		AccountID:                h.Identity.AccountID,
		GroupID:                  h.Identity.GroupID,
		StrategyID:               h.Identity.StrategyID,
		ServiceName:              h.Identity.ServiceName,
		InstanceID:               h.InstanceID,
		Status:                   events.HeartbeatStatusOnline,
		StartedAt:                h.StartedAt,
		SentAt:                   time.Now().UTC(),
		PublishedRecommendations: 0,
	}
	if h.Counters != nil {
		payload.ProcessedCandles = h.Counters.ProcessedCandles()
		payload.LastProcessedSequenceNumber = h.Counters.LastProcessedSequenceNumber()
	}

	envelope := events.NewEnvelope(eventTypeHeartbeat, events.NewUUIDv4(), h.Identity.ServiceName, nil, payload)

	if err := h.Producer.Produce(ctx, h.Topic, h.Identity.AccountID, envelope); err != nil {
		h.log().Warn("heartbeat publish failed", "error", err.Error())
	}
}
