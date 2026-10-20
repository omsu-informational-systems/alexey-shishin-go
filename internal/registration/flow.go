package registration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/omsu-informational-systems/alexey-shishin/internal/broker"
	"github.com/omsu-informational-systems/alexey-shishin/internal/events"
)

const (
	eventTypeRegistrationRequest = "account.registration.request"
	eventTypeRegistrationResult  = "account.registration.result"

	defaultReadinessPollInterval = 100 * time.Millisecond
	defaultReadinessTimeout      = 10 * time.Second
	defaultPrimeTimeout          = 2 * time.Second
	defaultFetchRetryBackoff     = time.Second
)

type Identity struct {
	SecretKey    string
	AccountID    string
	AccountName  string
	GroupID      string
	StrategyID   string
	StrategyName string
	ServiceName  string
	Description  string
}

type Result struct {
	Status events.RegistrationStatus
	Reason string
}

type Flow struct {
	Producer broker.Producer
	Consumer broker.Consumer
	Identity Identity

	RequestTopic string
	ResultTopic  string

	Health *HealthChecker
	Logger *slog.Logger

	ReadinessPollInterval time.Duration
	ReadinessTimeout      time.Duration
}

func (f *Flow) log() *slog.Logger {
	if f.Logger != nil {
		return f.Logger
	}
	return slog.Default()
}

func (f *Flow) primeAndWaitReady(ctx context.Context) (*kafka.Message, error) {
	primeCtx, cancel := context.WithTimeout(ctx, defaultPrimeTimeout)
	msg, ok := f.Consumer.Prime(primeCtx)
	cancel()

	var primed *kafka.Message
	if ok {
		primed = &msg
	}

	timeout := f.ReadinessTimeout
	if timeout <= 0 {
		timeout = defaultReadinessTimeout
	}
	interval := f.ReadinessPollInterval
	if interval <= 0 {
		interval = defaultReadinessPollInterval
	}

	deadline := time.Now().Add(timeout)
	for {
		if f.Consumer.Ready() {
			return primed, nil
		}
		if time.Now().After(deadline) {
			return primed, fmt.Errorf("registration: registration-result reader not ready after %s", timeout)
		}
		select {
		case <-time.After(interval):
		case <-ctx.Done():
			return primed, ctx.Err()
		}
	}
}

func (f *Flow) Run(ctx context.Context) (Result, error) {
	primed, err := f.primeAndWaitReady(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("registration: readiness wait failed: %w", err)
	}

	description := (*string)(nil)
	if f.Identity.Description != "" {
		d := f.Identity.Description
		description = &d
	}

	requestPayload := events.RegistrationRequestPayload{
		SecretKey:    f.Identity.SecretKey,
		AccountID:    f.Identity.AccountID,
		AccountName:  f.Identity.AccountName,
		GroupID:      f.Identity.GroupID,
		StrategyID:   f.Identity.StrategyID,
		StrategyName: f.Identity.StrategyName,
		ServiceName:  f.Identity.ServiceName,
		Description:  description,
	}

	requestEnvelope := events.NewEnvelope(eventTypeRegistrationRequest, events.NewUUIDv4(), f.Identity.ServiceName, nil, requestPayload)

	if err := f.Producer.Produce(ctx, f.RequestTopic, f.Identity.AccountID, requestEnvelope); err != nil {
		return Result{}, fmt.Errorf("registration: publish request: %w", err)
	}

	for {
		var msg kafka.Message
		if primed != nil {
			msg = *primed
			primed = nil
		} else {
			fetched, fetchErr := f.Consumer.Fetch(ctx)
			if fetchErr != nil {
				if ctx.Err() != nil {
					return Result{}, ctx.Err()
				}
				f.log().Warn("registration result fetch failed, retrying", "error", fetchErr.Error())
				if sleepErr := sleepOrDone(ctx, defaultFetchRetryBackoff); sleepErr != nil {
					return Result{}, sleepErr
				}
				continue
			}
			msg = fetched
		}

		var resultEnvelope events.Envelope
		if decodeErr := json.Unmarshal(msg.Value, &resultEnvelope); decodeErr != nil {
			f.log().Warn("registration result envelope decode failed, skipping", "error", decodeErr.Error())
			f.commit(ctx, msg)
			continue
		}

		if resultEnvelope.EventType != eventTypeRegistrationResult {
			f.commit(ctx, msg)
			continue
		}

		var resultPayload events.RegistrationResultPayload
		if decodeErr := resultEnvelope.DecodePayload(&resultPayload); decodeErr != nil {
			f.log().Warn("registration result payload decode failed, skipping", "error", decodeErr.Error())
			f.commit(ctx, msg)
			continue
		}

		f.commit(ctx, msg)

		matches := resultPayload.AccountID == f.Identity.AccountID &&
			resultEnvelope.CausationID != nil &&
			*resultEnvelope.CausationID == requestEnvelope.EventID

		if !matches {
			f.log().Debug("registration result ignored, not ours",
				"eventId", resultEnvelope.EventID, "correlationId", resultEnvelope.CorrelationID)
			continue
		}

		switch resultPayload.Status {
		case events.RegistrationStatusRegistered:
			if f.Health != nil {
				f.Health.MarkRegistered()
			}
			return Result{Status: resultPayload.Status}, nil
		case events.RegistrationStatusRejected:
			if f.Health != nil {
				f.Health.MarkRejected(resultPayload.Reason)
			}
			f.log().Error("registration rejected", "reason", resultPayload.Reason)
			return Result{Status: resultPayload.Status, Reason: resultPayload.Reason}, nil
		default:
			f.log().Warn("registration result has unknown status, ignoring", "status", string(resultPayload.Status))
			continue
		}
	}
}

func (f *Flow) commit(ctx context.Context, msg kafka.Message) {
	if err := f.Consumer.Commit(ctx, msg); err != nil {
		f.log().Warn("registration result commit failed", "error", err.Error())
	}
}

func sleepOrDone(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
