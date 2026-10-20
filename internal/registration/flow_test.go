package registration

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/omsu-informational-systems/alexey-shishin/internal/events"
	"github.com/omsu-informational-systems/alexey-shishin/internal/health"
)

type fakeProducer struct {
	mu        sync.Mutex
	produced  []events.Envelope
	produceFn func(topic, key string, event events.Envelope) error
}

func (p *fakeProducer) Produce(ctx context.Context, topic, key string, event events.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.produced = append(p.produced, event)
	if p.produceFn != nil {
		return p.produceFn(topic, key, event)
	}
	return nil
}

func (p *fakeProducer) Close() error { return nil }

type fakeConsumer struct {
	mu         sync.Mutex
	ready      bool
	queue      []kafka.Message
	fetchErr   error
	commits    []kafka.Message
	commitErr  error
	fetchCalls int
	primedMsg  *kafka.Message
}

func (c *fakeConsumer) Fetch(ctx context.Context) (kafka.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fetchCalls++
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

func (c *fakeConsumer) Prime(ctx context.Context) (kafka.Message, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.primedMsg != nil {
		return *c.primedMsg, true
	}
	return kafka.Message{}, false
}

func (c *fakeConsumer) Commit(ctx context.Context, msg kafka.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commits = append(c.commits, msg)
	return c.commitErr
}

func (c *fakeConsumer) Ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ready
}

func (c *fakeConsumer) Close() error { return nil }

func encodeResult(t *testing.T, causationID, accountID string, status events.RegistrationStatus, reason string) kafka.Message {
	t.Helper()
	payload := events.RegistrationResultPayload{
		AccountID: accountID,
		Status:    status,
		Reason:    reason,
	}
	env := events.NewEnvelope(eventTypeRegistrationResult, "corr-x", "platform", &causationID, payload)
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal fixture envelope: %v", err)
	}
	return kafka.Message{Topic: "account.registration.result", Value: raw}
}

func baseFlow(producer *fakeProducer, consumer *fakeConsumer) *Flow {
	return &Flow{
		Producer: producer,
		Consumer: consumer,
		Identity: Identity{
			SecretKey:    "secret",
			AccountID:    "acc-1",
			AccountName:  "vwap-account",
			GroupID:      "group-1",
			StrategyID:   "strategy-1",
			StrategyName: "vwap-reversion",
			ServiceName:  "vwap-reversion",
		},
		RequestTopic:          "account.registration.request",
		ResultTopic:           "account.registration.result",
		Health:                NewHealthChecker(),
		ReadinessPollInterval: time.Millisecond,
		ReadinessTimeout:      50 * time.Millisecond,
	}
}

func TestFlowWaitsForReadinessBeforePublishing(t *testing.T) {
	producer := &fakeProducer{}
	consumer := &fakeConsumer{ready: false}
	flow := baseFlow(producer, consumer)

	go func() {
		time.Sleep(10 * time.Millisecond)
		consumer.mu.Lock()
		consumer.ready = true
		consumer.queue = append(consumer.queue, kafka.Message{})
		consumer.mu.Unlock()
	}()

	consumer.mu.Lock()
	consumer.queue = nil
	consumer.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_, _ = flow.Run(ctx)
		close(done)
	}()

	time.Sleep(3 * time.Millisecond)
	producer.mu.Lock()
	publishedBeforeReady := len(producer.produced) > 0
	producer.mu.Unlock()
	if publishedBeforeReady {
		t.Fatalf("expected no publish before consumer became ready")
	}

	cancel()
	<-done
}

func TestFlowRegisteredPathMarksHealthUPAndStartsNothingElse(t *testing.T) {
	producer := &fakeProducer{}
	consumer := &fakeConsumer{ready: true}
	flow := baseFlow(producer, consumer)

	var resultMsg kafka.Message

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resultCh := make(chan Result, 1)
	errCh := make(chan error, 1)
	go func() {
		res, err := flow.Run(ctx)
		resultCh <- res
		errCh <- err
	}()

	time.Sleep(10 * time.Millisecond)
	producer.mu.Lock()
	if len(producer.produced) != 1 {
		producer.mu.Unlock()
		t.Fatalf("expected exactly one published request")
	}
	requestEventID := producer.produced[0].EventID
	producer.mu.Unlock()

	resultMsg = encodeResult(t, requestEventID, "acc-1", events.RegistrationStatusRegistered, "")
	consumer.mu.Lock()
	consumer.queue = append(consumer.queue, resultMsg)
	consumer.mu.Unlock()

	res := <-resultCh
	err := <-errCh
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != events.RegistrationStatusRegistered {
		t.Fatalf("expected REGISTERED, got %s", res.Status)
	}

	status, _ := flow.Health.Check(context.Background())
	if status != health.StatusUP {
		t.Fatalf("expected registration health UP, got %s", status)
	}

	consumer.mu.Lock()
	commitCount := len(consumer.commits)
	consumer.mu.Unlock()
	if commitCount != 1 {
		t.Fatalf("expected exactly one commit for the matching message, got %d", commitCount)
	}
}

func TestFlowRejectedPathMarksHealthDownWithReason(t *testing.T) {
	producer := &fakeProducer{}
	consumer := &fakeConsumer{ready: true}
	flow := baseFlow(producer, consumer)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resultCh := make(chan Result, 1)
	errCh := make(chan error, 1)
	go func() {
		res, err := flow.Run(ctx)
		resultCh <- res
		errCh <- err
	}()

	time.Sleep(10 * time.Millisecond)
	producer.mu.Lock()
	requestEventID := producer.produced[0].EventID
	producer.mu.Unlock()

	consumer.mu.Lock()
	consumer.queue = append(consumer.queue, encodeResult(t, requestEventID, "acc-1", events.RegistrationStatusRejected, "duplicate accountId"))
	consumer.mu.Unlock()

	res := <-resultCh
	err := <-errCh
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != events.RegistrationStatusRejected {
		t.Fatalf("expected REJECTED, got %s", res.Status)
	}
	if res.Reason != "duplicate accountId" {
		t.Fatalf("expected reason to be carried, got %q", res.Reason)
	}

	status, details := flow.Health.Check(context.Background())
	if status != health.StatusDOWN {
		t.Fatalf("expected registration health DOWN, got %s", status)
	}
	if details != "duplicate accountId" {
		t.Fatalf("expected details to carry the rejection reason, got %q", details)
	}
}

func TestFlowIgnoresButCommitsNonMatchingMessages(t *testing.T) {
	producer := &fakeProducer{}
	consumer := &fakeConsumer{ready: true}
	flow := baseFlow(producer, consumer)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resultCh := make(chan Result, 1)
	go func() {
		res, _ := flow.Run(ctx)
		resultCh <- res
	}()

	time.Sleep(10 * time.Millisecond)
	producer.mu.Lock()
	requestEventID := producer.produced[0].EventID
	producer.mu.Unlock()

	otherStudentsResult := encodeResult(t, "someone-elses-event-id", "acc-999", events.RegistrationStatusRegistered, "")
	ourResult := encodeResult(t, requestEventID, "acc-1", events.RegistrationStatusRegistered, "")

	consumer.mu.Lock()
	consumer.queue = append(consumer.queue, otherStudentsResult, ourResult)
	consumer.mu.Unlock()

	res := <-resultCh
	if res.Status != events.RegistrationStatusRegistered {
		t.Fatalf("expected REGISTERED from our own message, got %s", res.Status)
	}

	consumer.mu.Lock()
	commitCount := len(consumer.commits)
	consumer.mu.Unlock()
	if commitCount != 2 {
		t.Fatalf("expected both the foreign and our own message to be committed, got %d commits", commitCount)
	}
}

func TestFlowProcessesAndCommitsAMessagePickedUpDuringPriming(t *testing.T) {
	producer := &fakeProducer{}
	preExisting := encodeResult(t, "unrelated-event-id", "acc-999", events.RegistrationStatusRegistered, "")
	consumer := &fakeConsumer{ready: true, primedMsg: &preExisting}
	flow := baseFlow(producer, consumer)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resultCh := make(chan Result, 1)
	go func() {
		res, _ := flow.Run(ctx)
		resultCh <- res
	}()

	time.Sleep(10 * time.Millisecond)
	producer.mu.Lock()
	requestEventID := producer.produced[0].EventID
	producer.mu.Unlock()

	consumer.mu.Lock()
	consumer.queue = append(consumer.queue, encodeResult(t, requestEventID, "acc-1", events.RegistrationStatusRegistered, ""))
	consumer.mu.Unlock()

	res := <-resultCh
	if res.Status != events.RegistrationStatusRegistered {
		t.Fatalf("expected REGISTERED from our own message, got %s", res.Status)
	}

	consumer.mu.Lock()
	commitCount := len(consumer.commits)
	consumer.mu.Unlock()
	if commitCount != 2 {
		t.Fatalf("expected both the primed pre-existing message and our own message to be committed, got %d commits", commitCount)
	}
}

func TestFlowRetriesOnTransientFetchErrorWithoutCrashing(t *testing.T) {
	producer := &fakeProducer{}
	consumer := &fakeConsumer{ready: true, fetchErr: errors.New("broker unavailable")}
	flow := baseFlow(producer, consumer)
	flow.ReadinessTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resultCh := make(chan Result, 1)
	go func() {
		res, _ := flow.Run(ctx)
		resultCh <- res
	}()

	time.Sleep(10 * time.Millisecond)
	producer.mu.Lock()
	requestEventID := producer.produced[0].EventID
	producer.mu.Unlock()

	consumer.mu.Lock()
	consumer.queue = append(consumer.queue, encodeResult(t, requestEventID, "acc-1", events.RegistrationStatusRegistered, ""))
	consumer.mu.Unlock()

	select {
	case res := <-resultCh:
		if res.Status != events.RegistrationStatusRegistered {
			t.Fatalf("expected eventual REGISTERED after transient error, got %s", res.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("flow did not recover from a transient fetch error in time")
	}
}
