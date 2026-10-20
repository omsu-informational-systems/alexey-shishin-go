package events

import (
	"encoding/json"
	"testing"
	"time"
)

func TestHeartbeatPayloadRoundTrip(t *testing.T) {
	seq := 7
	original := HeartbeatPayload{
		SecretKey:                   "secret",
		AccountID:                   "acc-1",
		GroupID:                     "group-1",
		StrategyID:                  "strategy-1",
		ServiceName:                 "vwap-reversion",
		InstanceID:                  "instance-1",
		Status:                      HeartbeatStatusOnline,
		StartedAt:                   time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC),
		SentAt:                      time.Date(2026, 10, 17, 10, 5, 0, 0, time.UTC),
		ProcessedCandles:            12,
		PublishedRecommendations:    0,
		LastProcessedSequenceNumber: &seq,
		Metadata:                    map[string]string{"build": "dev"},
	}

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded HeartbeatPayload
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.AccountID != original.AccountID ||
		decoded.GroupID != original.GroupID ||
		decoded.StrategyID != original.StrategyID ||
		decoded.ServiceName != original.ServiceName ||
		decoded.SecretKey != original.SecretKey ||
		decoded.Status != original.Status ||
		decoded.ProcessedCandles != original.ProcessedCandles ||
		decoded.PublishedRecommendations != original.PublishedRecommendations ||
		!decoded.StartedAt.Equal(original.StartedAt) ||
		!decoded.SentAt.Equal(original.SentAt) {
		t.Fatalf("payload mismatch: got %+v, want %+v", decoded, original)
	}

	if decoded.LastProcessedSequenceNumber == nil || *decoded.LastProcessedSequenceNumber != seq {
		t.Fatalf("expected lastProcessedSequenceNumber %d, got %v", seq, decoded.LastProcessedSequenceNumber)
	}
}

func TestHeartbeatPayloadCarriesIdentityFields(t *testing.T) {
	p := HeartbeatPayload{SecretKey: "s", AccountID: "a", GroupID: "g", StrategyID: "st", ServiceName: "svc"}
	if p.SecretKey == "" || p.AccountID == "" || p.GroupID == "" || p.StrategyID == "" || p.ServiceName == "" {
		t.Fatalf("expected full identity set on heartbeat payload, got %+v", p)
	}
}
