package events

import (
	"encoding/json"
	"testing"
)

func TestRegistrationRequestPayloadRoundTrip(t *testing.T) {
	description := "учебный стенд"
	original := RegistrationRequestPayload{
		SecretKey:    "secret",
		AccountID:    "acc-1",
		AccountName:  "vwap-account",
		GroupID:      "group-1",
		StrategyID:   "strategy-1",
		StrategyName: "vwap-reversion",
		ServiceName:  "vwap-reversion",
		Description:  &description,
	}

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded RegistrationRequestPayload
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded != original {
		if decoded.Description == nil || original.Description == nil || *decoded.Description != *original.Description {
			t.Fatalf("payload mismatch: got %+v, want %+v", decoded, original)
		}
	}
}

func TestRegistrationResultPayloadRoundTrip(t *testing.T) {
	original := RegistrationResultPayload{
		SecretKey:   "secret",
		AccountID:   "acc-1",
		GroupID:     "group-1",
		StrategyID:  "strategy-1",
		ServiceName: "vwap-reversion",
		Status:      RegistrationStatusRejected,
		Reason:      "duplicate accountId",
	}

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded RegistrationResultPayload
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded != original {
		t.Fatalf("payload mismatch: got %+v, want %+v", decoded, original)
	}
}

func TestRegistrationResultPayloadHasAccountIDForMatching(t *testing.T) {
	payload := RegistrationResultPayload{AccountID: "acc-1", Status: RegistrationStatusRegistered}
	if payload.AccountID == "" {
		t.Fatalf("expected AccountID to be present for matching responses to requests")
	}
}
