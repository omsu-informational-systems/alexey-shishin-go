package events

import (
	"encoding/json"
	"regexp"
	"testing"
)

var uuidV4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewUUIDv4FormatAndUniqueness(t *testing.T) {
	a := NewUUIDv4()
	b := NewUUIDv4()

	if !uuidV4Pattern.MatchString(a) {
		t.Fatalf("expected UUIDv4 format, got %q", a)
	}
	if a == b {
		t.Fatalf("expected two distinct UUIDs, got the same value %q twice", a)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	causationID := "cause-123"
	payload := CandleClosedPayload{
		RunID:          "run-1",
		Instrument:     Instrument{Ticker: "SBER"},
		SequenceNumber: 42,
		Open:           "100.50",
		High:           "101.00",
		Low:            "99.90",
		Close:          "100.80",
		Volume:         1000,
	}

	original := NewEnvelope("market.candle.closed", "corr-1", "vwap-reversion", &causationID, payload)

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	var decoded Envelope
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}

	if decoded.EventID != original.EventID {
		t.Fatalf("eventId mismatch: got %q, want %q", decoded.EventID, original.EventID)
	}
	if decoded.EventType != "market.candle.closed" {
		t.Fatalf("unexpected eventType: %q", decoded.EventType)
	}
	if decoded.SchemaVersion != SchemaVersion {
		t.Fatalf("unexpected schemaVersion: %q", decoded.SchemaVersion)
	}
	if decoded.CausationID == nil || *decoded.CausationID != causationID {
		t.Fatalf("unexpected causationId: %v", decoded.CausationID)
	}

	var decodedPayload CandleClosedPayload
	if err := decoded.DecodePayload(&decodedPayload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if decodedPayload != payload {
		t.Fatalf("payload mismatch: got %+v, want %+v", decodedPayload, payload)
	}
}

func TestEnvelopeCausationIDOmittedWhenNil(t *testing.T) {
	e := NewEnvelope("account.registration.request", "corr-2", "vwap-reversion", nil, RegistrationRequestPayload{})

	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	var asMap map[string]any
	if err := json.Unmarshal(raw, &asMap); err != nil {
		t.Fatalf("unmarshal to map: %v", err)
	}
	if _, present := asMap["causationId"]; present {
		t.Fatalf("expected causationId to be omitted when nil, got %v", asMap["causationId"])
	}
}
