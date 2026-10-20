package events

import (
	"encoding/json"
	"testing"
	"time"
)

func TestCandleClosedPayloadRoundTrip(t *testing.T) {
	original := CandleClosedPayload{
		RunID:          "run-1",
		GroupID:        "group-1",
		Instrument:     Instrument{Ticker: "SBER", FIGI: "BBG1", Currency: "RUB", LotSize: 10},
		Timeframe:      "1m",
		SequenceNumber: 100,
		CandleTime:     time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC),
		TradingDate:    "2026-10-17",
		SessionPhase:   "MAIN",
		Open:           "250.10",
		High:           "251.00",
		Low:            "249.80",
		Close:          "250.90",
		Volume:         15000,
		IsWarmup:       false,
	}

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded CandleClosedPayload
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.RunID != original.RunID ||
		decoded.Instrument != original.Instrument ||
		decoded.SequenceNumber != original.SequenceNumber ||
		decoded.Open != original.Open ||
		decoded.High != original.High ||
		decoded.Low != original.Low ||
		decoded.Close != original.Close ||
		decoded.Volume != original.Volume ||
		!decoded.CandleTime.Equal(original.CandleTime) {
		t.Fatalf("payload mismatch: got %+v, want %+v", decoded, original)
	}
}

func TestCandleClosedPayloadDecimalFieldsStayStrings(t *testing.T) {
	payload := CandleClosedPayload{Open: "100.50", High: "101.00", Low: "99.90", Close: "100.80", Volume: 1}

	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var asMap map[string]any
	if err := json.Unmarshal(raw, &asMap); err != nil {
		t.Fatalf("unmarshal to map: %v", err)
	}

	for _, field := range []string{"open", "high", "low", "close"} {
		value, ok := asMap[field].(string)
		if !ok {
			t.Fatalf("expected field %q to decode as string, got %T (%v)", field, asMap[field], asMap[field])
		}
		if value == "" {
			t.Fatalf("expected non-empty decimal string for field %q", field)
		}
	}

	if _, isNumber := asMap["volume"].(float64); !isNumber {
		t.Fatalf("expected volume to decode as a JSON number, got %T", asMap["volume"])
	}
}
