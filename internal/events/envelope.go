package events

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"
)

const SchemaVersion = "1.0"

type Envelope struct {
	EventID       string    `json:"eventId"`
	EventType     string    `json:"eventType"`
	SchemaVersion string    `json:"schemaVersion"`
	OccurredAt    time.Time `json:"occurredAt"`
	CorrelationID string    `json:"correlationId"`
	CausationID   *string   `json:"causationId,omitempty"`
	Producer      string    `json:"producer"`
	Payload       any       `json:"payload"`
}

func NewEnvelope(eventType, correlationID, producer string, causationID *string, payload any) Envelope {
	return Envelope{
		EventID:       NewUUIDv4(),
		EventType:     eventType,
		SchemaVersion: SchemaVersion,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: correlationID,
		CausationID:   causationID,
		Producer:      producer,
		Payload:       payload,
	}
}

func (e Envelope) DecodePayload(v any) error {
	raw, err := json.Marshal(e.Payload)
	if err != nil {
		return fmt.Errorf("events: re-marshal payload: %w", err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("events: decode payload: %w", err)
	}
	return nil
}

func NewUUIDv4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("events: read random bytes for uuid: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
