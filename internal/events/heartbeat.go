package events

import "time"

const HeartbeatStatusOnline = "ONLINE"

type HeartbeatPayload struct {
	SecretKey                   string            `json:"secretKey"`
	AccountID                   string            `json:"accountId"`
	GroupID                     string            `json:"groupId"`
	StrategyID                  string            `json:"strategyId"`
	ServiceName                 string            `json:"serviceName"`
	InstanceID                  string            `json:"instanceId"`
	Status                      string            `json:"status"`
	StartedAt                   time.Time         `json:"startedAt"`
	SentAt                      time.Time         `json:"sentAt"`
	ProcessedCandles            int               `json:"processedCandles"`
	PublishedRecommendations    int               `json:"publishedRecommendations"`
	LastProcessedSequenceNumber *int              `json:"lastProcessedSequenceNumber,omitempty"`
	Metadata                    map[string]string `json:"metadata,omitempty"`
}
