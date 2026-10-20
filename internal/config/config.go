package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	Port           string
	ServiceName    string
	ServiceVersion string

	KafkaBootstrapServers    []string
	KafkaUsername            string
	KafkaPassword            string
	KafkaGroupID             string
	CandleTopic              string
	TopicRegistrationRequest string
	TopicRegistrationResult  string
	TopicHeartbeat           string

	AccountSecretKey string
	AccountID        string
	AccountName      string
	GroupID          string
	StrategyID       string
	StrategyName     string
	Description      string
}

const (
	defaultPort           = "8080"
	defaultServiceName    = "vwap-reversion"
	defaultServiceVersion = "0.1.0"

	defaultTopicRegistrationRequest = "account.registration.request"
	defaultTopicRegistrationResult  = "account.registration.result"
	defaultTopicHeartbeat           = "account.service.heartbeat"
)

func Load() (Config, error) {
	cfg := Config{
		Port:           getEnv("PORT", defaultPort),
		ServiceName:    getEnv("SERVICE_NAME", defaultServiceName),
		ServiceVersion: getEnv("SERVICE_VERSION", defaultServiceVersion),

		TopicRegistrationRequest: getEnv("TOPIC_REGISTRATION_REQUEST", defaultTopicRegistrationRequest),
		TopicRegistrationResult:  getEnv("TOPIC_REGISTRATION_RESULT", defaultTopicRegistrationResult),
		TopicHeartbeat:           getEnv("TOPIC_HEARTBEAT", defaultTopicHeartbeat),

		Description: os.Getenv("DESCRIPTION"),
	}

	var err error

	bootstrap, err := requireEnv("KAFKA_BOOTSTRAP_SERVERS")
	if err != nil {
		return Config{}, err
	}
	cfg.KafkaBootstrapServers = splitCSV(bootstrap)

	if cfg.KafkaUsername, err = requireEnv("KAFKA_USERNAME"); err != nil {
		return Config{}, err
	}
	if cfg.KafkaPassword, err = requireEnv("KAFKA_PASSWORD"); err != nil {
		return Config{}, err
	}
	if cfg.KafkaGroupID, err = requireEnv("KAFKA_GROUP_ID"); err != nil {
		return Config{}, err
	}
	if cfg.CandleTopic, err = requireEnv("CANDLE_TOPIC"); err != nil {
		return Config{}, err
	}

	if cfg.AccountSecretKey, err = requireEnv("ACCOUNT_SECRET_KEY"); err != nil {
		return Config{}, err
	}
	if cfg.AccountID, err = requireEnv("ACCOUNT_ID"); err != nil {
		return Config{}, err
	}
	if cfg.AccountName, err = requireEnv("ACCOUNT_NAME"); err != nil {
		return Config{}, err
	}
	if cfg.GroupID, err = requireEnv("GROUP_ID"); err != nil {
		return Config{}, err
	}
	if cfg.StrategyID, err = requireEnv("STRATEGY_ID"); err != nil {
		return Config{}, err
	}
	if cfg.StrategyName, err = requireEnv("STRATEGY_NAME"); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func requireEnv(name string) (string, error) {
	v := os.Getenv(name)
	if v == "" {
		return "", fmt.Errorf("config: required environment variable %s is not set", name)
	}
	return v, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
