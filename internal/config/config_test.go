package config

import (
	"strings"
	"testing"
)

func setAllRequiredEnv(t *testing.T) {
	t.Helper()
	required := map[string]string{
		"KAFKA_BOOTSTRAP_SERVERS": "broker1:9094,broker2:9094",
		"KAFKA_USERNAME":          "student",
		"KAFKA_PASSWORD":          "pwd",
		"KAFKA_GROUP_ID":          "group-vwap-reversion",
		"CANDLE_TOPIC":            "market.candle.closed",
		"ACCOUNT_SECRET_KEY":      "secret",
		"ACCOUNT_ID":              "acc-1",
		"ACCOUNT_NAME":            "vwap-account",
		"GROUP_ID":                "teaching-group-1",
		"STRATEGY_ID":             "strategy-1",
		"STRATEGY_NAME":           "vwap-reversion",
	}
	for k, v := range required {
		t.Setenv(k, v)
	}
}

func TestLoadSucceedsWithAllRequiredVarsSet(t *testing.T) {
	setAllRequiredEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(cfg.KafkaBootstrapServers) != 2 || cfg.KafkaBootstrapServers[0] != "broker1:9094" || cfg.KafkaBootstrapServers[1] != "broker2:9094" {
		t.Fatalf("unexpected KafkaBootstrapServers: %v", cfg.KafkaBootstrapServers)
	}
	if cfg.TopicRegistrationRequest != defaultTopicRegistrationRequest {
		t.Fatalf("expected default registration request topic, got %q", cfg.TopicRegistrationRequest)
	}
	if cfg.TopicRegistrationResult != defaultTopicRegistrationResult {
		t.Fatalf("expected default registration result topic, got %q", cfg.TopicRegistrationResult)
	}
	if cfg.TopicHeartbeat != defaultTopicHeartbeat {
		t.Fatalf("expected default heartbeat topic, got %q", cfg.TopicHeartbeat)
	}
}

func TestLoadEachRequiredVarMissingNamesItInError(t *testing.T) {
	requiredVars := []string{
		"KAFKA_BOOTSTRAP_SERVERS",
		"KAFKA_USERNAME",
		"KAFKA_PASSWORD",
		"KAFKA_GROUP_ID",
		"CANDLE_TOPIC",
		"ACCOUNT_SECRET_KEY",
		"ACCOUNT_ID",
		"ACCOUNT_NAME",
		"GROUP_ID",
		"STRATEGY_ID",
		"STRATEGY_NAME",
	}

	for _, missing := range requiredVars {
		t.Run(missing, func(t *testing.T) {
			setAllRequiredEnv(t)
			t.Setenv(missing, "")

			_, err := Load()
			if err == nil {
				t.Fatalf("expected an error when %s is missing, got nil", missing)
			}
			if !strings.Contains(err.Error(), missing) {
				t.Fatalf("expected error to name the missing variable %s, got %q", missing, err.Error())
			}
		})
	}
}

func TestLoadDoesNotExitProcessOnMissingVar(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "")

	_, err := Load()
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
}

func TestLoadDefaultsForOptionalFields(t *testing.T) {
	setAllRequiredEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.Port != defaultPort {
		t.Fatalf("expected default port %q, got %q", defaultPort, cfg.Port)
	}
	if cfg.ServiceName != defaultServiceName {
		t.Fatalf("expected default service name %q, got %q", defaultServiceName, cfg.ServiceName)
	}
	if cfg.Description != "" {
		t.Fatalf("expected empty description by default, got %q", cfg.Description)
	}
}
