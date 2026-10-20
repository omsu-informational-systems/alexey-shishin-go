package config

import "os"

type Config struct {
	Port           string
	ServiceName    string
	ServiceVersion string
}

const (
	defaultPort           = "8080"
	defaultServiceName    = "vwap-reversion"
	defaultServiceVersion = "0.1.0"
)

func Load() Config {
	return Config{
		Port:           getEnv("PORT", defaultPort),
		ServiceName:    getEnv("SERVICE_NAME", defaultServiceName),
		ServiceVersion: getEnv("SERVICE_VERSION", defaultServiceVersion),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
