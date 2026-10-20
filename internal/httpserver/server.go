package httpserver

import (
	"net/http"
	"time"

	"github.com/omsu-informational-systems/alexey-shishin/internal/health"
)

const (
	readTimeout  = 5 * time.Second
	writeTimeout = 10 * time.Second
	idleTimeout  = 60 * time.Second
)

func New(addr, serviceName, serviceVersion string, registry *health.Registry) *http.Server {
	mux := http.NewServeMux()
	registerRoutesExemptFromFutureAuthMiddleware(mux, serviceName, serviceVersion, registry)

	return &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}
}

func registerRoutesExemptFromFutureAuthMiddleware(mux *http.ServeMux, serviceName, serviceVersion string, registry *health.Registry) {
	mux.HandleFunc("/health", newHealthHandler(serviceName, serviceVersion, registry))
}
