package httpserver

import (
	"encoding/json"
	"net/http"

	"github.com/omsu-informational-systems/alexey-shishin/internal/health"
)

type healthResponse struct {
	Status     health.Status               `json:"status"`
	Service    string                      `json:"service"`
	Version    string                      `json:"version"`
	Components map[string]health.Component `json:"components"`
}

func newHealthHandler(serviceName, serviceVersion string, registry *health.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		report := registry.Aggregate(r.Context())

		resp := healthResponse{
			Status:     report.Status,
			Service:    serviceName,
			Version:    serviceVersion,
			Components: report.Components,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}
}
