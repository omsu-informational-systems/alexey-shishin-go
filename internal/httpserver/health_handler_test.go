package httpserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/omsu-informational-systems/alexey-shishin/internal/health"
)

func TestHealthHandlerReturnsOKWithoutAuthorization(t *testing.T) {
	registry := health.NewRegistry()
	srv := New(":0", "vwap-reversion", "0.1.0", registry)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Fatalf("expected Content-Type %q, got %q", "application/json", contentType)
	}

	var body struct {
		Status     string                 `json:"status"`
		Service    string                 `json:"service"`
		Version    string                 `json:"version"`
		Components map[string]interface{} `json:"components"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if body.Status != string(health.StatusUP) {
		t.Fatalf("expected status %q, got %q", health.StatusUP, body.Status)
	}
	if body.Service != "vwap-reversion" {
		t.Fatalf("expected service %q, got %q", "vwap-reversion", body.Service)
	}
	if body.Version != "0.1.0" {
		t.Fatalf("expected version %q, got %q", "0.1.0", body.Version)
	}
	if body.Components == nil || len(body.Components) != 0 {
		t.Fatalf("expected empty components object, got %v", body.Components)
	}
}
