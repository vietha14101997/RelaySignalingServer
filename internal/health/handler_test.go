package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestHealthCheckReportsServiceCapabilities(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler := NewHandler(nil, nil, true, false)
	if err := handler.HealthCheck(e.NewContext(req, rec)); err != nil {
		t.Fatalf("HealthCheck returned error: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status ok, got %v", body["status"])
	}
	if body["auth_enabled"] != true {
		t.Fatalf("expected auth_enabled=true, got %v", body["auth_enabled"])
	}
	if body["turn_enabled"] != false {
		t.Fatalf("expected turn_enabled=false, got %v", body["turn_enabled"])
	}
}

func TestHealthCheckReportsDegradedWithoutAuth(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler := NewHandler(nil, nil, false, true)
	if err := handler.HealthCheck(e.NewContext(req, rec)); err != nil {
		t.Fatalf("HealthCheck returned error: %v", err)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["status"] != "degraded" {
		t.Fatalf("expected degraded status without auth, got %v", body["status"])
	}
}
