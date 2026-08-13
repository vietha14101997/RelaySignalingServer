package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestUnavailableAuthRoutesReturnServiceUnavailable(t *testing.T) {
	e := echo.New()
	registerUnavailableAuthRoutes(e)

	req := httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(`{}`))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "database is not configured") {
		t.Fatalf("expected actionable error body, got %s", rec.Body.String())
	}
}
