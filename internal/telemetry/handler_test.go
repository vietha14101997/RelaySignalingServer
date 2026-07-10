package telemetry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

func postReport(t *testing.T, h *Handler, body string) int {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/telemetry/connection", strings.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	if err := h.Report(e.NewContext(req, rec)); err != nil {
		t.Fatalf("Report returned error: %v", err)
	}
	return rec.Code
}

func getStats(t *testing.T, h *Handler, token string) (int, map[string]interface{}) {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/telemetry/stats", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	if err := h.Stats(e.NewContext(req, rec)); err != nil {
		t.Fatalf("Stats returned error: %v", err)
	}
	var out map[string]interface{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec.Code, out
}

func TestReportAlwaysNoContent(t *testing.T) {
	h := NewHandler("secret")
	if code := postReport(t, h, `{"selected_pair_type":"srflx","address_family":"ipv4"}`); code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", code)
	}
	// Malformed JSON must still be swallowed as 204 (fire-and-forget).
	if code := postReport(t, h, `{not json`); code != http.StatusNoContent {
		t.Errorf("malformed body: expected 204, got %d", code)
	}
}

func TestWhitelistFoldsUnknown(t *testing.T) {
	h := NewHandler("secret")
	postReport(t, h, `{"selected_pair_type":"HACK","address_family":"ipv9"}`)
	_, stats := getStats(t, h, "secret")
	counts := stats["counts"].(map[string]interface{})
	if _, ok := counts["unknown/unknown"]; !ok {
		t.Errorf("off-list values should fold to unknown/unknown; counts=%v", counts)
	}
	if len(counts) != 1 {
		t.Errorf("cardinality must stay bounded (1 key), got %d: %v", len(counts), counts)
	}
}

func TestDirectRateComputation(t *testing.T) {
	h := NewHandler("secret")
	// 3 direct (host, srflx, prflx) + 1 relay + 1 unknown(excluded) → rate = 3/4 = 0.75
	postReport(t, h, `{"selected_pair_type":"host","address_family":"ipv6"}`)
	postReport(t, h, `{"selected_pair_type":"srflx","address_family":"ipv4"}`)
	postReport(t, h, `{"selected_pair_type":"prflx","address_family":"ipv4"}`)
	postReport(t, h, `{"selected_pair_type":"relay","address_family":"ipv4"}`)
	postReport(t, h, `{"selected_pair_type":"unknown","address_family":"unknown"}`)

	_, stats := getStats(t, h, "secret")
	if got := stats["total"].(float64); got != 5 {
		t.Errorf("total: want 5, got %v", got)
	}
	if got := stats["direct"].(float64); got != 3 {
		t.Errorf("direct: want 3, got %v", got)
	}
	if got := stats["relay"].(float64); got != 1 {
		t.Errorf("relay: want 1, got %v", got)
	}
	if got := stats["direct_rate"].(float64); got != 0.75 {
		t.Errorf("direct_rate: want 0.75, got %v", got)
	}
}

func TestStatsDisabledWithoutAdminToken(t *testing.T) {
	h := NewHandler("")
	if code, _ := getStats(t, h, ""); code != http.StatusServiceUnavailable {
		t.Errorf("no admin token configured: want 503, got %d", code)
	}
}

func TestStatsRejectsBadToken(t *testing.T) {
	h := NewHandler("secret")
	if code, _ := getStats(t, h, "wrong"); code != http.StatusUnauthorized {
		t.Errorf("bad token: want 401, got %d", code)
	}
}
