package telemetry

import (
	"encoding/json"
	"fmt"
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

// fullSnapshotJSON returns a valid full-contract payload with the given
// overrides layered on top of sane defaults.
func fullSnapshotJSON(overrides map[string]interface{}) string {
	base := map[string]interface{}{
		"schema_version":         1,
		"source":                 "android",
		"event":                  "snapshot",
		"session_id":             "sess-abc",
		"pc_role":                "main",
		"monitor_index":          0,
		"generation":             1,
		"sequence":               1,
		"local_candidate_type":   "relay",
		"remote_candidate_type":  "srflx",
		"address_family":         "ipv4",
		"protocol":               "udp",
		"relay_protocol":         "udp",
		"path_class":             "relay",
		"rtt_ms":                 42,
		"jitter_ms":              5,
		"loss_pct":               0.8,
		"send_bitrate_kbps":      8000,
		"available_bitrate_kbps": 12000,
		"codec":                  "h265",
		"width":                  1920,
		"height":                 1080,
		"fps":                    30,
		"qp":                     24,
		"frame_drops":            0,
		"ttff_ms":                1200,
		"freeze_count":           0,
		"freeze_ms_total":        0,
	}
	for k, v := range overrides {
		base[k] = v
	}
	b, err := json.Marshal(base)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestReportAlwaysNoContent(t *testing.T) {
	h := NewHandler("secret")
	if code := postReport(t, h, fullSnapshotJSON(nil)); code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", code)
	}
	// Malformed JSON must still be swallowed as 204 (fire-and-forget).
	if code := postReport(t, h, `{not json`); code != http.StatusNoContent {
		t.Errorf("malformed body: expected 204, got %d", code)
	}
	// Empty body must also be swallowed as 204.
	if code := postReport(t, h, ``); code != http.StatusNoContent {
		t.Errorf("empty body: expected 204, got %d", code)
	}
}

func TestFullPayloadIngestAndAggregate(t *testing.T) {
	h := NewHandler("secret")
	postReport(t, h, fullSnapshotJSON(nil))

	_, stats := getStats(t, h, "secret")
	if got := stats["total"].(float64); got != 1 {
		t.Fatalf("total: want 1, got %v", got)
	}
	pathClass := stats["path_class"].(map[string]interface{})
	if pathClass["relay"].(float64) != 1 {
		t.Errorf("path_class[relay]: want 1, got %v", pathClass)
	}
	family := stats["family"].(map[string]interface{})
	if family["ipv4"].(float64) != 1 {
		t.Errorf("family[ipv4]: want 1, got %v", family)
	}
	codec := stats["codec"].(map[string]interface{})
	if codec["h265"].(float64) != 1 {
		t.Errorf("codec[h265]: want 1, got %v", codec)
	}
	source := stats["source"].(map[string]interface{})
	if source["android"].(float64) != 1 {
		t.Errorf("source[android]: want 1, got %v", source)
	}
	event := stats["event"].(map[string]interface{})
	if event["snapshot"].(float64) != 1 {
		t.Errorf("event[snapshot]: want 1, got %v", event)
	}

	recent := stats["recent_snapshots"].([]interface{})
	if len(recent) != 1 {
		t.Fatalf("recent_snapshots: want 1 entry, got %d", len(recent))
	}
	entry := recent[0].(map[string]interface{})
	if entry["session_id"] != "sess-abc" {
		t.Errorf("recent_snapshots[0].session_id: want sess-abc, got %v", entry["session_id"])
	}
	if entry["rtt_ms"].(float64) != 42 {
		t.Errorf("recent_snapshots[0].rtt_ms: want 42, got %v", entry["rtt_ms"])
	}
	if _, ok := entry["received_at"]; !ok {
		t.Errorf("recent_snapshots[0] missing received_at")
	}
}

func TestUnknownEnumsFoldToUnknown(t *testing.T) {
	h := NewHandler("secret")
	postReport(t, h, fullSnapshotJSON(map[string]interface{}{
		"source":                "HACK",
		"event":                 "bogus_event",
		"pc_role":               "nope",
		"local_candidate_type":  "made_up",
		"remote_candidate_type": "made_up",
		"address_family":        "ipv9",
		"protocol":              "sctp",
		"relay_protocol":        "quic",
		"path_class":            "sideways",
		"codec":                 "vp9",
	}))

	_, stats := getStats(t, h, "secret")
	for _, dim := range []string{"source", "pc_role", "family", "protocol", "relay_protocol", "path_class", "codec", "event"} {
		m := stats[dim].(map[string]interface{})
		if len(m) != 1 {
			t.Errorf("%s: cardinality must stay bounded (1 key after fold), got %d: %v", dim, len(m), m)
		}
		if _, ok := m["unknown"]; !ok {
			t.Errorf("%s: off-list values should fold to unknown, got %v", dim, m)
		}
	}
}

func TestNumericClampingAndDropping(t *testing.T) {
	h := NewHandler("secret")
	// rtt_ms within bound (kept), jitter_ms way over bound (clamped),
	// loss_pct negative sentinel (dropped -> absent from recent snapshot),
	// qp absurdly high (clamped).
	postReport(t, h, fullSnapshotJSON(map[string]interface{}{
		"rtt_ms":    100,
		"jitter_ms": 999_999,
		"loss_pct":  -5,
		"qp":        9999,
		"fps":       -1, // negative sentinel -> dropped
	}))

	_, stats := getStats(t, h, "secret")
	recent := stats["recent_snapshots"].([]interface{})
	entry := recent[0].(map[string]interface{})

	if entry["rtt_ms"].(float64) != 100 {
		t.Errorf("rtt_ms should pass through unchanged, got %v", entry["rtt_ms"])
	}
	if entry["jitter_ms"].(float64) != 60_000 {
		t.Errorf("jitter_ms should clamp to 60000, got %v", entry["jitter_ms"])
	}
	if _, ok := entry["loss_pct"]; ok {
		t.Errorf("negative loss_pct should be dropped (absent), got %v", entry["loss_pct"])
	}
	if entry["qp"].(float64) != 63 {
		t.Errorf("qp should clamp to 63, got %v", entry["qp"])
	}
	if _, ok := entry["fps"]; ok {
		t.Errorf("negative fps should be dropped (absent), got %v", entry["fps"])
	}
}

func TestMonitorIndexAndSessionIDBounding(t *testing.T) {
	h := NewHandler("secret")
	longSession := strings.Repeat("x", 500)
	postReport(t, h, fullSnapshotJSON(map[string]interface{}{
		"monitor_index": 999,
		"session_id":    longSession,
	}))

	_, stats := getStats(t, h, "secret")
	recent := stats["recent_snapshots"].([]interface{})
	entry := recent[0].(map[string]interface{})
	if entry["monitor_index"].(float64) != 15 {
		t.Errorf("monitor_index should clamp to 15, got %v", entry["monitor_index"])
	}
	if sid := entry["session_id"].(string); len(sid) != maxSessionIDLen {
		t.Errorf("session_id should truncate to %d chars, got %d", maxSessionIDLen, len(sid))
	}

	// Empty session_id defaults to "unknown".
	h2 := NewHandler("secret")
	postReport(t, h2, fullSnapshotJSON(map[string]interface{}{"session_id": ""}))
	_, stats2 := getStats(t, h2, "secret")
	recent2 := stats2["recent_snapshots"].([]interface{})
	if got := recent2[0].(map[string]interface{})["session_id"]; got != "unknown" {
		t.Errorf("empty session_id should default to unknown, got %v", got)
	}
}

func TestDirectRateComputation(t *testing.T) {
	h := NewHandler("secret")
	// 3 direct (host/srflx/prflx local pairs) + 1 relay + 1 unknown path_class
	// (excluded from the graded denominator) -> rate = 3/4 = 0.75.
	postReport(t, h, fullSnapshotJSON(map[string]interface{}{"path_class": "direct"}))
	postReport(t, h, fullSnapshotJSON(map[string]interface{}{"path_class": "direct"}))
	postReport(t, h, fullSnapshotJSON(map[string]interface{}{"path_class": "direct"}))
	postReport(t, h, fullSnapshotJSON(map[string]interface{}{"path_class": "relay"}))
	postReport(t, h, fullSnapshotJSON(map[string]interface{}{"path_class": "totally-bogus"})) // -> unknown

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

func TestRingBufferBoundAndEviction(t *testing.T) {
	h := NewHandler("secret")
	// Push more than ringCapacity snapshots, each with a unique sequence so we
	// can identify which ones survived eviction.
	total := ringCapacity + 50
	for i := 0; i < total; i++ {
		postReport(t, h, fullSnapshotJSON(map[string]interface{}{"sequence": i}))
	}

	_, stats := getStats(t, h, "secret")
	if got := stats["total"].(float64); got != float64(total) {
		t.Errorf("total should count every ingest even past ring capacity: want %d, got %v", total, got)
	}
	recent := stats["recent_snapshots"].([]interface{})
	if len(recent) != ringCapacity {
		t.Fatalf("ring buffer must stay bounded at %d, got %d", ringCapacity, len(recent))
	}
	// Newest-first: first entry should be the very last sequence pushed.
	newest := recent[0].(map[string]interface{})
	if newest["sequence"].(float64) != float64(total-1) {
		t.Errorf("recent_snapshots[0] should be newest (seq %d), got %v", total-1, newest["sequence"])
	}
	oldestRetained := recent[len(recent)-1].(map[string]interface{})
	if oldestRetained["sequence"].(float64) != float64(total-ringCapacity) {
		t.Errorf("recent_snapshots[last] should be oldest retained (seq %d), got %v", total-ringCapacity, oldestRetained["sequence"])
	}
}

func TestStatsOutputShape(t *testing.T) {
	h := NewHandler("secret")
	postReport(t, h, fullSnapshotJSON(nil))
	_, stats := getStats(t, h, "secret")

	for _, key := range []string{
		"total", "path_class", "direct", "relay", "direct_rate",
		"family", "protocol", "relay_protocol", "pc_role", "source", "codec", "event",
		"recent_snapshots",
	} {
		if _, ok := stats[key]; !ok {
			t.Errorf("stats response missing key %q: %v", key, stats)
		}
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

func TestStatsAcceptsQueryParamToken(t *testing.T) {
	h := NewHandler("secret")
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/telemetry/stats?token=secret", nil)
	rec := httptest.NewRecorder()
	if err := h.Stats(e.NewContext(req, rec)); err != nil {
		t.Fatalf("Stats returned error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("query-param token: want 200, got %d", rec.Code)
	}
}

func TestWsSafeModeEventsIngestWithoutSelectedPair(t *testing.T) {
	h := NewHandler("secret")
	// ws_safe_mode_* events carry the identity block only; selected-pair/QoE
	// fields are absent. Must still ingest cleanly (folds to unknown, not a
	// crash) and be counted under the event breakdown.
	payload := fmt.Sprintf(`{"schema_version":1,"source":"host","event":"ws_safe_mode_enter","session_id":"s1","pc_role":"main","monitor_index":0,"generation":1,"sequence":1}`)
	if code := postReport(t, h, payload); code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", code)
	}
	_, stats := getStats(t, h, "secret")
	event := stats["event"].(map[string]interface{})
	if event["ws_safe_mode_enter"].(float64) != 1 {
		t.Errorf("event[ws_safe_mode_enter]: want 1, got %v", event)
	}
	pathClass := stats["path_class"].(map[string]interface{})
	if pathClass["unknown"].(float64) != 1 {
		t.Errorf("path_class should fold missing field to unknown, got %v", pathClass)
	}
}
