// Package telemetry collects fire-and-forget connection outcomes from clients
// to measure the P2P direct-connection rate (roadmap Success Metric). It stores
// ONLY bounded aggregate counts keyed by (selected_pair_type, address_family) —
// no PII, no IP addresses, no identifiers. POST /telemetry/connection is public
// + rate-limited; GET /telemetry/stats is admin-gated (RELAY_ADMIN_TOKEN).
package telemetry

import (
	"net/http"
	"strings"
	"sync"

	"github.com/labstack/echo/v4"
)

// Whitelists bound the counter cardinality so an abusive client cannot make the
// map grow unbounded — any value off-list is folded into "unknown".
var (
	validPairTypes = map[string]bool{"host": true, "srflx": true, "prflx": true, "relay": true, "unknown": true}
	validFamilies  = map[string]bool{"ipv4": true, "ipv6": true, "unknown": true}
)

type report struct {
	SelectedPairType string `json:"selected_pair_type"`
	AddressFamily    string `json:"address_family"`
}

type Handler struct {
	adminToken string
	mu         sync.Mutex
	counts     map[string]int64 // "pairType/family" -> count
	total      int64
}

func NewHandler(adminToken string) *Handler {
	return &Handler{adminToken: adminToken, counts: make(map[string]int64)}
}

// Report ingests one connection outcome. Fire-and-forget: it ALWAYS returns 204
// and never surfaces an error to the client (telemetry must not affect the app).
func (h *Handler) Report(c echo.Context) error {
	var r report
	if err := c.Bind(&r); err != nil {
		return c.NoContent(http.StatusNoContent)
	}

	pt := strings.ToLower(strings.TrimSpace(r.SelectedPairType))
	fam := strings.ToLower(strings.TrimSpace(r.AddressFamily))
	if !validPairTypes[pt] {
		pt = "unknown"
	}
	if !validFamilies[fam] {
		fam = "unknown"
	}

	h.mu.Lock()
	h.counts[pt+"/"+fam]++
	h.total++
	h.mu.Unlock()
	return c.NoContent(http.StatusNoContent)
}

// Stats returns aggregate counts + the computed direct-connection rate.
// direct = host+srflx+prflx; relay = relay; "unknown" is excluded from the rate
// denominator. Admin-gated (mirrors the diagnose endpoint).
func (h *Handler) Stats(c echo.Context) error {
	if h.adminToken == "" {
		return c.JSON(http.StatusServiceUnavailable, map[string]string{
			"error": "telemetry stats disabled (RELAY_ADMIN_TOKEN not set)",
		})
	}
	if !h.checkToken(c) {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "invalid or missing admin token"})
	}

	h.mu.Lock()
	counts := make(map[string]int64, len(h.counts))
	var direct, relay int64
	for k, v := range h.counts {
		counts[k] = v
		switch {
		case strings.HasPrefix(k, "relay/"):
			relay += v
		case strings.HasPrefix(k, "unknown/"):
			// excluded from the graded rate
		default:
			direct += v
		}
	}
	total := h.total
	h.mu.Unlock()

	var directRate float64
	if graded := direct + relay; graded > 0 {
		directRate = float64(direct) / float64(graded)
	}
	return c.JSON(http.StatusOK, map[string]interface{}{
		"total":       total,
		"counts":      counts,
		"direct":      direct,
		"relay":       relay,
		"direct_rate": directRate,
	})
}

func (h *Handler) checkToken(c echo.Context) bool {
	auth := c.Request().Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") && strings.TrimPrefix(auth, "Bearer ") == h.adminToken {
		return true
	}
	return c.QueryParam("token") == h.adminToken
}
