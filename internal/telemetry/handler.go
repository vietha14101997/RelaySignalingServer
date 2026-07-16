// Package telemetry collects fire-and-forget connection/QoE snapshots from
// clients to measure the P2P direct-connection rate and diagnose live path
// selection (see Telemetry Snapshot Contract v1, plan 260715-2315). It stores
// ONLY bounded aggregate counts keyed by low-cardinality enums, plus a bounded
// admin-only ring of recent raw snapshots — no unbounded per-session state.
// POST /telemetry/connection is public + rate-limited; GET /telemetry/stats is
// admin-gated (RELAY_ADMIN_TOKEN).
package telemetry

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
)

type Handler struct {
	adminToken string
	agg        *aggregator
}

func NewHandler(adminToken string) *Handler {
	return &Handler{adminToken: adminToken, agg: newAggregator()}
}

// Report ingests one telemetry snapshot. Fire-and-forget: it ALWAYS returns
// 204 and never surfaces an error to the client — malformed JSON is swallowed,
// not rejected, since telemetry must never affect app/connection state.
func (h *Handler) Report(c echo.Context) error {
	var r report
	if err := c.Bind(&r); err != nil {
		return c.NoContent(http.StatusNoContent)
	}
	r.normalize()
	h.agg.record(r)
	return c.NoContent(http.StatusNoContent)
}

// Stats returns the admin aggregate: totals, per-enum breakdowns, the
// computed direct-connection rate, and the bounded recent-snapshot ring.
// Admin-gated (mirrors the diagnose endpoint).
func (h *Handler) Stats(c echo.Context) error {
	if h.adminToken == "" {
		return c.JSON(http.StatusServiceUnavailable, map[string]string{
			"error": "telemetry stats disabled (RELAY_ADMIN_TOKEN not set)",
		})
	}
	if !h.checkToken(c) {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "invalid or missing admin token"})
	}

	data := h.agg.snapshot()

	// direct_rate: direct vs relay path_class, excluding "unknown" from the
	// denominator (mirrors the original selected-pair direct-rate metric).
	direct := data.pathClass["direct"]
	relay := data.pathClass["relay"]
	var directRate float64
	if graded := direct + relay; graded > 0 {
		directRate = float64(direct) / float64(graded)
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"total":            data.total,
		"path_class":       data.pathClass,
		"direct":           direct,
		"relay":            relay,
		"direct_rate":      directRate,
		"family":           data.family,
		"protocol":         data.protocol,
		"relay_protocol":   data.relayProtocol,
		"pc_role":          data.pcRole,
		"source":           data.source,
		"codec":            data.codec,
		"event":            data.event,
		"recent_snapshots": data.recent,
	})
}

func (h *Handler) checkToken(c echo.Context) bool {
	auth := c.Request().Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") && strings.TrimPrefix(auth, "Bearer ") == h.adminToken {
		return true
	}
	return c.QueryParam("token") == h.adminToken
}
