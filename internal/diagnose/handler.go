package diagnose

import (
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/reka/relay-server/internal/relay"
)

// Handler exposes a debug endpoint that returns room state, ICE summaries
// (sent by clients), and recent events. Requires RELAY_ADMIN_TOKEN in the
// Authorization header (Bearer scheme) or ?token=... query param.
//
// Example:
//
//	curl -H "Authorization: Bearer $RELAY_ADMIN_TOKEN" \
//	  https://relay.example.com/diagnose/ABC123
type Handler struct {
	deviceHub  *relay.DeviceHub
	adminToken string
}

func NewHandler(deviceHub *relay.DeviceHub, adminToken string) *Handler {
	return &Handler{deviceHub: deviceHub, adminToken: adminToken}
}

func (h *Handler) Diagnose(c echo.Context) error {
	if h.adminToken == "" {
		return c.JSON(http.StatusServiceUnavailable, map[string]string{
			"error": "diagnose endpoint disabled (RELAY_ADMIN_TOKEN not set)",
		})
	}
	if !h.checkToken(c) {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "invalid or missing admin token"})
	}

	roomID := c.Param("room_id")
	if roomID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "room_id required"})
	}

	room := h.deviceHub.GetRoom(roomID)
	if room == nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "room not found or expired"})
	}

	// Snapshot under read lock (via exported helper — room.mu is private to
	// the relay package, see relay.Room.WithReadLock)
	var clients []map[string]interface{}
	var state string
	var clientCount int
	room.WithReadLock(func() {
		clients = make([]map[string]interface{}, 0, len(room.Clients))
		for _, cl := range room.Clients {
			clients = append(clients, map[string]interface{}{
				"client_id":     cl.ID,
				"role":          string(cl.Role),
				"joined_at":     cl.JoinedAt,
				"input_allowed": cl.InputAllowed,
				"uptime_secs":   int(time.Since(cl.JoinedAt).Seconds()),
			})
		}
		state = string(room.State)
		clientCount = len(room.Clients)
	})
	createdAt := room.CreatedAt
	uptimeSecs := int(time.Since(createdAt).Seconds())

	// Optional ICE summary from server / clients
	// Clients can publish via WS message: { "type": "ice_summary", "role": "host|viewer", "h":N, "s":N, "r":N, "p":N, "gather_ms":N, "ts":"ISO" }
	// We don't store these centrally yet — return what the room struct knows.
	servers, sessions := h.deviceHub.Stats()

	return c.JSON(http.StatusOK, map[string]interface{}{
		"room_id":      roomID,
		"display_id":   room.DisplayID(),
		"device_id":    room.DeviceID,
		"state":        state,
		"client_count": clientCount,
		"clients":      clients,
		"created_at":   createdAt,
		"uptime_secs":  uptimeSecs,
		"server_stats": map[string]int{
			"online_servers":  servers,
			"active_sessions": sessions,
		},
		"note": "ICE summary aggregation not yet implemented — clients should log to their own console. " +
			"Future versions will store per-room H/S/R/P counts.",
	})
}

func (h *Handler) checkToken(c echo.Context) bool {
	auth := c.Request().Header.Get("Authorization")
	const bearer = "Bearer "
	if len(auth) > len(bearer) && auth[:len(bearer)] == bearer {
		return auth[len(bearer):] == h.adminToken
	}
	if t := c.QueryParam("token"); t != "" {
		return t == h.adminToken
	}
	return false
}
