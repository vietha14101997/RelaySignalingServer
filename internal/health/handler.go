package health

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/reka/relay-server/internal/relay"
)

type Handler struct {
	hub         *relay.Hub
	deviceHub   *relay.DeviceHub
	authEnabled bool
	turnEnabled bool
}

func NewHandler(hub *relay.Hub, deviceHub *relay.DeviceHub, authEnabled, turnEnabled bool) *Handler {
	return &Handler{
		hub:         hub,
		deviceHub:   deviceHub,
		authEnabled: authEnabled,
		turnEnabled: turnEnabled,
	}
}

func (h *Handler) HealthCheck(c echo.Context) error {
	status := "ok"
	if !h.authEnabled {
		status = "degraded"
	}
	result := map[string]interface{}{
		"status":       status,
		"auth_enabled": h.authEnabled,
		"turn_enabled": h.turnEnabled,
	}

	if h.hub != nil {
		rooms, connections := h.hub.Stats()
		result["rooms"] = rooms
		result["connections"] = connections
	}

	if h.deviceHub != nil {
		servers, sessions := h.deviceHub.Stats()
		result["online_servers"] = servers
		result["active_sessions"] = sessions
	}

	return c.JSON(http.StatusOK, result)
}
