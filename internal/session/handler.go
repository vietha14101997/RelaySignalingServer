package session

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/reka/relay-server/internal/auth"
	"github.com/reka/relay-server/internal/relay"
)

type Handler struct {
	deviceHub *relay.DeviceHub
}

func NewHandler(deviceHub *relay.DeviceHub) *Handler {
	return &Handler{deviceHub: deviceHub}
}

type createSessionRequest struct {
	DeviceID string `json:"device_id"`
}

func (h *Handler) Create(c echo.Context) error {
	userID := c.Get(auth.ContextUserID).(string)

	var req createSessionRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}
	if req.DeviceID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "device_id required"})
	}

	sess, err := h.deviceHub.CreateSession(userID, req.DeviceID)
	if err == relay.ErrServerOffline {
		return c.JSON(http.StatusConflict, map[string]string{"error": "server is offline"})
	}
	if err == relay.ErrServerBusy {
		return c.JSON(http.StatusConflict, map[string]string{"error": "server already has an active session"})
	}
	if err == relay.ErrNotOwner {
		return c.JSON(http.StatusForbidden, map[string]string{"error": "device does not belong to you"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to create session"})
	}

	// Notify server about session request
	sess.ServerConn.SendJSON(map[string]string{
		"type":       "session_request",
		"session_id": sess.ID,
	})

	return c.JSON(http.StatusCreated, map[string]interface{}{
		"session_id": sess.ID,
		"status":     "pending",
	})
}
