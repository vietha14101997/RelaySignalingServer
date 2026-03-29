package device

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/reka/relay-server/internal/auth"
	"github.com/reka/relay-server/internal/relay"
)

type Handler struct {
	repo      *Repository
	deviceHub *relay.DeviceHub
}

func NewHandler(repo *Repository, deviceHub *relay.DeviceHub) *Handler {
	return &Handler{repo: repo, deviceHub: deviceHub}
}

type registerDeviceRequest struct {
	DeviceName string          `json:"device_name"`
	DeviceType string          `json:"device_type"`
	HWInfo     json.RawMessage `json:"hw_info,omitempty"`
}

func (h *Handler) Register(c echo.Context) error {
	userID := c.Get(auth.ContextUserID).(string)

	var req registerDeviceRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}
	if req.DeviceName == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "device_name required"})
	}
	if req.DeviceType == "" {
		req.DeviceType = "windows_server"
	}

	device, err := h.repo.RegisterDevice(c.Request().Context(), userID, req.DeviceName, req.DeviceType, req.HWInfo)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to register device"})
	}

	return c.JSON(http.StatusCreated, map[string]interface{}{
		"device_id":  device.ID,
		"registered": true,
	})
}

func (h *Handler) List(c echo.Context) error {
	userID := c.Get(auth.ContextUserID).(string)

	devices, err := h.repo.ListDevices(c.Request().Context(), userID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to list devices"})
	}
	if devices == nil {
		devices = []Device{}
	}

	// Enrich with online status from DeviceHub
	for i := range devices {
		devices[i].Online = h.deviceHub.IsOnline(devices[i].ID)
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"devices": devices,
	})
}

func (h *Handler) Delete(c echo.Context) error {
	userID := c.Get(auth.ContextUserID).(string)
	deviceID := c.Param("id")

	err := h.repo.DeleteDevice(c.Request().Context(), deviceID, userID)
	if err == ErrNotOwner {
		return c.JSON(http.StatusForbidden, map[string]string{"error": "not device owner"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to delete device"})
	}

	// Also disconnect if online
	h.deviceHub.RemoveServer(deviceID)

	return c.JSON(http.StatusOK, map[string]string{"message": "device deleted"})
}
