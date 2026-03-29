package guest

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"golang.org/x/crypto/bcrypt"

	"github.com/reka/relay-server/internal/auth"
	"github.com/reka/relay-server/internal/relay"
)

type Handler struct {
	deviceHub      *relay.DeviceHub
	maxMessageSize int64
}

func NewHandler(deviceHub *relay.DeviceHub, maxMessageSize int64) *Handler {
	return &Handler{deviceHub: deviceHub, maxMessageSize: maxMessageSize}
}

// --- POST /guest/register (requires JWT — called by RemotePlayServer) ---

type registerRequest struct {
	ShortID    string `json:"short_id"`
	Password   string `json:"password"`
	DeviceID   string `json:"device_id"`
	DeviceName string `json:"device_name"`
}

func (h *Handler) Register(c echo.Context) error {
	userID := c.Get(auth.ContextUserID).(string)

	var req registerRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}
	if req.ShortID == "" || req.Password == "" || req.DeviceID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "short_id, password, and device_id required"})
	}

	// Hash password with bcrypt
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), 10)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}

	err = h.deviceHub.RegisterGuestDevice(req.ShortID, string(hash), userID, req.DeviceID, req.DeviceName)
	if err == relay.ErrShortIDConflict {
		return c.JSON(http.StatusConflict, map[string]string{"error": "short ID already in use"})
	}
	if err == relay.ErrServerOffline {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "server not connected via WebSocket"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "registration failed"})
	}

	return c.JSON(http.StatusCreated, map[string]interface{}{
		"registered": true,
		"short_id":   req.ShortID,
	})
}

// --- POST /sessions/guest (PUBLIC — called by Android client) ---

type guestSessionRequest struct {
	DeviceID string `json:"device_id"` // short ID
	Password string `json:"password"`
}

func (h *Handler) CreateSession(c echo.Context) error {
	var req guestSessionRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}
	if req.DeviceID == "" || req.Password == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "device_id and password required"})
	}

	guest := h.deviceHub.GetGuestDevice(req.DeviceID)
	if guest == nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "device not found or offline"})
	}

	// Verify password
	if err := bcrypt.CompareHashAndPassword([]byte(guest.PasswordHash), []byte(req.Password)); err != nil {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "invalid password"})
	}

	sess, err := h.deviceHub.CreateGuestSession(req.DeviceID)
	if err == relay.ErrServerBusy {
		return c.JSON(http.StatusConflict, map[string]string{"error": "server already has an active session"})
	}
	if err == relay.ErrServerOffline {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "device not found or offline"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to create session"})
	}

	// Notify server
	guest.Server.SendJSON(map[string]string{
		"type":       "session_request",
		"session_id": sess.ID,
	})

	return c.JSON(http.StatusCreated, map[string]interface{}{
		"session_id": sess.ID,
		"status":     "pending",
	})
}

// --- GET /ws/guest?session=xxx (PUBLIC — WebSocket for guest client) ---

var upgrader = websocket.Upgrader{
	ReadBufferSize:  64 * 1024,
	WriteBufferSize: 64 * 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func (h *Handler) HandleGuestWS(c echo.Context) error {
	sessionID := c.QueryParam("session")
	if sessionID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "session parameter required"})
	}

	sess := h.deviceHub.GetSession(sessionID)
	if sess == nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "session not found"})
	}

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		log.Printf("[WS/Guest] Upgrade failed: %v", err)
		return err
	}

	conn := relay.NewConnection(ws, relay.RoleClient, sessionID)
	go conn.WritePump()

	h.deviceHub.SetSessionClient(sessionID, conn)

	// Notify both sides
	conn.Send(relay.Message{
		Type: websocket.TextMessage,
		Data: mustJSON(map[string]string{"type": "room_ready", "session_id": sessionID}),
	})
	if sess.ServerConn != nil {
		sess.ServerConn.SendJSON(map[string]string{"type": "room_ready", "session_id": sessionID})
	}

	log.Printf("[WS/Guest] Client joined session %s", sessionID)

	// Read pump — forward guest messages to server
	h.guestReadPump(conn, sessionID)
	return nil
}

func (h *Handler) guestReadPump(conn *relay.Connection, sessionID string) {
	defer func() {
		sess := h.deviceHub.GetSession(sessionID)
		if sess != nil && sess.ServerConn != nil {
			sess.ServerConn.SendJSON(map[string]string{"type": "peer_disconnected"})
		}
		h.deviceHub.RemoveSession(sessionID)
		conn.Close()
		log.Printf("[WS/Guest] Client disconnected from session %s", sessionID)
	}()

	conn.Conn.SetReadLimit(h.maxMessageSize)
	conn.Conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	conn.Conn.SetPongHandler(func(string) error {
		conn.Conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})

	for {
		msgType, data, err := conn.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[WS/Guest] Read error session=%s: %v", sessionID, err)
			}
			return
		}
		conn.Conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		h.deviceHub.ForwardToServer(sessionID, msgType, data)
	}
}

func mustJSON(v interface{}) []byte {
	data, _ := json.Marshal(v)
	return data
}
