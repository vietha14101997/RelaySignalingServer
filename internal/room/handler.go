package room

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"golang.org/x/crypto/bcrypt"

	"github.com/reka/relay-server/internal/auth"
	"github.com/reka/relay-server/internal/relay"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  64 * 1024,
	WriteBufferSize: 64 * 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type Handler struct {
	deviceHub      *relay.DeviceHub
	maxMessageSize int64
}

func NewHandler(deviceHub *relay.DeviceHub, maxMessageSize int64) *Handler {
	return &Handler{deviceHub: deviceHub, maxMessageSize: maxMessageSize}
}

// --- POST /rooms/create (JWT required — server calls on startup) ---

func (h *Handler) Create(c echo.Context) error {
	userID := c.Get(auth.ContextUserID).(string)
	deviceID := c.QueryParam("device_id")

	if deviceID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "device_id required"})
	}

	room, err := h.deviceHub.CreateRoom(deviceID, userID)
	if err == relay.ErrServerOffline {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "server not connected"})
	}
	if err == relay.ErrRoomAlreadyExists {
		return c.JSON(http.StatusConflict, map[string]string{"error": "server already has a room"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to create room"})
	}

	return c.JSON(http.StatusCreated, map[string]interface{}{
		"room_id":    room.ID,
		"display_id": room.DisplayID(),
	})
}

// --- POST /rooms/set-password (JWT required — server calls) ---

type setPasswordRequest struct {
	RoomID   string `json:"room_id"`
	Password string `json:"password"`
}

func (h *Handler) SetPassword(c echo.Context) error {
	userID := c.Get(auth.ContextUserID).(string)

	var req setPasswordRequest
	if err := c.Bind(&req); err != nil || req.RoomID == "" || req.Password == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "room_id and password required"})
	}

	room := h.deviceHub.GetRoom(req.RoomID)
	if room == nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "room not found"})
	}
	if room.OwnerUserID != userID {
		return c.JSON(http.StatusForbidden, map[string]string{"error": "not room owner"})
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), 10)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}

	room.PasswordHash = string(hash)
	return c.JSON(http.StatusOK, map[string]interface{}{"updated": true})
}

// --- POST /rooms/join (PUBLIC — client calls, replaces /sessions/guest) ---

type joinRequest struct {
	RoomID   string `json:"room_id"`
	Password string `json:"password"`
}

func (h *Handler) Join(c echo.Context) error {
	var req joinRequest
	if err := c.Bind(&req); err != nil || req.RoomID == "" || req.Password == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "room_id and password required"})
	}

	// Normalize: remove dashes/spaces
	roomID := normalizeRoomID(req.RoomID)

	room := h.deviceHub.GetRoom(roomID)
	if room == nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "room not found"})
	}

	// Verify password
	if err := bcrypt.CompareHashAndPassword([]byte(room.PasswordHash), []byte(req.Password)); err != nil {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "invalid password"})
	}

	if room.IsFull() {
		return c.JSON(http.StatusConflict, map[string]string{"error": "room is full"})
	}

	// Determine role: check if request has JWT (logged-in user) or guest
	userID := ""
	if uid, ok := c.Get(auth.ContextUserID).(string); ok {
		userID = uid
	}

	role := room.DetermineRole(userID)
	clientID := uuid.New().String()

	// Notify server about new client joining
	room.ServerConn.SendJSON(map[string]interface{}{
		"type":      "client_joining",
		"client_id": clientID,
		"role":      string(role),
		"room_id":   roomID,
	})

	return c.JSON(http.StatusOK, map[string]interface{}{
		"room_id":   roomID,
		"state":     string(room.GetState()),
		"role":      string(role),
		"client_id": clientID,
	})
}

// --- GET /rooms/:id/info (PUBLIC) ---

func (h *Handler) GetInfo(c echo.Context) error {
	roomID := normalizeRoomID(c.Param("id"))

	room := h.deviceHub.GetRoom(roomID)
	if room == nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "room not found"})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"room_id":     roomID,
		"display_id":  room.DisplayID(),
		"state":       string(room.GetState()),
		"clients":     room.ClientCount(),
		"max_clients": relay.MaxClientsPerRoom,
	})
}

// --- GET /ws/room?room_id=X&client_id=Y (PUBLIC — client WS connection) ---

func (h *Handler) HandleRoomWS(c echo.Context) error {
	roomID := normalizeRoomID(c.QueryParam("room_id"))
	clientID := c.QueryParam("client_id")

	if roomID == "" || clientID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "room_id and client_id required"})
	}

	room := h.deviceHub.GetRoom(roomID)
	if room == nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "room not found"})
	}

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		log.Printf("[WS/Room] Upgrade failed: %v", err)
		return err
	}

	conn := relay.NewConnection(ws, relay.RoleClient, roomID)
	go conn.WritePump()

	// Find or create room client with pre-assigned role from /rooms/join
	role := relay.RoleViewer
	if room.ClientCount() == 0 && room.OwnerUserID == "" {
		role = relay.RoleHost
	}

	client := &relay.RoomClient{
		ID:           clientID,
		Role:         role,
		Conn:         conn,
		InputAllowed: role == relay.RoleHost,
		JoinedAt:     time.Now(),
	}

	if err := room.AddClient(client); err != nil {
		conn.Close()
		return nil
	}

	// Notify both sides
	readyMsg, _ := json.Marshal(map[string]interface{}{
		"type":      "room_ready",
		"room_id":   roomID,
		"client_id": clientID,
		"role":      string(role),
		"state":     string(room.GetState()),
	})
	conn.Send(relay.Message{Type: websocket.TextMessage, Data: readyMsg})
	room.ServerConn.SendJSON(map[string]interface{}{
		"type":      "room_ready",
		"room_id":   roomID,
		"client_id": clientID,
		"role":      string(role),
	})

	log.Printf("[WS/Room] Client %s joined room %s as %s", clientID, roomID, role)

	// Read pump — forward client messages to server (tagged with client_id)
	h.clientReadPump(conn, room, clientID)
	return nil
}

func (h *Handler) clientReadPump(conn *relay.Connection, room *relay.Room, clientID string) {
	defer func() {
		room.RemoveClient(clientID)
		room.ServerConn.SendJSON(map[string]string{
			"type":      "client_left",
			"client_id": clientID,
			"room_id":   room.ID,
		})
		conn.Close()
		log.Printf("[WS/Room] Client %s disconnected from room %s", clientID, room.ID)
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
				log.Printf("[WS/Room] Read error room=%s client=%s: %v", room.ID, clientID, err)
			}
			return
		}
		conn.Conn.SetReadDeadline(time.Now().Add(90 * time.Second))

		// Tag message with client_id for server to know who sent it
		// For text messages, inject client_id into JSON
		if msgType == websocket.TextMessage {
			var msg map[string]interface{}
			if json.Unmarshal(data, &msg) == nil {
				msg["_client_id"] = clientID
				if tagged, err := json.Marshal(msg); err == nil {
					data = tagged
				}
			}
		}

		// Forward to server
		room.ServerConn.SendMessage(msgType, data)
	}
}

func normalizeRoomID(id string) string {
	result := make([]byte, 0, len(id))
	for _, c := range id {
		if c != '-' && c != ' ' {
			if c >= 'a' && c <= 'z' {
				c -= 32 // uppercase
			}
			result = append(result, byte(c))
		}
	}
	return string(result)
}
