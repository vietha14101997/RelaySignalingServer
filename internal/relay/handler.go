package relay

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  64 * 1024, // 64KB
	WriteBufferSize: 64 * 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for now; tighten in production
	},
}

type Handler struct {
	hub            *Hub
	maxMessageSize int64
}

func NewHandler(hub *Hub, maxMessageSize int64) *Handler {
	return &Handler{
		hub:            hub,
		maxMessageSize: maxMessageSize,
	}
}

func (h *Handler) HandleWebSocket(c echo.Context) error {
	roleStr := c.QueryParam("role")
	roomID := c.QueryParam("room")

	if roomID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "room parameter required"})
	}
	if roleStr != string(RoleServer) && roleStr != string(RoleClient) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "role must be 'server' or 'client'"})
	}

	role := Role(roleStr)

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		log.Printf("[WS] Upgrade failed: %v", err)
		return err
	}

	conn := NewConnection(ws, role, roomID)
	log.Printf("[WS] %s connected to room %s", role, roomID)

	// Start write pump
	go conn.WritePump()

	// Join room
	paired := h.hub.Join(roomID, conn)
	if paired {
		log.Printf("[Room] %s is now paired", roomID)
		h.hub.NotifyRoomReady(roomID)
	}

	// Read pump (blocks until connection closes)
	h.readPump(conn)

	return nil
}

func (h *Handler) readPump(conn *Connection) {
	defer func() {
		h.hub.Leave(conn.RoomID, conn.Role)
		conn.Close()
		log.Printf("[WS] %s disconnected from room %s", conn.Role, conn.RoomID)
	}()

	conn.Conn.SetReadLimit(h.maxMessageSize)
	conn.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.Conn.SetPongHandler(func(string) error {
		conn.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		msgType, data, err := conn.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[WS] Read error room=%s role=%s: %v", conn.RoomID, conn.Role, err)
			}
			return
		}

		// Reset read deadline on any message
		conn.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		// Forward to peer
		h.hub.Forward(conn.RoomID, conn.Role, Message{Type: msgType, Data: data})
	}
}
