package relay

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/reka/relay-server/internal/auth"
)

type WSHandler struct {
	deviceHub      *DeviceHub
	maxMessageSize int64
}

func NewWSHandler(deviceHub *DeviceHub, maxMessageSize int64) *WSHandler {
	return &WSHandler{
		deviceHub:      deviceHub,
		maxMessageSize: maxMessageSize,
	}
}

// HandleServerWS handles /ws/server — Windows server presence connection.
func (h *WSHandler) HandleServerWS(c echo.Context) error {
	userID := c.Get(auth.ContextUserID).(string)
	deviceID := c.QueryParam("device_id")

	if deviceID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "device_id required"})
	}

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		log.Printf("[WS/Server] Upgrade failed: %v", err)
		return err
	}

	server := h.deviceHub.AddServer(deviceID, userID, ws)

	// Read pump — route server messages to room clients
	h.serverReadPump(server)

	return nil
}

func (h *WSHandler) serverReadPump(server *OnlineServer) {
	defer func() {
		h.deviceHub.RemoveServer(server.DeviceID)
		log.Printf("[WS/Server] Device %s read pump ended", server.DeviceID)
	}()

	server.Conn.SetReadLimit(h.maxMessageSize)
	server.Conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	server.Conn.SetPongHandler(func(string) error {
		server.Conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})

	for {
		msgType, data, err := server.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[WS/Server] Read error device=%s: %v", server.DeviceID, err)
			}
			return
		}
		server.Conn.SetReadDeadline(time.Now().Add(90 * time.Second))

		// Route message: check for Room first, fallback to legacy Session
		room := h.deviceHub.GetRoomByDevice(server.DeviceID)
		if room != nil {
			h.routeServerMessage(room, msgType, data)
			continue
		}

		// Legacy session fallback (backward compatibility)
		h.deviceHub.mu.RLock()
		for _, sess := range h.deviceHub.sessions {
			if sess.DeviceID == server.DeviceID && sess.ClientConn != nil {
				sess.ClientConn.Send(Message{Type: msgType, Data: data})
				break
			}
		}
		h.deviceHub.mu.RUnlock()
	}
}

// routeServerMessage routes a server message to room clients.
// Text messages with "target" field → send to specific client.
// Text messages without "target" → broadcast to all clients.
// Binary messages → always broadcast.
func (h *WSHandler) routeServerMessage(room *Room, msgType int, data []byte) {
	if msgType == websocket.BinaryMessage {
		// Binary (stream data) → broadcast
		room.BroadcastToClients(Message{Type: msgType, Data: data})
		return
	}

	// Text message: check for "target" field
	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		// Non-JSON text (ping/pong) → broadcast
		room.BroadcastToClients(Message{Type: msgType, Data: data})
		return
	}

	target, hasTarget := msg["target"].(string)
	if hasTarget && target != "" {
		// Targeted message → send to specific client only
		room.SendToClient(target, Message{Type: msgType, Data: data})
	} else {
		// No target → broadcast to all clients
		room.BroadcastToClients(Message{Type: msgType, Data: data})
	}
}

// HandleClientWS handles /ws/client — legacy session-based client connection.
func (h *WSHandler) HandleClientWS(c echo.Context) error {
	userID := c.Get(auth.ContextUserID).(string)
	sessionID := c.QueryParam("session")

	if sessionID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "session parameter required"})
	}

	sess := h.deviceHub.GetSession(sessionID)
	if sess == nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "session not found"})
	}
	if sess.UserID != userID {
		return c.JSON(http.StatusForbidden, map[string]string{"error": "session does not belong to you"})
	}

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		log.Printf("[WS/Client] Upgrade failed: %v", err)
		return err
	}

	conn := NewConnection(ws, RoleClient, sessionID)
	go conn.WritePump()

	h.deviceHub.SetSessionClient(sessionID, conn)

	conn.Send(Message{
		Type: websocket.TextMessage,
		Data: mustJSON(map[string]string{"type": "room_ready", "session_id": sessionID}),
	})
	if sess.ServerConn != nil {
		sess.ServerConn.SendJSON(map[string]string{"type": "room_ready", "session_id": sessionID})
	}

	log.Printf("[WS/Client] Client joined session %s", sessionID)
	h.clientReadPump(conn, sessionID)

	return nil
}

func (h *WSHandler) clientReadPump(conn *Connection, sessionID string) {
	defer func() {
		sess := h.deviceHub.GetSession(sessionID)
		if sess != nil && sess.ServerConn != nil {
			sess.ServerConn.SendJSON(map[string]string{"type": "peer_disconnected"})
		}
		h.deviceHub.RemoveSession(sessionID)
		conn.Close()
		log.Printf("[WS/Client] Client disconnected from session %s", sessionID)
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
				log.Printf("[WS/Client] Read error session=%s: %v", sessionID, err)
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
