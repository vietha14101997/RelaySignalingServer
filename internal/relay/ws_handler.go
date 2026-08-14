package relay

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/reka/relay-server/internal/auth"
)

type DeviceOwnerChecker interface {
	IsOwner(ctx context.Context, deviceID, userID string) (bool, error)
}

type WSHandler struct {
	deviceHub      *DeviceHub
	deviceOwners   DeviceOwnerChecker
	maxMessageSize int64
}

func NewWSHandler(deviceHub *DeviceHub, deviceOwners DeviceOwnerChecker, maxMessageSize int64) *WSHandler {
	return &WSHandler{
		deviceHub:      deviceHub,
		deviceOwners:   deviceOwners,
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
	owned, err := h.deviceOwners.IsOwner(c.Request().Context(), deviceID, userID)
	if err != nil {
		log.Printf("[WS/Server] Device ownership lookup failed device=%s user=%s: %v", deviceID, userID, err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to verify device ownership"})
	}
	if !owned {
		return c.JSON(http.StatusForbidden, map[string]string{"error": "not authorized for this device"})
	}

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		log.Printf("[WS/Server] Upgrade failed: %v", err)
		return err
	}

	// AddServer rejects the connection (ErrNotOwner) if a server-stale session
	// for this device is owned by a different JWT user (F12b: rebind during
	// grace must re-auth AND match the bound device/user — grace is not a
	// free-auth window).
	server, err := h.deviceHub.AddServer(deviceID, userID, ws)
	if err != nil {
		log.Printf("[WS/Server] Rejected device=%s user=%s: %v", deviceID, userID, err)
		ws.WriteMessage(websocket.TextMessage, mustJSON(map[string]string{"type": "error", "error": "not authorized for this device"}))
		ws.Close()
		return nil
	}

	// Read pump — route server messages to room clients
	h.serverReadPump(server)

	return nil
}

func (h *WSHandler) serverReadPump(server *OnlineServer) {
	defer func() {
		// RemoveServerConn (not RemoveServer) — only tears down / starts grace
		// if `server` is still the currently-registered connection for its
		// device. Guards against a race where a faster reconnect has already
		// replaced it (must not disturb the newer connection).
		h.deviceHub.RemoveServerConn(server)
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

		if !h.routeCurrentServerMessage(server, msgType, data) {
			return
		}
	}
}

// routeCurrentServerMessage keeps the server identity check and enqueue under
// the hub read lock. Replacement either happens after this frame is routed or
// first makes server stale, in which case the frame is rejected.
func (h *WSHandler) routeCurrentServerMessage(server *OnlineServer, msgType int, data []byte) bool {
	h.deviceHub.mu.RLock()
	defer h.deviceHub.mu.RUnlock()
	select {
	case <-server.done:
		return false
	default:
	}
	if h.deviceHub.onlineServers[server.DeviceID] != server {
		return false
	}

	if roomID, ok := h.deviceHub.serverRooms[server.DeviceID]; ok {
		if room := h.deviceHub.rooms[roomID]; room != nil {
			h.routeServerMessage(room, msgType, data)
			return true
		}
	}

	// Legacy session fallback (backward compatibility).
	for _, sess := range h.deviceHub.sessions {
		if sess.DeviceID == server.DeviceID && sess.ClientConn != nil {
			sess.ClientConn.Send(Message{Type: msgType, Data: data})
			break
		}
	}
	return true
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

	serverConn, _ := h.deviceHub.SetSessionClient(sessionID, conn)

	conn.Send(Message{
		Type: websocket.TextMessage,
		Data: mustJSON(map[string]string{"type": "room_ready", "session_id": sessionID}),
	})
	// R3: use the ServerConn snapshot captured under the lock, not a fresh
	// unsynchronized read of sess.ServerConn (races server resume).
	if serverConn != nil {
		serverConn.SendJSON(map[string]string{"type": "room_ready", "session_id": sessionID})
	}

	log.Printf("[WS/Client] Client joined session %s", sessionID)
	h.clientReadPump(conn, sessionID)

	return nil
}

func (h *WSHandler) clientReadPump(conn *Connection, sessionID string) {
	defer func() {
		// F2: don't tear down eagerly — mark the session client-stale and let
		// the grace timer decide. If the client reconnects with ?session=
		// within the grace window (HandleClientWS -> SetSessionClient), the
		// timer is cancelled and no peer_disconnected is ever sent. Otherwise
		// the timer runs the equivalent of the old immediate teardown.
		h.deviceHub.MarkClientStale(sessionID, conn)
		conn.Close()
		log.Printf("[WS/Client] Client disconnected from session %s (grace started)", sessionID)
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
