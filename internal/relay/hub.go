package relay

import (
	"encoding/json"
	"log"
	"sync"
	"time"
)

type LegacyRoom struct {
	Server    *Connection
	Client    *Connection
	CreatedAt time.Time
}

type Hub struct {
	rooms map[string]*LegacyRoom
	mu    sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		rooms: make(map[string]*LegacyRoom),
	}
}

// Join adds a connection to a room. Returns true if room is now paired.
func (h *Hub) Join(roomID string, conn *Connection) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	room, exists := h.rooms[roomID]
	if !exists {
		room = &LegacyRoom{CreatedAt: time.Now()}
		h.rooms[roomID] = room
	}

	switch conn.Role {
	case RoleServer:
		if room.Server != nil {
			room.Server.Close()
		}
		room.Server = conn
	case RoleClient:
		if room.Client != nil {
			room.Client.Close()
		}
		room.Client = conn
	}

	return room.Server != nil && room.Client != nil
}

// Leave removes a connection from its room and notifies the peer.
func (h *Hub) Leave(roomID string, role Role) {
	h.mu.Lock()
	room, exists := h.rooms[roomID]
	if !exists {
		h.mu.Unlock()
		return
	}

	var peer *Connection
	switch role {
	case RoleServer:
		room.Server = nil
		peer = room.Client
	case RoleClient:
		room.Client = nil
		peer = room.Server
	}

	// Remove room if empty
	if room.Server == nil && room.Client == nil {
		delete(h.rooms, roomID)
	}
	h.mu.Unlock()

	// Notify peer outside lock
	if peer != nil {
		msg, _ := json.Marshal(map[string]string{"type": "peer_disconnected"})
		peer.Send(Message{Type: 1, Data: msg}) // TextMessage = 1
	}
}

// Forward sends a message to the peer in the same room.
func (h *Hub) Forward(roomID string, fromRole Role, msg Message) bool {
	h.mu.RLock()
	room, exists := h.rooms[roomID]
	if !exists {
		h.mu.RUnlock()
		return false
	}

	var peer *Connection
	switch fromRole {
	case RoleServer:
		peer = room.Client
	case RoleClient:
		peer = room.Server
	}
	h.mu.RUnlock()

	if peer == nil {
		return false
	}
	return peer.Send(msg)
}

// NotifyRoomReady sends room_ready message to both peers.
func (h *Hub) NotifyRoomReady(roomID string) {
	h.mu.RLock()
	room, exists := h.rooms[roomID]
	if !exists {
		h.mu.RUnlock()
		return
	}
	server := room.Server
	client := room.Client
	h.mu.RUnlock()

	msg, _ := json.Marshal(map[string]string{"type": "room_ready", "room": roomID})
	readyMsg := Message{Type: 1, Data: msg}

	if server != nil {
		server.Send(readyMsg)
	}
	if client != nil {
		client.Send(readyMsg)
	}
}

// Stats returns the number of rooms and connections.
func (h *Hub) Stats() (rooms int, connections int) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	rooms = len(h.rooms)
	for _, r := range h.rooms {
		if r.Server != nil {
			connections++
		}
		if r.Client != nil {
			connections++
		}
	}
	return
}

// CleanupStaleRooms removes rooms older than maxAge with no activity.
func (h *Hub) CleanupStaleRooms(maxAge time.Duration) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	cleaned := 0
	for id, room := range h.rooms {
		if room.Server == nil && room.Client == nil && now.Sub(room.CreatedAt) > maxAge {
			delete(h.rooms, id)
			cleaned++
		}
	}

	if cleaned > 0 {
		log.Printf("[Hub] Cleaned up %d stale rooms", cleaned)
	}
	return cleaned
}
