package relay

import (
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type RoomState string

const (
	RoomIdle        RoomState = "idle"
	RoomConfiguring RoomState = "configuring"
	RoomStreaming    RoomState = "streaming"
)

type ClientRole string

const (
	RoleHost   ClientRole = "host"
	RoleViewer ClientRole = "viewer"
)

const MaxClientsPerRoom = 3

type RoomClient struct {
	ID           string
	UserID       string // "" = guest
	Role         ClientRole
	Conn         *Connection
	InputAllowed bool // host can toggle per-viewer
	JoinedAt     time.Time
}

type Room struct {
	ID           string
	DeviceID     string // server device owning this room
	OwnerUserID  string // "" = guest room
	PasswordHash string // bcrypt
	State        RoomState
	ServerConn   *OnlineServer
	Clients      map[string]*RoomClient // clientID → client
	CreatedAt    time.Time
	mu           sync.RWMutex
}

func NewRoom(id, deviceID, ownerUserID string, serverConn *OnlineServer) *Room {
	return &Room{
		ID:          id,
		DeviceID:    deviceID,
		OwnerUserID: ownerUserID,
		State:       RoomIdle,
		ServerConn:  serverConn,
		Clients:     make(map[string]*RoomClient),
		CreatedAt:   time.Now(),
	}
}

// DisplayID returns "XXX-XXX" format
func (r *Room) DisplayID() string {
	if len(r.ID) == 6 {
		return fmt.Sprintf("%s-%s", r.ID[:3], r.ID[3:])
	}
	return r.ID
}

func (r *Room) ClientCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.Clients)
}

func (r *Room) IsFull() bool {
	return r.ClientCount() >= MaxClientsPerRoom
}

func (r *Room) AddClient(client *RoomClient) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.Clients) >= MaxClientsPerRoom {
		return ErrRoomFull
	}

	r.Clients[client.ID] = client
	log.Printf("[Room %s] Client %s joined as %s (total: %d)", r.ID, client.ID, client.Role, len(r.Clients))
	return nil
}

func (r *Room) RemoveClient(clientID string) {
	r.mu.Lock()
	client, ok := r.Clients[clientID]
	if ok {
		delete(r.Clients, clientID)
	}
	remaining := len(r.Clients)
	r.mu.Unlock()

	if ok {
		log.Printf("[Room %s] Client %s left (remaining: %d)", r.ID, clientID, remaining)
		if client.Conn != nil {
			client.Conn.Close()
		}
	}
}

func (r *Room) GetClient(clientID string) *RoomClient {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.Clients[clientID]
}

func (r *Room) SetState(state RoomState) {
	r.mu.Lock()
	r.State = state
	r.mu.Unlock()
	log.Printf("[Room %s] State → %s", r.ID, state)
}

func (r *Room) GetState() RoomState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.State
}

// DetermineRole assigns host to first joiner in guest room, or to account owner in user room.
func (r *Room) DetermineRole(userID string) ClientRole {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// User room: owner account = host
	if r.OwnerUserID != "" && userID == r.OwnerUserID {
		return RoleHost
	}

	// First client to join any room = host (regardless of account)
	if len(r.Clients) == 0 {
		return RoleHost
	}

	return RoleViewer
}

// BroadcastToClients sends a message to all connected clients.
func (r *Room) BroadcastToClients(msg Message) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, client := range r.Clients {
		if client.Conn != nil {
			client.Conn.Send(msg)
		}
	}
}

// SendToClient sends a message to a specific client.
func (r *Room) SendToClient(clientID string, msg Message) bool {
	r.mu.RLock()
	client, ok := r.Clients[clientID]
	r.mu.RUnlock()

	if !ok || client.Conn == nil {
		return false
	}
	return client.Conn.Send(msg)
}

// NotifyAllClients sends a JSON text message to all clients.
func (r *Room) NotifyAllClients(data []byte) {
	r.BroadcastToClients(Message{Type: websocket.TextMessage, Data: data})
}

// CloseAllClients disconnects all clients.
func (r *Room) CloseAllClients(reason string) {
	r.mu.Lock()
	clients := make([]*RoomClient, 0, len(r.Clients))
	for _, c := range r.Clients {
		clients = append(clients, c)
	}
	r.Clients = make(map[string]*RoomClient)
	r.mu.Unlock()

	for _, c := range clients {
		if c.Conn != nil {
			msg := mustJSON(map[string]string{"type": "room_closed", "reason": reason})
			c.Conn.Send(Message{Type: websocket.TextMessage, Data: msg})
			c.Conn.Close()
		}
	}
	log.Printf("[Room %s] All clients kicked: %s", r.ID, reason)
}

// --- Room ID Generation ---

const roomIDChars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
const roomIDLength = 6

func GenerateRoomID() string {
	b := make([]byte, roomIDLength)
	rand.Read(b)
	for i := range b {
		b[i] = roomIDChars[b[i]%byte(len(roomIDChars))]
	}
	return string(b)
}

func FormatRoomID(id string) string {
	if len(id) == 6 {
		return fmt.Sprintf("%s-%s", id[:3], id[3:])
	}
	return id
}
