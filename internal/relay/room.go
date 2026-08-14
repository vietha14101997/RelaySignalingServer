package relay

import (
	"crypto/rand"
	"encoding/json"
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
	RoomStreaming   RoomState = "streaming"
)

type ClientRole string

const (
	RoleHost   ClientRole = "host"
	RoleViewer ClientRole = "viewer"
)

const MaxClientsPerRoom = 3

const defaultRoomAdmissionTTL = 30 * time.Second

type RoomClient struct {
	ID           string
	UserID       string // "" = guest
	Role         ClientRole
	Conn         *Connection
	InputAllowed bool // host can toggle per-viewer
	JoinedAt     time.Time
}

type roomAdmission struct {
	client    RoomClient
	expiresAt time.Time
}

type Room struct {
	ID           string
	DeviceID     string // server device owning this room
	OwnerUserID  string // "" = guest room
	PasswordHash string // bcrypt
	State        RoomState
	ServerConn   *OnlineServer
	Clients      map[string]*RoomClient // connected clientID -> client
	CreatedAt    time.Time
	admissions   map[string]*roomAdmission
	admissionTTL time.Duration
	closed       bool
	mu           sync.RWMutex
	serverMu     sync.RWMutex
	serverGen    uint64
}

func NewRoom(id, deviceID, ownerUserID string, serverConn *OnlineServer) *Room {
	r := &Room{
		ID:           id,
		DeviceID:     deviceID,
		OwnerUserID:  ownerUserID,
		State:        RoomIdle,
		ServerConn:   serverConn,
		Clients:      make(map[string]*RoomClient),
		admissions:   make(map[string]*roomAdmission),
		admissionTTL: defaultRoomAdmissionTTL,
		CreatedAt:    time.Now(),
	}
	if serverConn != nil {
		r.serverGen = serverConn.Generation
	}
	return r
}

// DisplayID returns "XXX-XXX" format
func (r *Room) DisplayID() string {
	if len(r.ID) == 6 {
		return fmt.Sprintf("%s-%s", r.ID[:3], r.ID[3:])
	}
	return r.ID
}

func (r *Room) ClientCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneExpiredAdmissionsLocked(time.Now())
	return len(r.admissions)
}

func (r *Room) IsFull() bool {
	return r.ClientCount() >= MaxClientsPerRoom
}

// ReserveClient records the identity and role established by /rooms/join.
// A websocket may only attach to an unexpired admission created here.
func (r *Room) ReserveClient(client *RoomClient) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRoomNotFound
	}
	now := time.Now()
	r.pruneExpiredAdmissionsLocked(now)
	if _, exists := r.admissions[client.ID]; exists {
		return ErrClientNotAdmitted
	}
	if len(r.admissions) >= MaxClientsPerRoom {
		return ErrRoomFull
	}

	if client.Role == "" {
		client.Role = r.determineRoleLocked(client.UserID)
	}
	if client.Role == RoleHost {
		client.InputAllowed = true
	}
	if client.JoinedAt.IsZero() {
		client.JoinedAt = now
	}
	client.Conn = nil
	r.admissions[client.ID] = &roomAdmission{
		client:    *client,
		expiresAt: now.Add(r.admissionTTL),
	}
	count := len(r.admissions)

	log.Printf("[Room %s] Client %s admitted as %s (total: %d)", r.ID, client.ID, client.Role, count)
	return nil
}

// AttachClient binds a websocket only to metadata previously established by
// ReserveClient. Reconnect input cannot overwrite authoritative admission data.
func (r *Room) AttachClient(clientID string, conn *Connection) (*RoomClient, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrRoomNotFound
	}
	now := time.Now()
	r.pruneExpiredAdmissionsLocked(now)
	admission := r.admissions[clientID]
	if admission == nil {
		r.mu.Unlock()
		return nil, ErrClientNotAdmitted
	}
	oldConn := admission.client.Conn
	admission.client.Conn = conn
	admission.expiresAt = time.Time{}
	r.Clients[clientID] = &admission.client
	client := &admission.client
	count := len(r.admissions)
	r.mu.Unlock()

	if oldConn != nil && oldConn != conn {
		oldConn.Close()
	}
	log.Printf("[Room %s] Client %s attached as %s (total: %d)", r.ID, clientID, client.Role, count)
	return client, nil
}

// RemoveClient detaches only the connection that owns this read pump. The
// admission metadata remains available for a bounded reconnect window.
func (r *Room) RemoveClient(clientID string, expectedConn *Connection) bool {
	r.mu.Lock()
	client, ok := r.Clients[clientID]
	if ok && client.Conn == expectedConn {
		delete(r.Clients, clientID)
		client.Conn = nil
		if admission := r.admissions[clientID]; admission != nil {
			admission.expiresAt = time.Now().Add(r.admissionTTL)
		}
	} else {
		ok = false
	}
	remaining := len(r.admissions)
	r.mu.Unlock()

	if ok {
		log.Printf("[Room %s] Client %s left (remaining: %d)", r.ID, clientID, remaining)
		expectedConn.Close()
	}
	return ok
}

// HasValidAdmission checks whether clientID can currently attach. AttachClient
// repeats this check after websocket upgrade to close the expiration race.
func (r *Room) HasValidAdmission(clientID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneExpiredAdmissionsLocked(time.Now())
	_, ok := r.admissions[clientID]
	return ok && !r.closed
}

// PruneExpiredAdmissions removes abandoned joins and expired reconnect slots.
func (r *Room) PruneExpiredAdmissions(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pruneExpiredAdmissionsLocked(now)
}

func (r *Room) pruneExpiredAdmissionsLocked(now time.Time) int {
	removed := 0
	for id, admission := range r.admissions {
		if admission.client.Conn == nil && !admission.expiresAt.After(now) {
			delete(r.admissions, id)
			delete(r.Clients, id)
			removed++
		}
	}
	return removed
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

// WithReadLock runs fn while holding the room's read lock. Lets callers in
// other packages (e.g. internal/diagnose) take a consistent multi-field
// snapshot (State + Clients together) without exposing the mutex itself.
func (r *Room) WithReadLock(fn func()) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn()
}

// DetermineRole assigns host to first joiner in guest room, or to account owner in user room.
func (r *Room) DetermineRole(userID string) ClientRole {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.determineRoleLocked(userID)
}

func (r *Room) determineRoleLocked(userID string) ClientRole {
	// User room: owner account = host
	if r.OwnerUserID != "" && userID == r.OwnerUserID {
		return RoleHost
	}

	// First client to join any room = host (regardless of account)
	if len(r.admissions) == 0 {
		return RoleHost
	}

	return RoleViewer
}

// RebindServer changes the room's routing target in generation order. The
// dedicated server lock keeps Room.mu out of the potentially 10s socket write.
func (r *Room) RebindServer(server *OnlineServer) bool {
	r.serverMu.Lock()
	defer r.serverMu.Unlock()
	r.mu.RLock()
	closed := r.closed
	r.mu.RUnlock()
	if closed || server == nil || server.Generation <= r.serverGen {
		return false
	}
	r.ServerConn = server
	r.serverGen = server.Generation
	return true
}

func (r *Room) markClosed() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
}

// SendToServer snapshots the current route without holding a room lock during
// network I/O. A failed write is retried once if a newer generation appeared.
func (r *Room) SendToServer(msgType int, data []byte) bool {
	r.mu.RLock()
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return false
	}
	server, generation := r.serverSnapshot()
	if server == nil {
		return false
	}
	if server.SendMessage(msgType, data) {
		return true
	}
	replacement, replacementGeneration := r.serverSnapshot()
	return replacement != nil && replacementGeneration > generation && replacement.SendMessage(msgType, data)
}

func (r *Room) serverSnapshot() (*OnlineServer, uint64) {
	r.serverMu.RLock()
	defer r.serverMu.RUnlock()
	return r.ServerConn, r.serverGen
}

func (r *Room) SendJSONToServer(v interface{}) bool {
	data, err := json.Marshal(v)
	if err != nil {
		return false
	}
	return r.SendToServer(websocket.TextMessage, data)
}

// BroadcastToClients sends a message to all connected clients.
func (r *Room) BroadcastToClients(msg Message) {
	r.mu.RLock()
	connections := make([]*Connection, 0, len(r.Clients))
	for _, client := range r.Clients {
		if client.Conn != nil {
			connections = append(connections, client.Conn)
		}
	}
	r.mu.RUnlock()
	for _, conn := range connections {
		conn.Send(msg)
	}
}

// SendToClient sends a message to a specific client.
func (r *Room) SendToClient(clientID string, msg Message) bool {
	r.mu.RLock()
	client, ok := r.Clients[clientID]
	var conn *Connection
	if ok {
		conn = client.Conn
	}
	r.mu.RUnlock()

	if conn == nil {
		return false
	}
	return conn.Send(msg)
}

// NotifyAllClients sends a JSON text message to all clients.
func (r *Room) NotifyAllClients(data []byte) {
	r.BroadcastToClients(Message{Type: websocket.TextMessage, Data: data})
}

// CloseAllClients disconnects all clients. It returns false if any connection
// had already committed to closing and could not accept room_closed.
func (r *Room) CloseAllClients(reason string) bool {
	r.mu.Lock()
	r.closed = true
	clients := make([]*RoomClient, 0, len(r.Clients))
	for _, c := range r.Clients {
		clients = append(clients, c)
	}
	r.Clients = make(map[string]*RoomClient)
	r.admissions = make(map[string]*roomAdmission)
	r.mu.Unlock()

	allAccepted := true
	for _, c := range clients {
		if c.Conn != nil {
			msg := mustJSON(map[string]string{"type": "room_closed", "reason": reason})
			if !c.Conn.SendAndClose(Message{Type: websocket.TextMessage, Data: msg}) {
				allAccepted = false
			}
		}
	}
	log.Printf("[Room %s] All clients kicked: %s", r.ID, reason)
	return allAccepted
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
