package relay

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type OnlineServer struct {
	DeviceID    string
	UserID      string
	Conn        *websocket.Conn
	OnlineSince time.Time
	send        chan []byte
	done        chan struct{}
	once        sync.Once
}

type Session struct {
	ID         string
	UserID     string
	DeviceID   string
	ServerConn *OnlineServer
	ClientConn *Connection // reuse from Phase 1
	CreatedAt  time.Time
}

type GuestDevice struct {
	ShortID      string
	PasswordHash string // bcrypt hash
	UserID       string // owner (may be empty for anonymous)
	DeviceID     string // maps to OnlineServer.DeviceID
	DeviceName   string
	Server       *OnlineServer
	RegisteredAt time.Time
}

type DeviceHub struct {
	onlineServers map[string]*OnlineServer // deviceID -> server
	guestDevices  map[string]*GuestDevice  // shortID -> guest device
	sessions      map[string]*Session      // sessionID -> session
	rooms         map[string]*Room         // roomID -> room
	serverRooms   map[string]string        // deviceID -> roomID (1:1)
	mu            sync.RWMutex
}

func NewDeviceHub() *DeviceHub {
	return &DeviceHub{
		onlineServers: make(map[string]*OnlineServer),
		guestDevices:  make(map[string]*GuestDevice),
		sessions:      make(map[string]*Session),
		rooms:         make(map[string]*Room),
		serverRooms:   make(map[string]string),
	}
}

// --- Online Server Management ---

func (h *DeviceHub) AddServer(deviceID, userID string, conn *websocket.Conn) *OnlineServer {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Close existing connection for same device
	if old, ok := h.onlineServers[deviceID]; ok {
		old.Close()
	}

	server := &OnlineServer{
		DeviceID:    deviceID,
		UserID:      userID,
		Conn:        conn,
		OnlineSince: time.Now(),
		send:        make(chan []byte, 64),
		done:        make(chan struct{}),
	}
	h.onlineServers[deviceID] = server

	log.Printf("[DeviceHub] Server %s online (user=%s)", deviceID, userID)
	return server
}

func (h *DeviceHub) RemoveServer(deviceID string) {
	h.mu.Lock()
	server, ok := h.onlineServers[deviceID]
	if ok {
		delete(h.onlineServers, deviceID)
	}

	// Close any active sessions for this device
	for id, sess := range h.sessions {
		if sess.DeviceID == deviceID {
			if sess.ClientConn != nil {
				msg, _ := json.Marshal(map[string]string{"type": "peer_disconnected"})
				sess.ClientConn.Send(Message{Type: websocket.TextMessage, Data: msg})
				sess.ClientConn.Close()
			}
			delete(h.sessions, id)
			log.Printf("[DeviceHub] Session %s closed (server offline)", id)
		}
	}

	// Cleanup guest devices linked to this server
	for id, guest := range h.guestDevices {
		if guest.DeviceID == deviceID {
			delete(h.guestDevices, id)
			log.Printf("[DeviceHub] Guest device %s removed (server offline)", id)
		}
	}

	// Cleanup room owned by this server — kick all clients
	if roomID, ok := h.serverRooms[deviceID]; ok {
		if room, exists := h.rooms[roomID]; exists {
			room.CloseAllClients("server disconnected")
			delete(h.rooms, roomID)
			log.Printf("[DeviceHub] Room %s destroyed (server offline)", roomID)
		}
		delete(h.serverRooms, deviceID)
	}
	h.mu.Unlock()

	if server != nil {
		server.Close()
		log.Printf("[DeviceHub] Server %s offline", deviceID)
	}
}

func (h *DeviceHub) IsOnline(deviceID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.onlineServers[deviceID]
	return ok
}

func (h *DeviceHub) GetServer(deviceID string) *OnlineServer {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.onlineServers[deviceID]
}

// --- Session Management ---

func (h *DeviceHub) CreateSession(userID, deviceID string) (*Session, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	server, ok := h.onlineServers[deviceID]
	if !ok {
		return nil, ErrServerOffline
	}
	if server.UserID != userID {
		return nil, ErrNotOwner
	}

	// Check if device already has active session
	for _, sess := range h.sessions {
		if sess.DeviceID == deviceID {
			return nil, ErrServerBusy
		}
	}

	sess := &Session{
		ID:         uuid.New().String(),
		UserID:     userID,
		DeviceID:   deviceID,
		ServerConn: server,
		CreatedAt:  time.Now(),
	}
	h.sessions[sess.ID] = sess

	log.Printf("[DeviceHub] Session %s created for device %s", sess.ID, deviceID)
	return sess, nil
}

func (h *DeviceHub) GetSession(sessionID string) *Session {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.sessions[sessionID]
}

func (h *DeviceHub) SetSessionClient(sessionID string, conn *Connection) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	sess, ok := h.sessions[sessionID]
	if !ok {
		return false
	}
	sess.ClientConn = conn
	return true
}

func (h *DeviceHub) RemoveSession(sessionID string) {
	h.mu.Lock()
	sess, ok := h.sessions[sessionID]
	if ok {
		delete(h.sessions, sessionID)
	}
	h.mu.Unlock()

	if sess != nil {
		log.Printf("[DeviceHub] Session %s removed", sessionID)
	}
}

// ForwardToClient forwards a message from server to the client in a session.
func (h *DeviceHub) ForwardToClient(sessionID string, msgType int, data []byte) bool {
	h.mu.RLock()
	sess, ok := h.sessions[sessionID]
	if !ok || sess.ClientConn == nil {
		h.mu.RUnlock()
		return false
	}
	client := sess.ClientConn
	h.mu.RUnlock()

	return client.Send(Message{Type: msgType, Data: data})
}

// ForwardToServer forwards a message from client to the server in a session.
func (h *DeviceHub) ForwardToServer(sessionID string, msgType int, data []byte) bool {
	h.mu.RLock()
	sess, ok := h.sessions[sessionID]
	if !ok || sess.ServerConn == nil {
		h.mu.RUnlock()
		return false
	}
	server := sess.ServerConn
	h.mu.RUnlock()

	return server.SendMessage(msgType, data)
}

// Stats returns online servers and active sessions count.
func (h *DeviceHub) Stats() (servers int, sessions int) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.onlineServers), len(h.sessions)
}

// CleanupStaleSessions removes sessions older than maxAge.
func (h *DeviceHub) CleanupStaleSessions(maxAge time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	for id, sess := range h.sessions {
		if now.Sub(sess.CreatedAt) > maxAge && sess.ClientConn == nil {
			delete(h.sessions, id)
			log.Printf("[DeviceHub] Cleaned stale session %s", id)
		}
	}
}

// --- OnlineServer methods ---

func (s *OnlineServer) SendMessage(msgType int, data []byte) bool {
	select {
	case <-s.done:
		return false
	default:
	}

	s.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	err := s.Conn.WriteMessage(msgType, data)
	return err == nil
}

func (s *OnlineServer) SendJSON(v interface{}) bool {
	data, err := json.Marshal(v)
	if err != nil {
		return false
	}
	return s.SendMessage(websocket.TextMessage, data)
}

func (s *OnlineServer) Close() {
	s.once.Do(func() {
		close(s.done)
		s.Conn.Close()
	})
}

func (s *OnlineServer) Done() <-chan struct{} {
	return s.done
}

// --- Guest Device Management ---

func (h *DeviceHub) RegisterGuestDevice(shortID, passwordHash, userID, deviceID, deviceName string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.guestDevices[shortID]; exists {
		return ErrShortIDConflict
	}

	server, ok := h.onlineServers[deviceID]
	if !ok {
		return ErrServerOffline
	}

	h.guestDevices[shortID] = &GuestDevice{
		ShortID:      shortID,
		PasswordHash: passwordHash,
		UserID:       userID,
		DeviceID:     deviceID,
		DeviceName:   deviceName,
		Server:       server,
		RegisteredAt: time.Now(),
	}

	log.Printf("[DeviceHub] Guest device registered: %s (device=%s)", shortID, deviceID)
	return nil
}

func (h *DeviceHub) UnregisterGuestDevice(shortID string) {
	h.mu.Lock()
	delete(h.guestDevices, shortID)
	h.mu.Unlock()
}

func (h *DeviceHub) GetGuestDevice(shortID string) *GuestDevice {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.guestDevices[shortID]
}

func (h *DeviceHub) UpdateGuestPassword(shortID, newPasswordHash string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	guest, ok := h.guestDevices[shortID]
	if !ok {
		return false
	}
	guest.PasswordHash = newPasswordHash
	return true
}

func (h *DeviceHub) CreateGuestSession(shortID string) (*Session, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	guest, ok := h.guestDevices[shortID]
	if !ok {
		return nil, ErrGuestNotFound
	}
	if guest.Server == nil {
		return nil, ErrServerOffline
	}

	// Check if server already busy
	for _, sess := range h.sessions {
		if sess.DeviceID == guest.DeviceID {
			return nil, ErrServerBusy
		}
	}

	sess := &Session{
		ID:         uuid.New().String(),
		UserID:     "guest",
		DeviceID:   guest.DeviceID,
		ServerConn: guest.Server,
		CreatedAt:  time.Now(),
	}
	h.sessions[sess.ID] = sess

	log.Printf("[DeviceHub] Guest session %s created for device %s (shortID=%s)", sess.ID, guest.DeviceID, shortID)
	return sess, nil
}

// --- Room Management ---

func (h *DeviceHub) CreateRoom(deviceID, ownerUserID string) (*Room, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	server, ok := h.onlineServers[deviceID]
	if !ok {
		return nil, ErrServerOffline
	}

	// One room per server
	if _, exists := h.serverRooms[deviceID]; exists {
		return nil, ErrRoomAlreadyExists
	}

	// Generate unique room ID (retry up to 10 times)
	var roomID string
	for i := 0; i < 10; i++ {
		candidate := GenerateRoomID()
		if _, exists := h.rooms[candidate]; !exists {
			roomID = candidate
			break
		}
	}
	if roomID == "" {
		return nil, ErrShortIDConflict
	}

	room := NewRoom(roomID, deviceID, ownerUserID, server)
	h.rooms[roomID] = room
	h.serverRooms[deviceID] = roomID

	log.Printf("[DeviceHub] Room %s created for device %s (owner=%s)", roomID, deviceID, ownerUserID)
	return room, nil
}

func (h *DeviceHub) GetRoom(roomID string) *Room {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.rooms[roomID]
}

func (h *DeviceHub) GetRoomByDevice(deviceID string) *Room {
	h.mu.RLock()
	roomID, ok := h.serverRooms[deviceID]
	if !ok {
		h.mu.RUnlock()
		return nil
	}
	room := h.rooms[roomID]
	h.mu.RUnlock()
	return room
}

func (h *DeviceHub) RemoveRoom(roomID string) {
	h.mu.Lock()
	room, ok := h.rooms[roomID]
	if ok {
		delete(h.rooms, roomID)
		delete(h.serverRooms, room.DeviceID)
	}
	h.mu.Unlock()

	if room != nil {
		room.CloseAllClients("room destroyed")
		log.Printf("[DeviceHub] Room %s removed", roomID)
	}
}

func (h *DeviceHub) RoomCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.rooms)
}
