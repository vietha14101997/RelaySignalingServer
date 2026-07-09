package relay

import (
	"encoding/json"
	"log"
	"sync"
	"time"

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
	ID     string
	UserID string
	// OwnerUserID is the JWT user that owns the underlying device (host). For
	// authenticated sessions this equals UserID; for guest sessions UserID is
	// the "guest" sentinel while OwnerUserID is the real host owner. Used to
	// authorize server-side resume/rebind (F12b) independent of the
	// session's own (possibly anonymous) UserID.
	OwnerUserID string
	DeviceID    string
	ServerConn  *OnlineServer
	ClientConn  *Connection // reuse from Phase 1
	CreatedAt   time.Time

	// --- Grace-period state (Phase 2 signaling resilience) ---
	// All fields below are guarded by DeviceHub.mu; see session_grace.go.
	clientStale      bool
	clientGraceGen   uint64
	clientGraceTimer *time.Timer
	serverStale      bool
	serverGraceGen   uint64
	serverGraceTimer *time.Timer
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

	// Phase 2 signaling resilience (see session_grace.go).
	staleSessions    map[string]time.Time // sessionID -> became-stale-at, for cap eviction
	sessionGrace     time.Duration        // grace window before a stale session tears down
	maxStaleSessions int                  // concurrent stale-session cap (F12d)
}

// NewDeviceHub builds a DeviceHub using the package default grace period
// (30s) and stale-session cap (100). Use NewDeviceHubWithGrace to wire these
// from config.Config instead.
func NewDeviceHub() *DeviceHub {
	return NewDeviceHubWithGrace(defaultSessionGrace, defaultMaxStaleSessions)
}

// --- Online Server Management ---

// AddServer registers deviceID's presence connection for userID.
//
// Phase 2 resume: if deviceID has a server-stale session (host WS dropped,
// still within its grace window), the reconnecting userID MUST match that
// session's OwnerUserID (F12b) — otherwise the connection is rejected with
// ErrNotOwner rather than silently taking over. On a successful match, the
// stale session(s) for deviceID are rebound to the new connection instead of
// being torn down, and room_ready is re-announced to both sides.
func (h *DeviceHub) AddServer(deviceID, userID string, conn *websocket.Conn) (*OnlineServer, error) {
	h.mu.Lock()

	for _, sess := range h.sessions {
		if sess.DeviceID == deviceID && sess.serverStale && sess.OwnerUserID != userID {
			h.mu.Unlock()
			return nil, ErrNotOwner
		}
	}

	// Close existing connection for same device (e.g. overlapping reconnect)
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

	// Snapshot ClientConn under the lock (R1): notifySessionResumed does network
	// I/O outside the lock, and a concurrent MarkClientStale can nil ClientConn.
	type resumeItem struct {
		sess   *Session
		client *Connection
	}
	var resumed []resumeItem
	for _, sess := range h.sessions {
		if sess.DeviceID == deviceID && sess.serverStale {
			sess.ServerConn = server
			h.cancelServerGraceLocked(sess)
			resumed = append(resumed, resumeItem{sess: sess, client: sess.ClientConn})
		}
	}
	h.mu.Unlock()

	log.Printf("[DeviceHub] Server %s online (user=%s)", deviceID, userID)
	for _, r := range resumed {
		h.notifySessionResumed(r.sess, server, r.client)
	}
	return server, nil
}

// RemoveServer forcibly takes deviceID offline regardless of which connection
// is currently registered. Used for explicit, destructive admin actions (e.g.
// device deletion) — so sessions for deviceID are torn down IMMEDIATELY
// (peer_disconnected now, no grace). A deleted device must not be resumable
// (R2). Transient WS drops use RemoveServerConn, which DOES grant grace.
func (h *DeviceHub) RemoveServer(deviceID string) {
	h.mu.Lock()
	server := h.onlineServers[deviceID]
	delete(h.onlineServers, deviceID)
	removed := h.removeDeviceSessionsLocked(deviceID)
	h.mu.Unlock()

	if server != nil {
		server.Close()
		log.Printf("[DeviceHub] Server %s removed, sessions torn down immediately", deviceID)
	}
	for _, sess := range removed {
		h.teardownSession(sess, "device removed")
	}
}

// RemoveServerConn is the race-safe counterpart to RemoveServer: it only acts
// if conn is still the currently-registered connection for its device. This
// guards against a WS read pump's defer firing after a faster reconnect has
// already replaced conn with a new OnlineServer — in that case the newer
// connection must not be disturbed.
func (h *DeviceHub) RemoveServerConn(conn *OnlineServer) {
	h.mu.Lock()
	if h.onlineServers[conn.DeviceID] != conn {
		h.mu.Unlock()
		return
	}
	delete(h.onlineServers, conn.DeviceID)
	evicted := h.markDeviceSessionsStaleLocked(conn.DeviceID)
	h.mu.Unlock()

	conn.Close()
	log.Printf("[DeviceHub] Server %s offline, sessions entering grace", conn.DeviceID)
	for _, sess := range evicted {
		h.teardownSession(sess, "stale session cap exceeded")
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
		ID:          generateSessionID(),
		UserID:      userID,
		OwnerUserID: userID,
		DeviceID:    deviceID,
		ServerConn:  server,
		CreatedAt:   time.Now(),
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

// SetSessionClient binds conn as the session's client side. Also cancels any
// pending client-grace timer (F2 resume) — a no-op when the session was not
// stale, so this is safe to call on both a fresh connect and a resume.
//
// Returns the session's current ServerConn snapshot (captured under the lock)
// so callers can notify the server side WITHOUT a separate unsynchronized read
// of sess.ServerConn (R3): that field is written under h.mu during server
// resume and would otherwise race the join. serverConn may be nil (server side
// currently stale/absent). ok is false when the session no longer exists.
func (h *DeviceHub) SetSessionClient(sessionID string, conn *Connection) (serverConn *OnlineServer, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	sess, exists := h.sessions[sessionID]
	if !exists {
		return nil, false
	}
	sess.ClientConn = conn
	h.cancelClientGraceLocked(sess)
	return sess.ServerConn, true
}

func (h *DeviceHub) RemoveSession(sessionID string) {
	h.mu.Lock()
	sess, ok := h.sessions[sessionID]
	if ok {
		delete(h.sessions, sessionID)
		delete(h.staleSessions, sessionID)
		h.clearGraceLocked(sess)
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

// CleanupStaleSessions removes sessions older than maxAge that a client never
// joined (abandoned CreateSession API calls). Sessions currently in a
// client/server grace window (Phase 2) are skipped here — their own grace
// timer is the sole authority for tearing them down with a proper
// peer_disconnected notification; sweeping them here would delete them
// silently and race with that timer.
func (h *DeviceHub) CleanupStaleSessions(maxAge time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	for id, sess := range h.sessions {
		if now.Sub(sess.CreatedAt) > maxAge && sess.ClientConn == nil && !sess.clientStale && !sess.serverStale {
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
		ID:          generateSessionID(),
		UserID:      "guest",
		OwnerUserID: guest.UserID, // real host owner, for server-resume ownership checks
		DeviceID:    guest.DeviceID,
		ServerConn:  guest.Server,
		CreatedAt:   time.Now(),
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
