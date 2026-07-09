package relay

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Phase 2 — Signaling Resilience: session grace-period handling.
//
// A transient WS drop (network blip, brief relay/host restart of the *socket*,
// not the process) on either the client side or the server side no longer
// destroys the session immediately. Instead the affected side is marked
// "stale" and a grace timer (DeviceHub.sessionGrace) starts. If the same
// owner reconnects within the grace window, the session is rebound and no
// peer_disconnected is emitted. If the timer expires first, the existing
// (pre-Phase-2) teardown behavior runs: peer_disconnected + session removal.
//
// All Session field mutations here happen under DeviceHub.mu — callers of the
// exported entry points (MarkClientStale, AddServer, RemoveServer,
// RemoveServerConn) must NOT hold the lock themselves.

const (
	defaultSessionGrace     = 30 * time.Second
	defaultMaxStaleSessions = 100

	// sessionIDBytes is the raw entropy size for generated session IDs.
	// 32 bytes = 256 bits, comfortably over the >=128-bit CSPRNG requirement
	// (F12) for a resumable, longer-lived session identifier.
	sessionIDBytes = 32
)

// NewDeviceHubWithGrace builds a DeviceHub with an explicit grace period and
// stale-session cap (see config.Config.SessionGrace / MaxStaleSessions).
// NewDeviceHub() delegates here with the package defaults.
func NewDeviceHubWithGrace(grace time.Duration, maxStaleSessions int) *DeviceHub {
	if grace <= 0 {
		grace = defaultSessionGrace
	}
	if maxStaleSessions <= 0 {
		maxStaleSessions = defaultMaxStaleSessions
	}
	return &DeviceHub{
		onlineServers:    make(map[string]*OnlineServer),
		guestDevices:     make(map[string]*GuestDevice),
		sessions:         make(map[string]*Session),
		rooms:            make(map[string]*Room),
		serverRooms:      make(map[string]string),
		staleSessions:    make(map[string]time.Time),
		sessionGrace:     grace,
		maxStaleSessions: maxStaleSessions,
	}
}

// generateSessionID returns a CSPRNG, URL-safe, opaque session identifier
// (F12: session ids are resumable now, so entropy matters more than before).
func generateSessionID() string {
	buf := make([]byte, sessionIDBytes)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failing means the entropy source itself is broken — an
		// unrecoverable environment fault. Fall back to uuid (still
		// crypto/rand-backed under the hood) rather than crash session
		// creation entirely.
		log.Printf("[DeviceHub] crypto/rand unavailable, falling back to uuid: %v", err)
		return uuid.New().String()
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

// --- Client-side grace (F2) ---

// MarkClientStale marks sessionID's client side as stale and starts (or
// restarts) its grace timer. Called from the client WS read pump's defer
// instead of immediately removing the session. Returns false if the session
// no longer exists.
func (h *DeviceHub) MarkClientStale(sessionID string) bool {
	h.mu.Lock()
	sess, ok := h.sessions[sessionID]
	if !ok {
		h.mu.Unlock()
		return false
	}

	sess.clientStale = true
	sess.ClientConn = nil
	sess.clientGraceGen++
	gen := sess.clientGraceGen
	if sess.clientGraceTimer != nil {
		sess.clientGraceTimer.Stop()
	}
	sess.clientGraceTimer = time.AfterFunc(h.sessionGrace, func() {
		h.expireClientGrace(sessionID, gen)
	})
	victim := h.markStaleLocked(sess)
	h.mu.Unlock()

	if victim != nil && victim.ID != sessionID {
		h.teardownSession(victim, "stale session cap exceeded")
	}
	return true
}

// cancelClientGraceLocked cancels sess's client grace timer (if any) and
// clears the stale flag. Must be called with h.mu held.
func (h *DeviceHub) cancelClientGraceLocked(sess *Session) {
	if sess.clientGraceTimer != nil {
		sess.clientGraceTimer.Stop()
		sess.clientGraceTimer = nil
	}
	sess.clientStale = false
	h.unmarkStaleIfDoneLocked(sess)
}

// expireClientGrace runs when a client grace timer fires. gen guards against
// a timer that fired concurrently with (or after) a cancel/rebind — if the
// session's current generation no longer matches, a rebind already happened
// and this is a stale callback that must do nothing.
func (h *DeviceHub) expireClientGrace(sessionID string, gen uint64) {
	h.mu.Lock()
	sess, ok := h.sessions[sessionID]
	if !ok || !sess.clientStale || sess.clientGraceGen != gen {
		h.mu.Unlock()
		return
	}
	delete(h.sessions, sessionID)
	delete(h.staleSessions, sessionID)
	h.mu.Unlock()

	h.teardownSession(sess, "client grace expired")
}

// --- Server-side grace (F3) ---

// markServerStaleLocked marks sess as server-stale and (re)starts its grace
// timer. Must be called with h.mu held. Returns a session that must be torn
// down (outside the lock) if marking sess stale pushed the stale-session
// count over the cap, or nil.
func (h *DeviceHub) markServerStaleLocked(sess *Session) *Session {
	sess.serverStale = true
	sess.ServerConn = nil
	sess.serverGraceGen++
	gen := sess.serverGraceGen
	if sess.serverGraceTimer != nil {
		sess.serverGraceTimer.Stop()
	}
	sessionID := sess.ID
	sess.serverGraceTimer = time.AfterFunc(h.sessionGrace, func() {
		h.expireServerGrace(sessionID, gen)
	})
	return h.markStaleLocked(sess)
}

// cancelServerGraceLocked cancels sess's server grace timer (if any) and
// clears the stale flag. Must be called with h.mu held.
func (h *DeviceHub) cancelServerGraceLocked(sess *Session) {
	if sess.serverGraceTimer != nil {
		sess.serverGraceTimer.Stop()
		sess.serverGraceTimer = nil
	}
	sess.serverStale = false
	h.unmarkStaleIfDoneLocked(sess)
}

// expireServerGrace runs when a server grace timer fires; see expireClientGrace
// for the generation-guard rationale.
func (h *DeviceHub) expireServerGrace(sessionID string, gen uint64) {
	h.mu.Lock()
	sess, ok := h.sessions[sessionID]
	if !ok || !sess.serverStale || sess.serverGraceGen != gen {
		h.mu.Unlock()
		return
	}
	delete(h.sessions, sessionID)
	delete(h.staleSessions, sessionID)
	h.mu.Unlock()

	h.teardownSession(sess, "server grace expired")
}

// markDeviceSessionsStaleLocked marks every session for deviceID as
// server-stale and performs the (unchanged, immediate) guest-device + room
// cleanup for that device. Grace is scoped to session lifetime only — per
// phase spec, guest-device registration and rooms are NOT given a grace
// period. Must be called with h.mu held. Returns sessions evicted due to the
// stale-session cap being exceeded (to be torn down outside the lock).
func (h *DeviceHub) markDeviceSessionsStaleLocked(deviceID string) []*Session {
	var evicted []*Session
	for _, sess := range h.sessions {
		if sess.DeviceID == deviceID {
			if v := h.markServerStaleLocked(sess); v != nil {
				evicted = append(evicted, v)
			}
		}
	}
	h.cleanupDeviceGuestsAndRoomsLocked(deviceID)
	return evicted
}

// removeDeviceSessionsLocked immediately removes every session for deviceID
// (no grace) and performs the same guest-device + room cleanup. Used by the
// destructive RemoveServer path (R2: a deleted device must not be resumable).
// Returns the removed sessions so the caller can send peer_disconnected
// OUTSIDE the lock. Must be called with h.mu held.
func (h *DeviceHub) removeDeviceSessionsLocked(deviceID string) []*Session {
	var removed []*Session
	for id, sess := range h.sessions {
		if sess.DeviceID == deviceID {
			h.clearGraceLocked(sess)
			delete(h.sessions, id)
			delete(h.staleSessions, id)
			removed = append(removed, sess)
		}
	}
	h.cleanupDeviceGuestsAndRoomsLocked(deviceID)
	return removed
}

// cleanupDeviceGuestsAndRoomsLocked removes guest-device registrations and the
// room owned by deviceID. Guest/room state is never grace-delayed (per phase
// spec, grace is scoped to session lifetime only). Must be called with h.mu held.
func (h *DeviceHub) cleanupDeviceGuestsAndRoomsLocked(deviceID string) {
	for id, guest := range h.guestDevices {
		if guest.DeviceID == deviceID {
			delete(h.guestDevices, id)
			log.Printf("[DeviceHub] Guest device %s removed (server offline)", id)
		}
	}

	if roomID, ok := h.serverRooms[deviceID]; ok {
		if room, exists := h.rooms[roomID]; exists {
			room.CloseAllClients("server disconnected")
			delete(h.rooms, roomID)
			log.Printf("[DeviceHub] Room %s destroyed (server offline)", roomID)
		}
		delete(h.serverRooms, deviceID)
	}
}

// notifySessionResumed re-announces room_ready to both sides of a session that
// just rebound its server connection (F3/step 6 symmetric resume). Must be
// called WITHOUT h.mu held (performs network I/O). client is a snapshot of
// sess.ClientConn captured by the caller UNDER the lock (R1): reading
// sess.ClientConn here directly would race a concurrent MarkClientStale nil-ing
// it → check-then-use nil-deref panic in the both-ends-reconnect case.
// (Connection.Send on an already-closed conn safely returns false, so a
// captured-then-closed pointer is harmless.)
func (h *DeviceHub) notifySessionResumed(sess *Session, server *OnlineServer, client *Connection) {
	payload := map[string]string{"type": "room_ready", "session_id": sess.ID}
	server.SendJSON(payload)
	if client != nil {
		data, _ := json.Marshal(payload)
		client.Send(Message{Type: websocket.TextMessage, Data: data})
	}
	log.Printf("[DeviceHub] Session %s resumed (server reconnected)", sess.ID)
}

// --- Shared stale-set bookkeeping + cap enforcement ---

// markStaleLocked records sess as stale (if not already tracked) and, if the
// stale-session cap is now exceeded, picks the single oldest stale session
// across the whole hub and evicts it (tears down grace timers + removes it
// from the sessions map). Must be called with h.mu held. The returned victim
// (if non-nil) still needs its network-side teardown (peer_disconnected)
// performed by the caller OUTSIDE the lock.
func (h *DeviceHub) markStaleLocked(sess *Session) *Session {
	if _, tracked := h.staleSessions[sess.ID]; !tracked {
		h.staleSessions[sess.ID] = time.Now()
	}
	if len(h.staleSessions) <= h.maxStaleSessions {
		return nil
	}

	var oldestID string
	var oldestAt time.Time
	for id, at := range h.staleSessions {
		if oldestID == "" || at.Before(oldestAt) {
			oldestID, oldestAt = id, at
		}
	}

	victim, ok := h.sessions[oldestID]
	delete(h.staleSessions, oldestID)
	if !ok {
		return nil
	}
	h.clearGraceLocked(victim)
	delete(h.sessions, oldestID)
	log.Printf("[DeviceHub] Session %s evicted (stale-session cap %d exceeded)", oldestID, h.maxStaleSessions)
	return victim
}

// unmarkStaleIfDoneLocked drops sess from the stale-tracking set once neither
// side is stale anymore. Must be called with h.mu held.
func (h *DeviceHub) unmarkStaleIfDoneLocked(sess *Session) {
	if !sess.clientStale && !sess.serverStale {
		delete(h.staleSessions, sess.ID)
	}
}

// clearGraceLocked stops both grace timers on sess and clears both stale
// flags, without deleting it from any map. Must be called with h.mu held.
func (h *DeviceHub) clearGraceLocked(sess *Session) {
	if sess.clientGraceTimer != nil {
		sess.clientGraceTimer.Stop()
		sess.clientGraceTimer = nil
	}
	if sess.serverGraceTimer != nil {
		sess.serverGraceTimer.Stop()
		sess.serverGraceTimer = nil
	}
	sess.clientStale = false
	sess.serverStale = false
}

// teardownSession performs the (pre-Phase-2-equivalent) session teardown:
// notify whichever side is still connected that its peer disconnected. Must
// be called WITHOUT h.mu held (network I/O). sess must already be removed
// from DeviceHub.sessions by the caller.
//
// Deliberately does NOT call ClientConn.Close() here: Connection.Close()
// closes the raw socket immediately, which races with WritePump flushing the
// just-queued Send() (Go's select has no priority between a newly-ready send
// and an already-closed done channel), and can silently drop this very
// notification. The client is expected to disconnect on its own upon
// receiving peer_disconnected; if it doesn't, its read pump's existing 90s
// ping/pong deadline reaps the now-orphaned connection.
func (h *DeviceHub) teardownSession(sess *Session, reason string) {
	if sess.ServerConn != nil {
		sess.ServerConn.SendJSON(map[string]string{"type": "peer_disconnected"})
	}
	if sess.ClientConn != nil {
		data, _ := json.Marshal(map[string]string{"type": "peer_disconnected"})
		sess.ClientConn.Send(Message{Type: websocket.TextMessage, Data: data})
	}
	log.Printf("[DeviceHub] Session %s torn down (%s)", sess.ID, reason)
}
