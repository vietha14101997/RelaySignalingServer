package relay

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func dialWS(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	return ws
}

func wsEchoServer(t *testing.T) (*httptest.Server, *websocket.Conn) {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		for {
			mt, msg, err := ws.ReadMessage()
			if err != nil {
				return
			}
			ws.WriteMessage(mt, msg)
		}
	}))

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	ws := dialWS(t, wsURL)
	return ts, ws
}

func TestDeviceHubAddRemoveServer(t *testing.T) {
	hub := NewDeviceHub()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		// Keep alive
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	ws := dialWS(t, wsURL)
	defer ws.Close()

	if _, err := hub.AddServer("device-1", "user-1", ws); err != nil {
		t.Fatalf("AddServer failed: %v", err)
	}

	if !hub.IsOnline("device-1") {
		t.Error("device-1 should be online")
	}
	if hub.IsOnline("device-2") {
		t.Error("device-2 should not be online")
	}

	servers, sessions := hub.Stats()
	if servers != 1 || sessions != 0 {
		t.Errorf("Expected 1 server, 0 sessions; got %d, %d", servers, sessions)
	}

	hub.RemoveServer("device-1")

	if hub.IsOnline("device-1") {
		t.Error("device-1 should be offline after removal")
	}

	servers, sessions = hub.Stats()
	if servers != 0 {
		t.Errorf("Expected 0 servers after removal, got %d", servers)
	}
}

func TestCreateSession(t *testing.T) {
	hub := NewDeviceHub()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, _ := upgrader.Upgrade(w, r, nil)
		defer ws.Close()
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	ws := dialWS(t, wsURL)
	defer ws.Close()

	if _, err := hub.AddServer("device-1", "user-1", ws); err != nil {
		t.Fatalf("AddServer failed: %v", err)
	}

	// Create session
	sess, err := hub.CreateSession("user-1", "device-1")
	if err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}
	if sess.ID == "" {
		t.Error("Session ID should not be empty")
	}
	if sess.DeviceID != "device-1" {
		t.Errorf("Expected device-1, got %s", sess.DeviceID)
	}
	if sess.OwnerUserID != "user-1" {
		t.Errorf("Expected OwnerUserID=user-1, got %s", sess.OwnerUserID)
	}

	_, sessions := hub.Stats()
	if sessions != 1 {
		t.Errorf("Expected 1 session, got %d", sessions)
	}

	// Cannot create duplicate session
	_, err = hub.CreateSession("user-1", "device-1")
	if err != ErrServerBusy {
		t.Errorf("Expected ErrServerBusy, got %v", err)
	}
}

// TestSessionIDEntropy verifies session IDs meet the F12 >=128-bit CSPRNG
// requirement (generateSessionID uses 32 bytes = 256 bits, base64url-encoded).
func TestSessionIDEntropy(t *testing.T) {
	id := generateSessionID()
	raw, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		t.Fatalf("session id should be valid base64url: %v", err)
	}
	if bits := len(raw) * 8; bits < 128 {
		t.Errorf("session id entropy = %d bits, want >= 128", bits)
	}

	// IDs must be unique across calls (sanity check on the RNG being used).
	if id == generateSessionID() {
		t.Error("two generated session ids should not collide")
	}
}

func TestCreateSessionErrors(t *testing.T) {
	hub := NewDeviceHub()

	// Offline server
	_, err := hub.CreateSession("user-1", "device-nonexist")
	if err != ErrServerOffline {
		t.Errorf("Expected ErrServerOffline, got %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, _ := upgrader.Upgrade(w, r, nil)
		defer ws.Close()
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	ws := dialWS(t, wsURL)
	defer ws.Close()

	if _, err := hub.AddServer("device-1", "user-1", ws); err != nil {
		t.Fatalf("AddServer failed: %v", err)
	}

	// Wrong user
	_, err = hub.CreateSession("user-2", "device-1")
	if err != ErrNotOwner {
		t.Errorf("Expected ErrNotOwner, got %v", err)
	}
}

// wsConnPair creates a connected pair: hubSide (fed into DeviceHub as the
// "physical" server/client socket) and observerSide (what the test reads
// from to see what the hub wrote to hubSide — simulating the real remote
// peer, host, or browser client on the other end of the connection).
func wsConnPair(t *testing.T) (hubSide *websocket.Conn, observerSide *websocket.Conn, cleanup func()) {
	t.Helper()
	var sConn *websocket.Conn
	ready := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, _ := upgrader.Upgrade(w, r, nil)
		sConn = ws
		close(ready)
		// Keep alive until test ends
		select {}
	}))
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	cConn := dialWS(t, wsURL)
	<-ready
	return sConn, cConn, func() { cConn.Close(); ts.Close() }
}

func TestSessionForwarding(t *testing.T) {
	hub := NewDeviceHub()

	// Server WS for DeviceHub
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, _ := upgrader.Upgrade(w, r, nil)
		defer ws.Close()
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer ts.Close()
	serverWS := dialWS(t, "ws"+strings.TrimPrefix(ts.URL, "http"))
	defer serverWS.Close()

	if _, err := hub.AddServer("device-1", "user-1", serverWS); err != nil {
		t.Fatalf("AddServer failed: %v", err)
	}
	sess, _ := hub.CreateSession("user-1", "device-1")

	// Create a WS pair: hubSide goes into Connection, readerSide we read from
	hubSide, readerSide, cleanup := wsConnPair(t)
	defer cleanup()

	clientConn := NewConnection(hubSide, RoleClient, sess.ID)
	go clientConn.WritePump()

	hub.SetSessionClient(sess.ID, clientConn)

	// Forward to client via hub
	ok := hub.ForwardToClient(sess.ID, websocket.TextMessage, []byte(`{"type":"test"}`))
	if !ok {
		t.Error("ForwardToClient should return true")
	}

	// Read from the other end of the pair
	readerSide.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, err := readerSide.ReadMessage()
	if err != nil {
		t.Fatalf("Client failed to read: %v", err)
	}
	var msg map[string]string
	json.Unmarshal(data, &msg)
	if msg["type"] != "test" {
		t.Errorf("Expected type=test, got %s", msg["type"])
	}

	// Forward to nonexistent session
	ok = hub.ForwardToClient("nonexist", websocket.TextMessage, []byte("x"))
	if ok {
		t.Error("ForwardToClient should return false for nonexistent session")
	}
}

// --- Phase 2: signaling resilience (client/server grace + resume) ---

// readMessageType reads one WS text message with a deadline and returns its
// "type" field ("" + non-nil err if the read timed out / connection closed).
func readMessageType(t *testing.T, conn *websocket.Conn, timeout time.Duration) (string, error) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	_, data, err := conn.ReadMessage()
	if err != nil {
		return "", err
	}
	var msg map[string]string
	if err := json.Unmarshal(data, &msg); err != nil {
		return "", err
	}
	return msg["type"], nil
}

// (i) Client drops -> session survives during grace -> torn down after timeout.
func TestClientDropGraceKeepsSessionThenTearsDown(t *testing.T) {
	hub := NewDeviceHubWithGrace(120*time.Millisecond, 100)

	serverHubSide, serverObserver, cleanupServer := wsConnPair(t)
	defer cleanupServer()
	if _, err := hub.AddServer("device-1", "user-1", serverHubSide); err != nil {
		t.Fatalf("AddServer failed: %v", err)
	}

	sess, err := hub.CreateSession("user-1", "device-1")
	if err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}

	clientHubSide, _, cleanupClient := wsConnPair(t)
	defer cleanupClient()
	clientConn := NewConnection(clientHubSide, RoleClient, sess.ID)
	go clientConn.WritePump()
	hub.SetSessionClient(sess.ID, clientConn)

	// Simulate the client WS dropping — what clientReadPump's defer does.
	hub.MarkClientStale(sess.ID)

	// Check the session survives the grace window BEFORE consuming any reads
	// on serverObserver — gorilla/websocket permanently caches the first read
	// error (including a deadline timeout) on a Conn, so a single connection
	// can only be usefully read from once in these tests; verify "no message
	// yet" via hub state instead of a doomed pre-grace read attempt.
	if hub.GetSession(sess.ID) == nil {
		t.Fatal("session should survive during the grace window")
	}

	time.Sleep(250 * time.Millisecond) // past grace

	if hub.GetSession(sess.ID) != nil {
		t.Error("session should be torn down after grace expires")
	}
	msgType, err := readMessageType(t, serverObserver, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("expected peer_disconnected after grace expiry, got error: %v", err)
	}
	if msgType != "peer_disconnected" {
		t.Errorf("expected peer_disconnected, got %q", msgType)
	}
}

// (ii) Client reconnects within grace -> teardown cancelled, no peer_disconnected.
func TestClientReconnectWithinGraceCancelsTeardown(t *testing.T) {
	hub := NewDeviceHubWithGrace(150*time.Millisecond, 100)

	serverHubSide, serverObserver, cleanupServer := wsConnPair(t)
	defer cleanupServer()
	if _, err := hub.AddServer("device-1", "user-1", serverHubSide); err != nil {
		t.Fatalf("AddServer failed: %v", err)
	}

	sess, err := hub.CreateSession("user-1", "device-1")
	if err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}

	clientHubSide1, _, cleanupClient1 := wsConnPair(t)
	defer cleanupClient1()
	clientConn1 := NewConnection(clientHubSide1, RoleClient, sess.ID)
	go clientConn1.WritePump()
	hub.SetSessionClient(sess.ID, clientConn1)

	hub.MarkClientStale(sess.ID) // simulate drop

	// Reconnect within grace — what HandleClientWS does on ?session= resume.
	clientHubSide2, _, cleanupClient2 := wsConnPair(t)
	defer cleanupClient2()
	clientConn2 := NewConnection(clientHubSide2, RoleClient, sess.ID)
	go clientConn2.WritePump()
	if _, ok := hub.SetSessionClient(sess.ID, clientConn2); !ok {
		t.Fatal("SetSessionClient should succeed for an existing (stale) session")
	}

	time.Sleep(250 * time.Millisecond) // past the ORIGINAL grace window

	if hub.GetSession(sess.ID) == nil {
		t.Fatal("session should survive — grace was cancelled by the reconnect")
	}
	if _, err := readMessageType(t, serverObserver, 50*time.Millisecond); err == nil {
		t.Error("peer_disconnected should NOT be sent after a successful reconnect within grace")
	}
}

// (iii) Server drops -> session survives -> same-owner reconnect rebinds it,
// no peer_disconnected, and room_ready is re-announced to the client.
func TestServerDropGraceThenSameOwnerReconnectRebinds(t *testing.T) {
	hub := NewDeviceHubWithGrace(150*time.Millisecond, 100)

	serverHubSide1, _, cleanupServer1 := wsConnPair(t)
	defer cleanupServer1()
	server1, err := hub.AddServer("device-1", "user-1", serverHubSide1)
	if err != nil {
		t.Fatalf("AddServer failed: %v", err)
	}

	sess, err := hub.CreateSession("user-1", "device-1")
	if err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}

	clientHubSide, clientObserver, cleanupClient := wsConnPair(t)
	defer cleanupClient()
	clientConn := NewConnection(clientHubSide, RoleClient, sess.ID)
	go clientConn.WritePump()
	hub.SetSessionClient(sess.ID, clientConn)

	// Drop the server — what serverReadPump's defer does.
	hub.RemoveServerConn(server1)

	if hub.GetSession(sess.ID) == nil {
		t.Fatal("session should survive a server drop during grace")
	}
	if hub.IsOnline("device-1") {
		t.Error("device should be offline immediately after drop")
	}

	// Reconnect within grace, same owner.
	serverHubSide2, _, cleanupServer2 := wsConnPair(t)
	defer cleanupServer2()
	server2, err := hub.AddServer("device-1", "user-1", serverHubSide2)
	if err != nil {
		t.Fatalf("AddServer (resume) should succeed for the same owner: %v", err)
	}

	rebound := hub.GetSession(sess.ID)
	if rebound == nil {
		t.Fatal("session should still exist after resume")
	}
	if rebound.ServerConn != server2 {
		t.Error("session should be rebound to the NEW server connection")
	}

	// room_ready should be re-announced to the client on resume.
	msgType, err := readMessageType(t, clientObserver, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("expected room_ready on resume, got error: %v", err)
	}
	if msgType != "room_ready" {
		t.Errorf("expected room_ready, got %q", msgType)
	}

	// Past the ORIGINAL grace window: session must still be alive, no peer_disconnected.
	time.Sleep(250 * time.Millisecond)
	if hub.GetSession(sess.ID) == nil {
		t.Error("session should survive past the original grace window after rebind")
	}
	if _, err := readMessageType(t, clientObserver, 50*time.Millisecond); err == nil {
		t.Error("no peer_disconnected expected after a successful rebind")
	}
}

// (iv) Ownership mismatch on server rebind is rejected (F12b) — reconnecting
// as a device with a different JWT user than the stale session's owner must
// fail, and must NOT take over the device presence slot.
func TestServerReconnectOwnershipMismatchRejected(t *testing.T) {
	hub := NewDeviceHubWithGrace(200*time.Millisecond, 100)

	serverHubSide1, _, cleanupServer1 := wsConnPair(t)
	defer cleanupServer1()
	server1, err := hub.AddServer("device-1", "user-1", serverHubSide1)
	if err != nil {
		t.Fatalf("AddServer failed: %v", err)
	}

	sess, err := hub.CreateSession("user-1", "device-1")
	if err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}

	hub.RemoveServerConn(server1)

	// A DIFFERENT user attempts to reconnect as the same device_id while the
	// session is still server-stale.
	serverHubSide2, _, cleanupServer2 := wsConnPair(t)
	defer cleanupServer2()
	if _, err := hub.AddServer("device-1", "user-2", serverHubSide2); err != ErrNotOwner {
		t.Errorf("expected ErrNotOwner, got %v", err)
	}

	if hub.IsOnline("device-1") {
		t.Error("device should remain offline after a rejected ownership-mismatch reconnect")
	}
	rebound := hub.GetSession(sess.ID)
	if rebound == nil {
		t.Fatal("session should still exist (still within grace)")
	}
	if rebound.ServerConn != nil {
		t.Error("session should NOT be rebound to the impostor's connection")
	}
}

// (v) Concurrent stale-session cap: exceeding it evicts the oldest stale
// session (bounds memory + hijack surface, F12d).
func TestStaleSessionCapEvictsOldest(t *testing.T) {
	hub := NewDeviceHubWithGrace(5*time.Second, 2) // long grace, tiny cap

	makeSession := func(deviceID, userID string) *Session {
		serverHubSide, _, cleanup := wsConnPair(t)
		t.Cleanup(cleanup)
		if _, err := hub.AddServer(deviceID, userID, serverHubSide); err != nil {
			t.Fatalf("AddServer failed: %v", err)
		}
		sess, err := hub.CreateSession(userID, deviceID)
		if err != nil {
			t.Fatalf("CreateSession failed: %v", err)
		}
		return sess
	}

	sess1 := makeSession("device-1", "user-1")
	sess2 := makeSession("device-2", "user-2")
	sess3 := makeSession("device-3", "user-3")

	hub.MarkClientStale(sess1.ID)
	time.Sleep(15 * time.Millisecond)
	hub.MarkClientStale(sess2.ID)
	time.Sleep(15 * time.Millisecond)
	hub.MarkClientStale(sess3.ID) // 3rd stale entry exceeds cap of 2 -> evicts sess1 (oldest)

	if hub.GetSession(sess1.ID) != nil {
		t.Error("oldest stale session should have been evicted once the cap was exceeded")
	}
	if hub.GetSession(sess2.ID) == nil {
		t.Error("sess2 should still be within its grace window")
	}
	if hub.GetSession(sess3.ID) == nil {
		t.Error("sess3 should still be within its grace window")
	}
}

// Server drop with NO reconnect: grace keeps the session, then it's torn
// down with peer_disconnected sent to the client — symmetric to (i).
func TestServerDropGraceTimeoutTearsDownSession(t *testing.T) {
	hub := NewDeviceHubWithGrace(120*time.Millisecond, 100)

	serverHubSide, _, cleanupServer := wsConnPair(t)
	defer cleanupServer()
	server, err := hub.AddServer("device-1", "user-1", serverHubSide)
	if err != nil {
		t.Fatalf("AddServer failed: %v", err)
	}

	sess, err := hub.CreateSession("user-1", "device-1")
	if err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}

	clientHubSide, clientObserver, cleanupClient := wsConnPair(t)
	defer cleanupClient()
	clientConn := NewConnection(clientHubSide, RoleClient, sess.ID)
	go clientConn.WritePump()
	hub.SetSessionClient(sess.ID, clientConn)

	hub.RemoveServerConn(server)

	// See TestClientDropGraceKeepsSessionThenTearsDown for why we don't do a
	// pre-grace read attempt here (gorilla/websocket permanently poisons a
	// Conn after its first read error, including a deadline timeout).
	if hub.GetSession(sess.ID) == nil {
		t.Fatal("session should survive during the server grace window")
	}

	time.Sleep(250 * time.Millisecond) // past grace

	if hub.GetSession(sess.ID) != nil {
		t.Error("session should be torn down after server grace expires")
	}
	msgType, err := readMessageType(t, clientObserver, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("expected peer_disconnected after grace expiry, got error: %v", err)
	}
	if msgType != "peer_disconnected" {
		t.Errorf("expected peer_disconnected, got %q", msgType)
	}
}

// RemoveServerConn must not disturb a connection that has already been
// superseded by a faster reconnect (guards the AddServer/RemoveServerConn
// race described in device_hub.go).
func TestRemoveServerConnIgnoresSupersededConnection(t *testing.T) {
	hub := NewDeviceHubWithGrace(120*time.Millisecond, 100)

	serverHubSide1, _, cleanupServer1 := wsConnPair(t)
	defer cleanupServer1()
	server1, err := hub.AddServer("device-1", "user-1", serverHubSide1)
	if err != nil {
		t.Fatalf("AddServer failed: %v", err)
	}

	// A newer connection takes over before the old read pump's defer runs.
	serverHubSide2, _, cleanupServer2 := wsConnPair(t)
	defer cleanupServer2()
	server2, err := hub.AddServer("device-1", "user-1", serverHubSide2)
	if err != nil {
		t.Fatalf("AddServer (2nd) failed: %v", err)
	}

	// The OLD read pump's defer fires with the stale `server1` reference —
	// must be a no-op since server2 is now current.
	hub.RemoveServerConn(server1)

	if !hub.IsOnline("device-1") {
		t.Error("device should still be online — the newer connection must not be disturbed")
	}
	if hub.GetServer("device-1") != server2 {
		t.Error("current server should still be the newer connection")
	}
}

// R1 regression: the both-ends-reconnect case — server resume (AddServer, which
// reads ClientConn to re-announce room_ready) racing a client drop
// (MarkClientStale, which nils ClientConn). Pre-fix this was an unsynchronized
// read + check-then-use nil-deref; must be race-clean under `go test -race`.
func TestConcurrentServerResumeVsClientStaleNoRace(t *testing.T) {
	for iter := 0; iter < 40; iter++ {
		hub := NewDeviceHubWithGrace(500*time.Millisecond, 100)

		serverHubSide, _, cleanupServer := wsConnPair(t)
		server, err := hub.AddServer("device-1", "user-1", serverHubSide)
		if err != nil {
			cleanupServer()
			t.Fatalf("AddServer failed: %v", err)
		}
		sess, err := hub.CreateSession("user-1", "device-1")
		if err != nil {
			cleanupServer()
			t.Fatalf("CreateSession failed: %v", err)
		}

		clientHubSide, _, cleanupClient := wsConnPair(t)
		clientConn := NewConnection(clientHubSide, RoleClient, sess.ID)
		go clientConn.WritePump()
		hub.SetSessionClient(sess.ID, clientConn)

		// Shared blip: server socket drops → session server-stale, keeps ClientConn.
		hub.RemoveServerConn(server)

		// Race: server reconnects (resume path snapshots + notifies) vs client drop.
		newServerHubSide, _, cleanupServer2 := wsConnPair(t)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); hub.AddServer("device-1", "user-1", newServerHubSide) }()
		go func() { defer wg.Done(); hub.MarkClientStale(sess.ID) }()
		wg.Wait()

		cleanupServer()
		cleanupClient()
		cleanupServer2()
	}
}
