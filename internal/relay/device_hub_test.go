package relay

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

	hub.AddServer("device-1", "user-1", ws)

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

	hub.AddServer("device-1", "user-1", ws)

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

	hub.AddServer("device-1", "user-1", ws)

	// Wrong user
	_, err = hub.CreateSession("user-2", "device-1")
	if err != ErrNotOwner {
		t.Errorf("Expected ErrNotOwner, got %v", err)
	}
}

// wsConnPair creates a connected pair: serverSide (for hub) and clientSide (for reading).
func wsConnPair(t *testing.T) (serverSide *websocket.Conn, clientSide *websocket.Conn, cleanup func()) {
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

	hub.AddServer("device-1", "user-1", serverWS)
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

func TestRemoveServerClosesSession(t *testing.T) {
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

	hub.AddServer("device-1", "user-1", ws)
	sess, _ := hub.CreateSession("user-1", "device-1")

	// Removing server should also close session
	hub.RemoveServer("device-1")

	if hub.GetSession(sess.ID) != nil {
		t.Error("Session should be removed when server goes offline")
	}
}
