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

func setupTestServer(hub *Hub, maxMsgSize int64) *httptest.Server {
	handler := NewHandler(hub, maxMsgSize)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		roleStr := r.URL.Query().Get("role")
		roomID := r.URL.Query().Get("room")

		if roomID == "" || (roleStr != "server" && roleStr != "client") {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		conn := NewConnection(ws, Role(roleStr), roomID)
		go conn.WritePump()

		paired := hub.Join(roomID, conn)
		if paired {
			hub.NotifyRoomReady(roomID)
		}

		handler.readPump(conn)
	})

	return httptest.NewServer(mux)
}

func connectWS(t *testing.T, url, role, room string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(url, "http") + "/ws?role=" + role + "&room=" + room
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect as %s: %v", role, err)
	}
	return ws
}

func readJSON(t *testing.T, ws *websocket.Conn) map[string]string {
	t.Helper()
	ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("Failed to read message: %v", err)
	}
	var msg map[string]string
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}
	return msg
}

func TestPairAndRoomReady(t *testing.T) {
	hub := NewHub()
	ts := setupTestServer(hub, 1<<20)
	defer ts.Close()

	// Server connects first
	serverWS := connectWS(t, ts.URL, "server", "test-room")
	defer serverWS.Close()

	// Client connects — should trigger room_ready
	clientWS := connectWS(t, ts.URL, "client", "test-room")
	defer clientWS.Close()

	// Both should receive room_ready
	serverMsg := readJSON(t, serverWS)
	if serverMsg["type"] != "room_ready" {
		t.Errorf("Server expected room_ready, got %s", serverMsg["type"])
	}

	clientMsg := readJSON(t, clientWS)
	if clientMsg["type"] != "room_ready" {
		t.Errorf("Client expected room_ready, got %s", clientMsg["type"])
	}
}

func TestForwardTextMessages(t *testing.T) {
	hub := NewHub()
	ts := setupTestServer(hub, 1<<20)
	defer ts.Close()

	serverWS := connectWS(t, ts.URL, "server", "fwd-room")
	defer serverWS.Close()
	clientWS := connectWS(t, ts.URL, "client", "fwd-room")
	defer clientWS.Close()

	// Drain room_ready messages
	readJSON(t, serverWS)
	readJSON(t, clientWS)

	// Client sends to server
	testMsg := `{"type":"hardware_info_ack","codec":"h265"}`
	clientWS.WriteMessage(websocket.TextMessage, []byte(testMsg))

	serverWS.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, err := serverWS.ReadMessage()
	if err != nil {
		t.Fatalf("Server failed to read forwarded message: %v", err)
	}
	if string(data) != testMsg {
		t.Errorf("Expected %s, got %s", testMsg, string(data))
	}

	// Server sends to client
	testMsg2 := `{"type":"suggested_config","bitrate":15000}`
	serverWS.WriteMessage(websocket.TextMessage, []byte(testMsg2))

	clientWS.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data2, err := clientWS.ReadMessage()
	if err != nil {
		t.Fatalf("Client failed to read forwarded message: %v", err)
	}
	if string(data2) != testMsg2 {
		t.Errorf("Expected %s, got %s", testMsg2, string(data2))
	}
}

func TestForwardBinaryMessages(t *testing.T) {
	hub := NewHub()
	ts := setupTestServer(hub, 1<<20)
	defer ts.Close()

	serverWS := connectWS(t, ts.URL, "server", "bin-room")
	defer serverWS.Close()
	clientWS := connectWS(t, ts.URL, "client", "bin-room")
	defer clientWS.Close()

	// Drain room_ready
	readJSON(t, serverWS)
	readJSON(t, clientWS)

	// Send binary (simulating speedtest data)
	binData := make([]byte, 1024)
	for i := range binData {
		binData[i] = byte(i % 256)
	}
	serverWS.WriteMessage(websocket.BinaryMessage, binData)

	clientWS.SetReadDeadline(time.Now().Add(3 * time.Second))
	msgType, data, err := clientWS.ReadMessage()
	if err != nil {
		t.Fatalf("Client failed to read binary: %v", err)
	}
	if msgType != websocket.BinaryMessage {
		t.Errorf("Expected binary message type, got %d", msgType)
	}
	if len(data) != len(binData) {
		t.Errorf("Binary size mismatch: expected %d, got %d", len(binData), len(data))
	}
}

func TestPeerDisconnectNotification(t *testing.T) {
	hub := NewHub()
	ts := setupTestServer(hub, 1<<20)
	defer ts.Close()

	serverWS := connectWS(t, ts.URL, "server", "dc-room")
	defer serverWS.Close()
	clientWS := connectWS(t, ts.URL, "client", "dc-room")

	// Drain room_ready
	readJSON(t, serverWS)
	readJSON(t, clientWS)

	// Client disconnects
	clientWS.Close()

	// Server should receive peer_disconnected
	serverMsg := readJSON(t, serverWS)
	if serverMsg["type"] != "peer_disconnected" {
		t.Errorf("Server expected peer_disconnected, got %s", serverMsg["type"])
	}
}

func TestHubStats(t *testing.T) {
	hub := NewHub()

	rooms, conns := hub.Stats()
	if rooms != 0 || conns != 0 {
		t.Errorf("Expected 0 rooms, 0 conns; got %d, %d", rooms, conns)
	}

	ts := setupTestServer(hub, 1<<20)
	defer ts.Close()

	serverWS := connectWS(t, ts.URL, "server", "stats-room")
	defer serverWS.Close()

	// Wait for connection to register
	time.Sleep(100 * time.Millisecond)

	rooms, conns = hub.Stats()
	if rooms != 1 || conns != 1 {
		t.Errorf("Expected 1 room, 1 conn; got %d, %d", rooms, conns)
	}

	clientWS := connectWS(t, ts.URL, "client", "stats-room")
	defer clientWS.Close()

	// Drain room_ready
	readJSON(t, serverWS)
	readJSON(t, clientWS)

	time.Sleep(100 * time.Millisecond)

	rooms, conns = hub.Stats()
	if rooms != 1 || conns != 2 {
		t.Errorf("Expected 1 room, 2 conns; got %d, %d", rooms, conns)
	}
}
