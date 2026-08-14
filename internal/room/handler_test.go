package room

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"golang.org/x/crypto/bcrypt"

	"github.com/reka/relay-server/internal/relay"
)

func setupRoomHandler(t *testing.T) (*relay.Room, *httptest.Server) {
	t.Helper()
	hub := relay.NewDeviceHub()
	room := relay.NewRoom("ABC123", "device", "owner", nil)
	passwordHash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	room.PasswordHash = string(passwordHash)
	handler := NewHandler(hub, 1<<20)

	e := echo.New()
	e.POST("/rooms/join", handler.Join)
	e.GET("/ws/room", handler.HandleRoomWS)

	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	serverConn, _, cleanup := roomServerPair(t)
	t.Cleanup(cleanup)
	if _, err := hub.AddServer("device", "owner", serverConn); err != nil {
		t.Fatalf("add server: %v", err)
	}
	created, err := hub.CreateRoom("device", "owner")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	created.PasswordHash = room.PasswordHash
	return created, server
}

func TestHandleRoomWSRejectsUnreservedClientID(t *testing.T) {
	room, server := setupRoomHandler(t)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/room?room_id=" + room.ID + "&client_id=attacker"
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if conn != nil {
		conn.Close()
	}
	if err == nil {
		t.Fatal("unreserved client websocket was accepted")
	}
	if response == nil || response.StatusCode != http.StatusForbidden {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		t.Fatalf("status = %d, want %d", status, http.StatusForbidden)
	}
	if room.ClientCount() != 0 {
		t.Fatal("unreserved websocket created a client admission")
	}
}

func TestJoinReservationAllowsRoomWebSocket(t *testing.T) {
	room, server := setupRoomHandler(t)
	response, err := http.Post(server.URL+"/rooms/join", "application/json", strings.NewReader(`{"room_id":"`+room.ID+`","password":"password"}`))
	if err != nil {
		t.Fatalf("join request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("join status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	var joined struct {
		ClientID string `json:"client_id"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(response.Body).Decode(&joined); err != nil {
		t.Fatalf("decode join response: %v", err)
	}

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/room?room_id=" + room.ID + "&client_id=" + joined.ClientID
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial admitted websocket: %v", err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read room_ready: %v", err)
	}
	var ready map[string]string
	if err := json.Unmarshal(data, &ready); err != nil {
		t.Fatalf("decode room_ready: %v", err)
	}
	if ready["type"] != "room_ready" || ready["client_id"] != joined.ClientID || ready["role"] != joined.Role {
		t.Fatalf("unexpected room_ready: %v", ready)
	}
}

func roomServerPair(t *testing.T) (*websocket.Conn, *websocket.Conn, func()) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			accepted <- conn
		}
	}))
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		server.Close()
		t.Fatalf("dial websocket pair: %v", err)
	}
	serverSide := <-accepted
	return serverSide, client, func() {
		serverSide.Close()
		client.Close()
		server.Close()
	}
}
