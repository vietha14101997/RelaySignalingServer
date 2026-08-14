package relay

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestStaleRoomClientCannotRemoveReplacement(t *testing.T) {
	room := NewRoom("ABC123", "device", "owner", nil)
	oldConn := NewConnection(nil, RoleClient, room.ID)
	newConn := NewConnection(nil, RoleClient, room.ID)
	joinedAt := time.Now().Add(-time.Minute)

	if err := room.ReserveClient(&RoomClient{
		ID:           "client",
		UserID:       "owner",
		Role:         RoleHost,
		InputAllowed: true,
		JoinedAt:     joinedAt,
	}); err != nil {
		t.Fatalf("add old client: %v", err)
	}
	if _, err := room.AttachClient("client", oldConn); err != nil {
		t.Fatalf("attach old client: %v", err)
	}
	if _, err := room.AttachClient("client", newConn); err != nil {
		t.Fatalf("replace client: %v", err)
	}

	if room.RemoveClient("client", oldConn) {
		t.Fatal("stale connection removed the replacement")
	}
	got := room.GetClient("client")
	if got == nil || got.Conn != newConn {
		t.Fatal("replacement connection was not installed")
	}
	if got.UserID != "owner" || got.Role != RoleHost || !got.InputAllowed || !got.JoinedAt.Equal(joinedAt) {
		t.Fatalf("authoritative metadata changed on reconnect: %+v", got)
	}
	if !room.RemoveClient("client", newConn) {
		t.Fatal("replacement connection could not remove itself")
	}
}

func TestRoomReconnectPreservesGuestHostMetadata(t *testing.T) {
	room := NewRoom("ABC123", "device", "", nil)
	joinedAt := time.Now().Add(-time.Minute)
	authoritative := &RoomClient{ID: "guest", JoinedAt: joinedAt}
	if err := room.ReserveClient(authoritative); err != nil {
		t.Fatalf("reserve guest: %v", err)
	}
	if authoritative.Role != RoleHost || !authoritative.InputAllowed {
		t.Fatalf("first guest should be authoritative host: %+v", authoritative)
	}

	conn := NewConnection(nil, RoleClient, room.ID)
	if _, err := room.AttachClient("guest", conn); err != nil {
		t.Fatalf("reconnect guest: %v", err)
	}
	got := room.GetClient("guest")
	if got == nil || got.Conn != conn || got.Role != RoleHost || !got.InputAllowed || !got.JoinedAt.Equal(joinedAt) {
		t.Fatalf("guest host metadata changed on reconnect: %+v", got)
	}
}

func TestRoomReconnectPreservesAuthenticatedOwnerMetadata(t *testing.T) {
	room := NewRoom("ABC123", "device", "owner", nil)
	if err := room.ReserveClient(&RoomClient{ID: "viewer", UserID: "viewer"}); err != nil {
		t.Fatalf("reserve viewer: %v", err)
	}
	joinedAt := time.Now().Add(-time.Minute)
	authoritative := &RoomClient{ID: "owner-client", UserID: "owner", JoinedAt: joinedAt}
	if err := room.ReserveClient(authoritative); err != nil {
		t.Fatalf("reserve owner: %v", err)
	}
	if authoritative.Role != RoleHost || !authoritative.InputAllowed {
		t.Fatalf("authenticated owner should be authoritative host: %+v", authoritative)
	}

	conn := NewConnection(nil, RoleClient, room.ID)
	if _, err := room.AttachClient("owner-client", conn); err != nil {
		t.Fatalf("reconnect owner: %v", err)
	}
	got := room.GetClient("owner-client")
	if got == nil || got.Conn != conn || got.UserID != "owner" || got.Role != RoleHost || !got.InputAllowed || !got.JoinedAt.Equal(joinedAt) {
		t.Fatalf("owner metadata changed on reconnect: %+v", got)
	}
}

func TestAddServerRebindsExistingRoom(t *testing.T) {
	hub := NewDeviceHub()
	serverConn1, _, cleanup1 := wsConnPair(t)
	defer cleanup1()
	server1, err := hub.AddServer("device", "owner", serverConn1)
	if err != nil {
		t.Fatalf("add first server: %v", err)
	}
	room, err := hub.CreateRoom("device", "owner")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}

	serverConn2, observer2, cleanup2 := wsConnPair(t)
	defer cleanup2()
	server2, err := hub.AddServer("device", "owner", serverConn2)
	if err != nil {
		t.Fatalf("replace server: %v", err)
	}
	if room.ServerConn != server2 || room.ServerConn == server1 {
		t.Fatal("room was not rebound to replacement server")
	}
	if !room.SendJSONToServer(map[string]string{"type": "routed"}) {
		t.Fatal("room failed to route through replacement server")
	}
	if got, err := readMessageType(t, observer2, time.Second); err != nil || got != "routed" {
		t.Fatalf("replacement server did not receive routed message: type=%q err=%v", got, err)
	}
}

func TestActiveRoomServerTakeoverRejectedWithoutDisconnectingOwner(t *testing.T) {
	hub := NewDeviceHub()
	ownerConn, ownerObserver, cleanupOwner := wsConnPair(t)
	defer cleanupOwner()
	owner, err := hub.AddServer("device", "owner", ownerConn)
	if err != nil {
		t.Fatalf("add owner server: %v", err)
	}
	room, err := hub.CreateRoom("device", "owner")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}

	attackerConn, _, cleanupAttacker := wsConnPair(t)
	defer cleanupAttacker()
	if _, err := hub.AddServer("device", "attacker", attackerConn); err != ErrNotOwner {
		t.Fatalf("takeover error = %v, want %v", err, ErrNotOwner)
	}
	if hub.GetServer("device") != owner || room.ServerConn != owner {
		t.Fatal("takeover replaced the legitimate server")
	}
	if !room.SendJSONToServer(map[string]string{"type": "owner_still_routed"}) {
		t.Fatal("legitimate server was disconnected by takeover attempt")
	}
	if got, err := readMessageType(t, ownerObserver, time.Second); err != nil || got != "owner_still_routed" {
		t.Fatalf("owner route failed after takeover: type=%q err=%v", got, err)
	}
}

func TestRoomDisconnectPreservesAdmissionForReconnect(t *testing.T) {
	room := NewRoom("ABC123", "device", "owner", nil)
	joinedAt := time.Now().Add(-time.Minute)
	if err := room.ReserveClient(&RoomClient{ID: "client", UserID: "owner", JoinedAt: joinedAt}); err != nil {
		t.Fatalf("reserve client: %v", err)
	}
	oldConn := NewConnection(nil, RoleClient, room.ID)
	if _, err := room.AttachClient("client", oldConn); err != nil {
		t.Fatalf("attach old client: %v", err)
	}
	if !room.RemoveClient("client", oldConn) {
		t.Fatal("old read pump did not detach its connection")
	}
	if !room.HasValidAdmission("client") {
		t.Fatal("disconnect erased reconnect admission metadata")
	}

	newConn := NewConnection(nil, RoleClient, room.ID)
	client, err := room.AttachClient("client", newConn)
	if err != nil {
		t.Fatalf("reconnect after old pump cleanup: %v", err)
	}
	if client.UserID != "owner" || client.Role != RoleHost || !client.InputAllowed || !client.JoinedAt.Equal(joinedAt) {
		t.Fatalf("reconnect metadata changed: %+v", client)
	}
}

func TestRoomReservationsExpireAndReleaseCapacity(t *testing.T) {
	room := NewRoom("ABC123", "device", "owner", nil)
	room.admissionTTL = 20 * time.Millisecond
	for i := 0; i < MaxClientsPerRoom; i++ {
		id := fmt.Sprintf("client-%d", i)
		if err := room.ReserveClient(&RoomClient{ID: id}); err != nil {
			t.Fatalf("reserve %s: %v", id, err)
		}
	}
	if err := room.ReserveClient(&RoomClient{ID: "full"}); err != ErrRoomFull {
		t.Fatalf("full room error = %v, want %v", err, ErrRoomFull)
	}

	time.Sleep(30 * time.Millisecond)
	if removed := room.PruneExpiredAdmissions(time.Now()); removed != MaxClientsPerRoom {
		t.Fatalf("pruned %d reservations, want %d", removed, MaxClientsPerRoom)
	}
	if room.HasValidAdmission("client-0") {
		t.Fatal("expired reservation remained attachable")
	}
	if err := room.ReserveClient(&RoomClient{ID: "replacement"}); err != nil {
		t.Fatalf("expired reservations still consumed capacity: %v", err)
	}
}

func TestCleanupRoomAdmissionsPrunesHubRooms(t *testing.T) {
	hub := NewDeviceHub()
	room := NewRoom("ABC123", "device", "owner", nil)
	room.admissionTTL = time.Millisecond
	hub.rooms[room.ID] = room
	if err := room.ReserveClient(&RoomClient{ID: "abandoned"}); err != nil {
		t.Fatalf("reserve client: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if removed := hub.CleanupRoomAdmissions(); removed != 1 {
		t.Fatalf("cleanup removed %d admissions, want 1", removed)
	}
	if room.ClientCount() != 0 {
		t.Fatal("hub cleanup left expired admission consuming capacity")
	}
}

func TestAttachClientRejectsUnreservedID(t *testing.T) {
	room := NewRoom("ABC123", "device", "owner", nil)
	if _, err := room.AttachClient("caller-controlled", NewConnection(nil, RoleClient, room.ID)); err != ErrClientNotAdmitted {
		t.Fatalf("attach error = %v, want %v", err, ErrClientNotAdmitted)
	}
	if room.ClientCount() != 0 {
		t.Fatal("unreserved client consumed room capacity")
	}
}

func TestOnlineServerSerializesConcurrentWrites(t *testing.T) {
	hub := NewDeviceHub()
	hubSide, observer, cleanup := wsConnPair(t)
	defer cleanup()
	server, err := hub.AddServer("device", "owner", hubSide)
	if err != nil {
		t.Fatalf("add server: %v", err)
	}

	const messages = 64
	errs := make(chan string, messages)
	var writers sync.WaitGroup
	writers.Add(messages)
	for i := 0; i < messages; i++ {
		go func(i int) {
			defer writers.Done()
			if !server.SendMessage(websocket.TextMessage, []byte(fmt.Sprintf("message-%d", i))) {
				errs <- fmt.Sprintf("send %d failed", i)
			}
		}(i)
	}
	writers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	observer.SetReadDeadline(time.Now().Add(3 * time.Second))
	for i := 0; i < messages; i++ {
		if _, _, err := observer.ReadMessage(); err != nil {
			t.Fatalf("read message %d: %v", i, err)
		}
	}
}

func TestSendToServerDoesNotHoldRoomMutexDuringWrite(t *testing.T) {
	server := &OnlineServer{Generation: 1, done: make(chan struct{})}
	room := NewRoom("ABC123", "device", "owner", server)
	server.writeMu.Lock()
	sendDone := make(chan struct{})
	go func() {
		room.SendToServer(websocket.TextMessage, []byte("blocked"))
		close(sendDone)
	}()
	time.Sleep(10 * time.Millisecond)

	stateDone := make(chan struct{})
	go func() {
		room.SetState(RoomStreaming)
		close(stateDone)
	}()
	select {
	case <-stateDone:
	case <-time.After(100 * time.Millisecond):
		server.writeMu.Unlock()
		t.Fatal("room mutex was held while server write was blocked")
	}
	server.writeMu.Unlock()
	select {
	case <-sendDone:
	case <-time.After(time.Second):
		t.Fatal("blocked send did not finish")
	}
}

func TestClosedRoomRejectsDetachedAdmission(t *testing.T) {
	hub := NewDeviceHub()
	room := NewRoom("ABC123", "device", "owner", nil)
	hub.rooms[room.ID] = room
	hub.serverRooms[room.DeviceID] = room.ID
	hub.RemoveRoom(room.ID)
	if err := room.ReserveClient(&RoomClient{ID: "late", UserID: "owner"}); err != ErrRoomNotFound {
		t.Fatalf("detached room admission error = %v, want %v", err, ErrRoomNotFound)
	}
	if room.ClientCount() != 0 {
		t.Fatal("detached room admitted a client")
	}
}

func TestCloseAllClientsFlushesRoomClosed(t *testing.T) {
	room := NewRoom("ABC123", "device", "owner", nil)
	hubSide, observer, cleanup := wsConnPair(t)
	defer cleanup()
	conn := NewConnection(hubSide, RoleClient, room.ID)
	go conn.WritePump()
	if err := room.ReserveClient(&RoomClient{ID: "client", UserID: "owner"}); err != nil {
		t.Fatalf("add client: %v", err)
	}
	if _, err := room.AttachClient("client", conn); err != nil {
		t.Fatalf("attach client: %v", err)
	}

	if !room.CloseAllClients("server disconnected") {
		t.Fatal("room_closed was not accepted")
	}
	if got, err := readMessageType(t, observer, time.Second); err != nil || got != "room_closed" {
		t.Fatalf("room_closed was not flushed before close: type=%q err=%v", got, err)
	}
	select {
	case <-conn.Done():
	case <-time.After(time.Second):
		t.Fatal("connection did not close after room_closed")
	}
}

func TestSendAndCloseConcurrentCloseFlushesFinalMessage(t *testing.T) {
	for i := 0; i < 50; i++ {
		hubSide, observer, cleanup := wsConnPair(t)
		conn := NewConnection(hubSide, RoleClient, "room")
		go conn.WritePump()

		start := make(chan struct{})
		accepted := make(chan bool, 1)
		closed := make(chan struct{})
		go func() {
			<-start
			accepted <- conn.SendAndClose(Message{Type: websocket.TextMessage, Data: []byte(`{"type":"final"}`)})
		}()
		go func() {
			<-start
			conn.Close()
			close(closed)
		}()
		close(start)

		if <-accepted {
			if got, err := readMessageType(t, observer, time.Second); err != nil || got != "final" {
				cleanup()
				t.Fatalf("iteration %d: accepted final message was not flushed: type=%q err=%v", i, got, err)
			}
		}
		select {
		case <-closed:
		case <-time.After(time.Second):
			cleanup()
			t.Fatalf("iteration %d: concurrent Close blocked", i)
		}
		cleanup()
	}
}

func TestOnlineServerCloseDoesNotWaitForWriter(t *testing.T) {
	hubSide, observer, cleanup := wsConnPair(t)
	defer cleanup()
	server := &OnlineServer{Conn: hubSide, done: make(chan struct{})}

	// Holding writeMu deterministically models a writer inside WriteMessage.
	server.writeMu.Lock()
	closed := make(chan struct{})
	go func() {
		server.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(100 * time.Millisecond):
		server.writeMu.Unlock()
		t.Fatal("Close waited behind the in-flight writer")
	}
	server.writeMu.Unlock()

	observer.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := observer.ReadMessage(); err == nil {
		t.Fatal("Close returned without closing the underlying socket")
	}
}

func TestReplacementRejectsStaleServerMessages(t *testing.T) {
	hub := NewDeviceHub()
	oldSide, _, cleanupOld := wsConnPair(t)
	defer cleanupOld()
	old, err := hub.AddServer("device", "owner", oldSide)
	if err != nil {
		t.Fatalf("add old server: %v", err)
	}
	room, err := hub.CreateRoom("device", "owner")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	clientSide, observer, cleanupClient := wsConnPair(t)
	defer cleanupClient()
	client := NewConnection(clientSide, RoleClient, room.ID)
	go client.WritePump()
	if err := room.ReserveClient(&RoomClient{ID: "client", UserID: "owner"}); err != nil {
		t.Fatalf("reserve client: %v", err)
	}
	if _, err := room.AttachClient("client", client); err != nil {
		t.Fatalf("attach client: %v", err)
	}

	newSide, _, cleanupNew := wsConnPair(t)
	defer cleanupNew()
	newServer, err := hub.AddServer("device", "owner", newSide)
	if err != nil {
		t.Fatalf("replace server: %v", err)
	}
	handler := NewWSHandler(hub, nil, 1<<20)
	if handler.routeCurrentServerMessage(old, websocket.TextMessage, []byte(`{"type":"stale"}`)) {
		t.Fatal("stale server frame was accepted after replacement")
	}
	if !handler.routeCurrentServerMessage(newServer, websocket.TextMessage, []byte(`{"type":"current"}`)) {
		t.Fatal("current server frame was rejected")
	}
	if got, err := readMessageType(t, observer, time.Second); err != nil || got != "current" {
		t.Fatalf("routed message type=%q err=%v, want current", got, err)
	}
}
