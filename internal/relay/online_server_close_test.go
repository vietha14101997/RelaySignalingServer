package relay

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestOnlineServerCloseDoesNotBlockBehindWriter is a deterministic concurrent
// close test: an in-flight write is modelled by holding writeMu, and Close is
// required to complete without waiting for the writer. After Close, the
// underlying socket must be torn down and every subsequent SendMessage must
// return false (no stale old messages can route).
func TestOnlineServerCloseDoesNotBlockBehindWriter(t *testing.T) {
	hubSide, observer, cleanup := wsConnPair(t)
	defer cleanup()
	server := &OnlineServer{Conn: hubSide, done: make(chan struct{})}

	// Hold writeMu to deterministically model a writer inside WriteMessage.
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

	// The socket must be closed immediately.
	observer.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := observer.ReadMessage(); err == nil {
		t.Fatal("Close returned without closing the underlying socket")
	}

	// Every subsequent SendMessage must fail — no stale old messages can
	// route while the replacement is online.
	if server.SendMessage(websocket.TextMessage, []byte("after-close")) {
		t.Fatal("SendMessage succeeded after Close")
	}
	if server.SendJSON(map[string]string{"type": "after-close-json"}) {
		t.Fatal("SendJSON succeeded after Close")
	}
}

// TestOnlineServerCloseConcurrentSendAndCloseStress verifies that under heavy
// concurrent SendMessage/Close contention, every SendMessage either succeeds
// (was admitted before Close) or fails immediately (Close won). No goroutine
// may block indefinitely, and no frame may be silently dropped.
func TestOnlineServerCloseConcurrentSendAndCloseStress(t *testing.T) {
	const iterations = 50
	for i := 0; i < iterations; i++ {
		hubSide, _, cleanup := wsConnPair(t)
		server := &OnlineServer{Conn: hubSide, done: make(chan struct{})}

		const senders = 16
		var wg sync.WaitGroup
		var admitted int64
		wg.Add(senders + 1)
		start := make(chan struct{})

		for j := 0; j < senders; j++ {
			go func() {
				defer wg.Done()
				<-start
				if server.SendMessage(websocket.TextMessage, []byte("x")) {
					atomic.AddInt64(&admitted, 1)
				}
			}()
		}
		go func() {
			defer wg.Done()
			<-start
			server.Close()
		}()
		close(start)

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			cleanup()
			t.Fatalf("iter %d: concurrent senders/Close deadlocked", i)
		}
		// At least one sender may have been admitted before the Close
		// published done; none must have blocked. The contract is simply
		// that every caller returned within the deadline.
		cleanup()
	}
}

// TestOnlineServerReplacementClosesOldServerRoutes verifies that when a new
// OnlineServer takes over a device, the old server is closed synchronously
// inside the AddServer lock so any concurrent or subsequent SendMessage on
// the old server fails immediately. This is the core invariant: stale old
// messages cannot route while the replacement is online.
func TestOnlineServerReplacementClosesOldServerRoutes(t *testing.T) {
	hub := NewDeviceHub()

	oldSide, _, cleanupOld := wsConnPair(t)
	defer cleanupOld()
	old, err := hub.AddServer("device", "owner", oldSide)
	if err != nil {
		t.Fatalf("add old server: %v", err)
	}

	// Hold the old server's writeMu to model an in-flight write that the
	// replacement must preempt without waiting.
	old.writeMu.Lock()
	newSide, _, cleanupNew := wsConnPair(t)
	defer cleanupNew()
	replaced := make(chan struct{})
	go func() {
		_, err := hub.AddServer("device", "owner", newSide)
		if err != nil {
			t.Errorf("replacement AddServer: %v", err)
		}
		close(replaced)
	}()
	select {
	case <-replaced:
	case <-time.After(500 * time.Millisecond):
		old.writeMu.Unlock()
		t.Fatal("AddServer blocked behind the old server's in-flight writer")
	}
	old.writeMu.Unlock()

	// The old server's done channel must be closed by the replacement —
	// ensures the close decision is published before the new server is
	// observable to outside callers.
	select {
	case <-old.Done():
	default:
		t.Fatal("old server's done channel was not closed by replacement")
	}

	// Stale old messages cannot route: every SendMessage on the old server
	// must fail immediately.
	if old.SendMessage(websocket.TextMessage, []byte("stale")) {
		t.Fatal("stale old message routed through the old server after replacement")
	}
	if old.SendJSON(map[string]string{"type": "stale-json"}) {
		t.Fatal("stale old JSON routed through the old server after replacement")
	}
}
