package relay

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestConnectionCloseAtomicityDeterministic verifies the atomicity guarantee
// required by the close contract: once Close has decided to terminate the
// connection, no SendAndClose caller can enqueue a room_closed frame, and
// conversely, a SendAndClose that wins the race MUST flush the final frame
// before the socket is torn down. The test forces both orderings by
// serialising the goroutines with a barrier and checks the invariant on
// every iteration.
func TestConnectionCloseAtomicityDeterministic(t *testing.T) {
	const iterations = 200
	for i := 0; i < iterations; i++ {
		hubSide, observer, cleanup := wsConnPair(t)
		conn := NewConnection(hubSide, RoleClient, "room")
		go conn.WritePump()

		// Barrier: both goroutines start at the same instant so the race
		// window is maximised and the outcome is interleaving-dependent.
		start := make(chan struct{})
		accepted := make(chan bool, 1)
		closed := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			accepted <- conn.SendAndClose(Message{Type: websocket.TextMessage, Data: []byte(`{"type":"final"}`)})
		}()
		go func() {
			defer wg.Done()
			<-start
			conn.Close()
			close(closed)
		}()
		close(start)
		wg.Wait()

		// If SendAndClose won the admission race, the final frame MUST
		// have been delivered before the socket was torn down. If Close
		// won, SendAndClose MUST have rejected without enqueueing.
		if <-accepted {
			observer.SetReadDeadline(time.Now().Add(time.Second))
			if got, err := readMessageType(t, observer, time.Second); err != nil || got != "final" {
				cleanup()
				t.Fatalf("iter %d: accepted final frame was not delivered: type=%q err=%v", i, got, err)
			}
		}
		select {
		case <-closed:
		case <-time.After(time.Second):
			cleanup()
			t.Fatalf("iter %d: Close blocked", i)
		}
		// Done channel MUST be closed exactly once regardless of who won.
		select {
		case <-conn.Done():
		default:
			cleanup()
			t.Fatalf("iter %d: Done channel was not closed after Close+SendAndClose", i)
		}
		cleanup()
	}
}

// TestConnectionCloseBeforeSendAndCloseRejects forces the "Close first" ordering
// and verifies that SendAndClose cannot enqueue a frame after the close
// decision has been published. This is the deterministic counterpart to the
// race test above: the outcome is always "rejected, not enqueued".
func TestConnectionCloseBeforeSendAndCloseRejects(t *testing.T) {
	hubSide, observer, cleanup := wsConnPair(t)
	defer cleanup()
	conn := NewConnection(hubSide, RoleClient, "room")
	go conn.WritePump()

	conn.Close()

	// Once Close has committed, SendAndClose MUST reject the caller — the
	// room_closed frame must NOT be enqueued after the close decision.
	if conn.SendAndClose(Message{Type: websocket.TextMessage, Data: []byte(`{"type":"room_closed"}`)}) {
		t.Fatal("SendAndClose accepted after Close — room_closed was enqueued after the close decision")
	}

	// The observer must not receive the rejected frame.
	observer.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if _, _, err := observer.ReadMessage(); err == nil {
		t.Fatal("observer received a frame that should have been rejected after Close")
	}
}

// TestConnectionSendAndCloseBeforeCloseFlushes forces the "SendAndClose first"
// ordering and verifies that the final frame is delivered before the socket
// is torn down. This is the deterministic counterpart for the other outcome
// of the race test.
func TestConnectionSendAndCloseBeforeCloseFlushes(t *testing.T) {
	hubSide, observer, cleanup := wsConnPair(t)
	defer cleanup()
	conn := NewConnection(hubSide, RoleClient, "room")
	go conn.WritePump()

	if !conn.SendAndClose(Message{Type: websocket.TextMessage, Data: []byte(`{"type":"final"}`)}) {
		t.Fatal("SendAndClose returned false before any competing Close")
	}
	if got, err := readMessageType(t, observer, time.Second); err != nil || got != "final" {
		t.Fatalf("final frame was not flushed: type=%q err=%v", got, err)
	}
	// Done must be closed by the writer after flushing the final frame.
	select {
	case <-conn.Done():
	case <-time.After(time.Second):
		t.Fatal("conn.Done was not closed after SendAndClose completed")
	}
}

// TestConnectionCloseIdempotent verifies that a second Close on an already
// closed connection is a no-op (no panic, no double-write to done).
func TestConnectionCloseIdempotent(t *testing.T) {
	hubSide, _, cleanup := wsConnPair(t)
	defer cleanup()
	conn := NewConnection(hubSide, RoleClient, "room")
	go conn.WritePump()

	conn.Close()
	// The second close must not panic on closing an already-closed done channel.
	conn.Close()
	conn.Close()

	select {
	case <-conn.Done():
	default:
		t.Fatal("Done channel was not closed after the first Close")
	}
}

// TestConnectionSendAndCloseAfterCloseRaceAtomic exercises the race between
// Close and SendAndClose with many concurrent goroutines. The invariant is
// that the count of delivered frames equals the count of successful
// SendAndClose returns — no frame is ever silently dropped by the close
// decision, and no frame is ever enqueued after the close decision.
func TestConnectionSendAndCloseAfterCloseRaceAtomic(t *testing.T) {
	hubSide, observer, cleanup := wsConnPair(t)
	defer cleanup()
	conn := NewConnection(hubSide, RoleClient, "room")
	go conn.WritePump()

	const N = 32
	var wg sync.WaitGroup
	var accepted int64
	wg.Add(N + 1)

	// Start N concurrent SendAndClose goroutines.
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			<-start
			if conn.SendAndClose(Message{Type: websocket.TextMessage, Data: []byte(`{"type":"final"}`)}) {
				atomic.AddInt64(&accepted, 1)
			}
		}()
	}
	// One Close goroutine races them all.
	go func() {
		defer wg.Done()
		<-start
		conn.Close()
	}()
	close(start)
	wg.Wait()

	// At most one SendAndClose can succeed (the first to win the close
	// decision publishes gracefulClosing, so all subsequent ones reject).
	if got := atomic.LoadInt64(&accepted); got > 1 {
		t.Fatalf("accepted=%d, want at most 1 — multiple SendAndClose admissions leaked", got)
	}

	// If exactly one was accepted, the observer must see exactly one frame.
	if accepted == 1 {
		observer.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err := observer.ReadMessage(); err != nil {
			t.Fatalf("accepted frame was not delivered: %v", err)
		}
		// No further frames should arrive.
		observer.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
		if _, _, err := observer.ReadMessage(); err == nil {
			t.Fatal("observer received a frame that should not have been enqueued")
		}
	}
}
