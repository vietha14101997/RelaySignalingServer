package relay

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Role string

const (
	RoleServer Role = "server"
	RoleClient Role = "client"
)

type Connection struct {
	Conn        *websocket.Conn
	Role        Role
	RoomID      string
	ConnectedAt time.Time

	send chan Message
	done chan struct{}
	mu   sync.Mutex
	// gracefulClosing prevents a concurrent Close from dropping a final frame
	// already accepted by SendAndClose.
	gracefulClosing bool
	closed          bool
}

type Message struct {
	Type        int // websocket.TextMessage or websocket.BinaryMessage
	Data        []byte
	closeAfter  bool
	writeResult chan bool
}

func NewConnection(conn *websocket.Conn, role Role, roomID string) *Connection {
	return &Connection{
		Conn:        conn,
		Role:        role,
		RoomID:      roomID,
		ConnectedAt: time.Now(),
		send:        make(chan Message, 256),
		done:        make(chan struct{}),
	}
}

func (c *Connection) WritePump() {
	defer c.finishClose()

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(msg.Type, msg.Data); err != nil {
				if msg.writeResult != nil {
					msg.writeResult <- false
				}
				return
			}
			if msg.writeResult != nil {
				msg.writeResult <- true
			}
			if msg.closeAfter {
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *Connection) Send(msg Message) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.gracefulClosing {
		return false
	}

	select {
	case c.send <- msg:
		return true
	default:
	}

	// Buffer full: drop the connection. Re-check the close decision under
	// the lock so a concurrent SendAndClose that committed in the
	// meantime is not overwritten by a redundant socket close.
	if c.closed || c.gracefulClosing {
		return false
	}
	conn := c.closeLocked()
	closeWebSocket(conn)
	return false
}

// SendAndClose waits for the writer pump to flush a final message before it
// closes the socket. It returns false if closure won admission or writing failed.
func (c *Connection) SendAndClose(msg Message) bool {
	msg.closeAfter = true
	msg.writeResult = make(chan bool)

	c.mu.Lock()
	// Atomic close-or-enqueue decision: once we commit to enqueueing the
	// final frame, gracefulClosing is set and Close becomes a no-op. A
	// concurrent Close that already committed is visible via c.closed and
	// rejects the caller without enqueueing — room_closed cannot be
	// enqueued after the close decision.
	if c.closed || c.gracefulClosing {
		c.mu.Unlock()
		return false
	}

	// Optimistic non-blocking enqueue. The send channel is drained by
	// WritePump holding the single-writer guarantee; a full buffer means
	// the writer is wedged and we must close the socket to break the
	// deadlock rather than silently drop the final frame.
	select {
	case c.send <- msg:
		c.gracefulClosing = true
		c.mu.Unlock()
		select {
		case ok := <-msg.writeResult:
			return ok
		case <-c.done:
			return false
		}
	default:
	}

	// Backpressure: close the socket immediately. The close decision and
	// the socket close both happen under the lock so no other caller can
	// observe a torn state.
	conn := c.closeLocked()
	closeWebSocket(conn)
	c.mu.Unlock()
	return false
}

func (c *Connection) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if c.gracefulClosing {
		// A graceful close is already in flight; the writer will close
		// the socket via finishClose after the final frame is flushed.
		return
	}
	conn := c.closeLocked()
	closeWebSocket(conn)
}

// finishClose is used by the writer after a graceful final frame and by write
// failures. Unlike Close, it must complete a graceful close already in flight.
func (c *Connection) finishClose() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	conn := c.closeLocked()
	closeWebSocket(conn)
}

// closeLocked publishes the close decision while enqueue admission is locked.
// A successful SendAndClose therefore always linearizes before this decision.
func (c *Connection) closeLocked() *websocket.Conn {
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.done)
	return c.Conn
}

func closeWebSocket(conn *websocket.Conn) {
	if conn != nil {
		conn.Close()
	}
}

func (c *Connection) Done() <-chan struct{} {
	return c.done
}
