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
	if c.gracefulClosing {
		c.mu.Unlock()
		return false
	}
	select {
	case <-c.done:
		c.mu.Unlock()
		return false
	default:
	}

	select {
	case c.send <- msg:
		c.mu.Unlock()
		return true
	case <-c.done:
		c.mu.Unlock()
		return false
	default:
		// Buffer full, drop connection
		conn := c.closeLocked()
		c.mu.Unlock()
		closeWebSocket(conn)
		return false
	}
}

// SendAndClose waits for the writer pump to flush a final message before it
// closes the socket. It returns false if closure won admission or writing failed.
func (c *Connection) SendAndClose(msg Message) bool {
	msg.closeAfter = true
	msg.writeResult = make(chan bool)
	c.mu.Lock()
	if c.gracefulClosing {
		c.mu.Unlock()
		return false
	}
	select {
	case <-c.done:
		c.mu.Unlock()
		return false
	default:
	}

	c.gracefulClosing = true
	select {
	case c.send <- msg:
		c.mu.Unlock()
		select {
		case ok := <-msg.writeResult:
			return ok
		case <-c.done:
			return false
		}
	case <-c.done:
		c.gracefulClosing = false
		c.mu.Unlock()
		return false
	default:
		c.gracefulClosing = false
		conn := c.closeLocked()
		c.mu.Unlock()
		closeWebSocket(conn)
		return false
	}
}

func (c *Connection) Close() {
	c.mu.Lock()
	if c.gracefulClosing {
		c.mu.Unlock()
		return
	}
	conn := c.closeLocked()
	c.mu.Unlock()
	closeWebSocket(conn)
}

// finishClose is used by the writer after a graceful final frame and by write
// failures. Unlike Close, it must complete a graceful close already in flight.
func (c *Connection) finishClose() {
	c.mu.Lock()
	conn := c.closeLocked()
	c.mu.Unlock()
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
