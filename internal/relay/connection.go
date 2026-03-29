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
	once sync.Once
}

type Message struct {
	Type int    // websocket.TextMessage or websocket.BinaryMessage
	Data []byte
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
	defer c.Close()

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(msg.Type, msg.Data); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *Connection) Send(msg Message) bool {
	select {
	case c.send <- msg:
		return true
	default:
		// Buffer full, drop connection
		c.Close()
		return false
	}
}

func (c *Connection) Close() {
	c.once.Do(func() {
		close(c.done)
		c.Conn.Close()
	})
}

func (c *Connection) Done() <-chan struct{} {
	return c.done
}
