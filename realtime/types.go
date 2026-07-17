package realtime

import (
	"sync"

	"github.com/gofiber/websocket/v2"
)

type Client struct {
	UserID string
	Conn   *websocket.Conn
	Send   chan interface{}
	mu     sync.RWMutex
	closed bool
}
