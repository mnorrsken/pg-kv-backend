package server

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var clientIDCounter uint64

// ClientState holds per-connection state
type ClientState struct {
	ID          uint64
	Name        string
	Addr        string
	CreatedAt   time.Time
	LibName     string
	LibVersion  string
	mu          sync.RWMutex
}

// NewClientState creates a new client state for a connection
func NewClientState(conn net.Conn) *ClientState {
	return &ClientState{
		ID:        atomic.AddUint64(&clientIDCounter, 1),
		Addr:      conn.RemoteAddr().String(),
		CreatedAt: time.Now(),
	}
}

// GetID returns the client ID
func (c *ClientState) GetID() uint64 {
	return c.ID
}

// GetName returns the client name
func (c *ClientState) GetName() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Name
}

// SetName sets the client name
func (c *ClientState) SetName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Name = name
}

// SetLibInfo sets the client library info
func (c *ClientState) SetLibInfo(libName, libVersion string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if libName != "" {
		c.LibName = libName
	}
	if libVersion != "" {
		c.LibVersion = libVersion
	}
}

// GetInfo returns a formatted info string for CLIENT INFO/LIST
func (c *ClientState) GetInfo() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	age := int64(time.Since(c.CreatedAt).Seconds())
	
	info := fmt.Sprintf("id=%d addr=%s age=%d name=%s",
		c.ID, c.Addr, age, c.Name)
	
	if c.LibName != "" {
		info += fmt.Sprintf(" lib-name=%s", c.LibName)
	}
	if c.LibVersion != "" {
		info += fmt.Sprintf(" lib-ver=%s", c.LibVersion)
	}
	
	return info
}
