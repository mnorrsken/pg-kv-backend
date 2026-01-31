package server

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mnorrsken/postkeys/internal/resp"
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
	
	// Transaction state
	inTransaction   bool
	queuedCommands  []resp.Value
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

// InTransaction returns true if the client is in a MULTI transaction
func (c *ClientState) InTransaction() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.inTransaction
}

// StartTransaction starts a MULTI transaction
func (c *ClientState) StartTransaction() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inTransaction {
		return fmt.Errorf("ERR MULTI calls can not be nested")
	}
	c.inTransaction = true
	c.queuedCommands = nil
	return nil
}

// QueueCommand adds a command to the transaction queue
func (c *ClientState) QueueCommand(cmd resp.Value) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queuedCommands = append(c.queuedCommands, cmd)
}

// GetQueuedCommands returns the queued commands and clears them
func (c *ClientState) GetQueuedCommands() []resp.Value {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmds := c.queuedCommands
	c.queuedCommands = nil
	c.inTransaction = false
	return cmds
}

// DiscardTransaction discards the current transaction
func (c *ClientState) DiscardTransaction() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.inTransaction {
		return fmt.Errorf("ERR DISCARD without MULTI")
	}
	c.inTransaction = false
	c.queuedCommands = nil
	return nil
}

// QueueLength returns the number of queued commands
func (c *ClientState) QueueLength() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.queuedCommands)
}
