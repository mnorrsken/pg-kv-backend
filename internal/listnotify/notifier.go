// Package listnotify provides LISTEN/NOTIFY based notifications for blocking list operations.
// This allows BRPOP/BLPOP to wait efficiently without polling.
package listnotify

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Channel name for list push notifications
const listPushChannel = "postkeys_list_push"

// Notifier manages notifications for blocking list operations
type Notifier struct {
	pool     *pgxpool.Pool
	connStr  string

	// Listener connection
	listenerConn *pgx.Conn

	// Subscribers waiting for specific keys
	mu          sync.RWMutex
	subscribers map[string][]chan string // key -> list of waiting channels

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	debug  bool
}

// New creates a new list notifier
func New(pool *pgxpool.Pool, connStr string) *Notifier {
	ctx, cancel := context.WithCancel(context.Background())
	return &Notifier{
		pool:        pool,
		connStr:     connStr,
		subscribers: make(map[string][]chan string),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// SetDebug enables debug logging
func (n *Notifier) SetDebug(debug bool) {
	n.debug = debug
}

// Start initializes the notifier and starts listening
func (n *Notifier) Start(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, n.connStr)
	if err != nil {
		return fmt.Errorf("failed to create listener connection: %w", err)
	}
	n.listenerConn = conn

	// Start listening on the list push channel
	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", listPushChannel))
	if err != nil {
		conn.Close(ctx)
		return fmt.Errorf("failed to LISTEN: %w", err)
	}

	n.wg.Add(1)
	go n.listenLoop()

	return nil
}

// Stop gracefully stops the notifier
func (n *Notifier) Stop() {
	n.cancel()
	n.wg.Wait()

	if n.listenerConn != nil {
		n.listenerConn.Close(context.Background())
	}
}

// NotifyPush sends a notification that items were pushed to a list key
func (n *Notifier) NotifyPush(ctx context.Context, key string) error {
	_, err := n.pool.Exec(ctx, "SELECT pg_notify($1, $2)", listPushChannel, key)
	return err
}

// WaitForKey waits for a notification on the given key or until timeout/cancellation
// Returns true if notified, false if timeout or cancelled
func (n *Notifier) WaitForKey(ctx context.Context, key string, timeout time.Duration) bool {
	ch := make(chan string, 1)

	// Register the subscriber
	n.mu.Lock()
	n.subscribers[key] = append(n.subscribers[key], ch)
	n.mu.Unlock()

	// Ensure cleanup
	defer func() {
		n.mu.Lock()
		channels := n.subscribers[key]
		for i, c := range channels {
			if c == ch {
				n.subscribers[key] = append(channels[:i], channels[i+1:]...)
				break
			}
		}
		if len(n.subscribers[key]) == 0 {
			delete(n.subscribers, key)
		}
		n.mu.Unlock()
		close(ch)
	}()

	// Wait for notification or timeout
	var timer <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timer = t.C
	}

	select {
	case <-ch:
		return true
	case <-timer:
		return false
	case <-ctx.Done():
		return false
	case <-n.ctx.Done():
		return false
	}
}

// listenLoop continuously listens for PostgreSQL notifications
func (n *Notifier) listenLoop() {
	defer n.wg.Done()

	for {
		select {
		case <-n.ctx.Done():
			return
		default:
		}

		// Wait for notification with a timeout for graceful shutdown
		ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
		notification, err := n.listenerConn.WaitForNotification(ctx)
		cancel()

		if err != nil {
			if n.ctx.Err() != nil {
				return
			}
			continue
		}

		key := notification.Payload
		if n.debug {
			log.Printf("[DEBUG] List push notification for key: %s", key)
		}

		// Notify all subscribers waiting for this key
		n.mu.RLock()
		channels := n.subscribers[key]
		n.mu.RUnlock()

		for _, ch := range channels {
			select {
			case ch <- key:
			default:
				// Channel full or closed, skip
			}
		}
	}
}
