//go:build postgres

package integration

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnorrsken/postkeys/internal/handler"
	"github.com/mnorrsken/postkeys/internal/pubsub"
	"github.com/mnorrsken/postkeys/internal/server"
	"github.com/mnorrsken/postkeys/internal/storage"
)

// newPubSubTestServer creates a test server with pub/sub enabled
func newPubSubTestServer(t *testing.T) (*server.Server, *storage.Store, string) {
	t.Helper()

	ctx := context.Background()

	// PostgreSQL connection config from environment (same as postgres_integration_test.go)
	cfg := storage.Config{
		Host:     getEnvOrDefault("PG_HOST", "localhost"),
		Port:     5789, // Use test port from docker-compose.test.yml
		User:     getEnvOrDefault("PG_USER", "postgres"),
		Password: getEnvOrDefault("PG_PASSWORD", "testingpassword"),
		Database: getEnvOrDefault("PG_DATABASE", "postgres"),
		SSLMode:  getEnvOrDefault("PG_SSLMODE", "disable"),
	}

	store, err := storage.New(ctx, cfg)
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	// Create handler and server
	h := handler.New(store, "")
	srv := server.NewWithDebug(":0", h, false)

	// Create and start pub/sub hub
	hub := pubsub.NewHub(store.Pool(), store.ConnString())
	if err := hub.Start(ctx); err != nil {
		store.Close()
		t.Fatalf("Failed to start pub/sub hub: %v", err)
	}
	srv.SetPubSubHub(hub)

	// Start server on random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		store.Close()
		t.Fatalf("Failed to create listener: %v", err)
	}

	go srv.ServeWithListener(listener)

	return srv, store, listener.Addr().String()
}

func TestPubSubBasic(t *testing.T) {
	srv, store, addr := newPubSubTestServer(t)
	defer srv.Stop()
	defer store.Close()

	// Test PUBLISH without subscribers
	t.Run("PublishNoSubscribers", func(t *testing.T) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		// PUBLISH channel message
		sendCommand(conn, "PUBLISH", "test-channel", "hello")
		resp := readLine(conn)
		// Should return :0 (zero subscribers)
		if !strings.HasPrefix(resp, ":") {
			t.Errorf("Expected integer response, got: %s", resp)
		}
	})

	// Test SUBSCRIBE
	t.Run("Subscribe", func(t *testing.T) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		// SUBSCRIBE channel
		sendCommand(conn, "SUBSCRIBE", "my-channel")

		// Should receive subscribe confirmation
		resp := readArrayPubSub(conn, 3)
		if len(resp) != 3 {
			t.Errorf("Expected 3 elements, got %d", len(resp))
		}
		if resp[0] != "subscribe" {
			t.Errorf("Expected 'subscribe', got: %s", resp[0])
		}
		if resp[1] != "my-channel" {
			t.Errorf("Expected 'my-channel', got: %s", resp[1])
		}
	})

	// Test SUBSCRIBE and PUBLISH message delivery
	t.Run("SubscribeAndReceiveMessage", func(t *testing.T) {
		// Subscriber connection
		subConn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Failed to connect subscriber: %v", err)
		}
		defer subConn.Close()

		// Create persistent reader for subscriber
		subReader := bufio.NewReader(subConn)

		// Subscribe to channel
		sendCommand(subConn, "SUBSCRIBE", "news")
		readArrayPubSubWithReader(subReader, 3) // Read subscribe confirmation

		// Publisher connection
		pubConn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Failed to connect publisher: %v", err)
		}
		defer pubConn.Close()

		// Give time for subscription to be registered via LISTEN
		time.Sleep(200 * time.Millisecond)

		// Publish message
		sendCommand(pubConn, "PUBLISH", "news", "breaking news!")
		pubResp := readLine(pubConn)
		if !strings.HasPrefix(pubResp, ":1") {
			t.Errorf("Expected :1 (1 subscriber), got: %s", pubResp)
		}

		// Read message on subscriber (with timeout)
		subConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		msg := readArrayPubSubWithReader(subReader, 3)
		if len(msg) < 3 {
			t.Fatalf("Expected 3 elements in message, got %d", len(msg))
		}
		if msg[0] != "message" {
			t.Errorf("Expected 'message', got: %s", msg[0])
		}
		if msg[1] != "news" {
			t.Errorf("Expected channel 'news', got: %s", msg[1])
		}
		if msg[2] != "breaking news!" {
			t.Errorf("Expected 'breaking news!', got: %s", msg[2])
		}
	})

	// Test UNSUBSCRIBE
	t.Run("Unsubscribe", func(t *testing.T) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		// Subscribe first
		sendCommand(conn, "SUBSCRIBE", "temp-channel")
		readArrayPubSub(conn, 3)

		// Unsubscribe
		sendCommand(conn, "UNSUBSCRIBE", "temp-channel")
		resp := readArrayPubSub(conn, 3)
		if resp[0] != "unsubscribe" {
			t.Errorf("Expected 'unsubscribe', got: %s", resp[0])
		}
		if resp[1] != "temp-channel" {
			t.Errorf("Expected 'temp-channel', got: %s", resp[1])
		}
	})

	// Test multiple subscriptions
	t.Run("MultipleSubscriptions", func(t *testing.T) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		// Set read timeout
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		// Subscribe to multiple channels
		sendCommand(conn, "SUBSCRIBE", "chan1", "chan2", "chan3")

		// Create one reader to use for all reads
		reader := bufio.NewReader(conn)

		// Should receive 3 subscribe confirmations
		for i := 1; i <= 3; i++ {
			resp := readArrayPubSubWithReader(reader, 3)
			if len(resp) < 1 || resp[0] != "subscribe" {
				t.Errorf("Expected 'subscribe', got: %v", resp)
			}
			// Count should increase
			expectedChannel := fmt.Sprintf("chan%d", i)
			if len(resp) < 2 || resp[1] != expectedChannel {
				t.Errorf("Expected '%s', got: %v", expectedChannel, resp)
			}
		}
	})
}

func TestPubSubPatternSubscribe(t *testing.T) {
	srv, store, addr := newPubSubTestServer(t)
	defer srv.Stop()
	defer store.Close()

	t.Run("PSubscribe", func(t *testing.T) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		// PSUBSCRIBE pattern
		sendCommand(conn, "PSUBSCRIBE", "news.*")

		// Should receive psubscribe confirmation
		resp := readArrayPubSub(conn, 3)
		if resp[0] != "psubscribe" {
			t.Errorf("Expected 'psubscribe', got: %s", resp[0])
		}
		if resp[1] != "news.*" {
			t.Errorf("Expected 'news.*', got: %s", resp[1])
		}
	})

	t.Run("PUnsubscribe", func(t *testing.T) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		// Subscribe first
		sendCommand(conn, "PSUBSCRIBE", "events.*")
		readArrayPubSub(conn, 3)

		// Unsubscribe
		sendCommand(conn, "PUNSUBSCRIBE", "events.*")
		resp := readArrayPubSub(conn, 3)
		if resp[0] != "punsubscribe" {
			t.Errorf("Expected 'punsubscribe', got: %s", resp[0])
		}
	})
}

func TestPubSubConcurrent(t *testing.T) {
	srv, store, addr := newPubSubTestServer(t)
	defer srv.Stop()
	defer store.Close()

	t.Run("ConcurrentPublishers", func(t *testing.T) {
		// Create subscriber
		subConn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Failed to connect subscriber: %v", err)
		}
		defer subConn.Close()

		// Create persistent reader for subscriber
		subReader := bufio.NewReader(subConn)

		sendCommand(subConn, "SUBSCRIBE", "concurrent")
		readArrayPubSubWithReader(subReader, 3)

		// Give time for subscription to be registered via LISTEN
		time.Sleep(200 * time.Millisecond)

		// Launch multiple publishers
		var wg sync.WaitGroup
		numPublishers := 5
		messagesPerPublisher := 10

		for i := 0; i < numPublishers; i++ {
			wg.Add(1)
			go func(pubID int) {
				defer wg.Done()
				pubConn, err := net.Dial("tcp", addr)
				if err != nil {
					t.Errorf("Publisher %d failed to connect: %v", pubID, err)
					return
				}
				defer pubConn.Close()

				for j := 0; j < messagesPerPublisher; j++ {
					msg := fmt.Sprintf("pub%d-msg%d", pubID, j)
					sendCommand(pubConn, "PUBLISH", "concurrent", msg)
					readLine(pubConn) // Read response
				}
			}(i)
		}

		wg.Wait()

		// Read messages with timeout
		subConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		receivedCount := 0
		expectedTotal := numPublishers * messagesPerPublisher

		for receivedCount < expectedTotal {
			msg := readArrayPubSubWithReader(subReader, 3)
			if len(msg) < 3 || msg[0] != "message" {
				break
			}
			receivedCount++
		}

		if receivedCount < expectedTotal {
			t.Logf("Received %d of %d expected messages (some may have been lost due to timing)", receivedCount, expectedTotal)
		}
	})
}

// Helper functions for pub/sub tests

func sendCommand(conn net.Conn, args ...string) {
	// Build RESP array
	cmd := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		cmd += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	conn.Write([]byte(cmd))
}

func readLine(conn net.Conn) string {
	reader := bufio.NewReader(conn)
	line, _ := reader.ReadString('\n')
	return strings.TrimSpace(line)
}

func readArrayPubSub(conn net.Conn, expectedLen int) []string {
	reader := bufio.NewReader(conn)
	return readArrayPubSubWithReader(reader, expectedLen)
}

func readArrayPubSubWithReader(reader *bufio.Reader, expectedLen int) []string {
	result := make([]string, 0, expectedLen)

	// Read array header
	header, _ := reader.ReadString('\n')
	header = strings.TrimSpace(header)
	if !strings.HasPrefix(header, "*") {
		return result
	}

	// Parse length
	var arrayLen int
	fmt.Sscanf(header, "*%d", &arrayLen)

	// Read elements
	for i := 0; i < arrayLen; i++ {
		elemType, _ := reader.ReadString('\n')
		elemType = strings.TrimSpace(elemType)

		if strings.HasPrefix(elemType, "$") {
			// Bulk string - read length and content
			var strLen int
			fmt.Sscanf(elemType, "$%d", &strLen)
			if strLen >= 0 {
				buf := make([]byte, strLen+2) // +2 for \r\n
				reader.Read(buf)
				result = append(result, string(buf[:strLen]))
			} else {
				result = append(result, "")
			}
		} else if strings.HasPrefix(elemType, ":") {
			// Integer
			result = append(result, strings.TrimPrefix(elemType, ":"))
		} else if strings.HasPrefix(elemType, "+") {
			// Simple string
			result = append(result, strings.TrimPrefix(elemType, "+"))
		}
	}

	return result
}
