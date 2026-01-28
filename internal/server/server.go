// Package server provides the TCP server for Redis protocol.
package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/mnorrsken/pg-kv-backend/internal/handler"
	"github.com/mnorrsken/pg-kv-backend/internal/resp"
)

// Server represents a Redis-compatible server
type Server struct {
	addr     string
	handler  *handler.Handler
	listener net.Listener
	quit     chan struct{}
	wg       sync.WaitGroup
}

// New creates a new server
func New(addr string, h *handler.Handler) *Server {
	return &Server{
		addr:    addr,
		handler: h,
		quit:    make(chan struct{}),
	}
}

// Start starts the server
func (s *Server) Start(ctx context.Context) error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	log.Printf("Server listening on %s", s.addr)

	go s.acceptLoop(ctx)

	return nil
}

// Stop gracefully stops the server
func (s *Server) Stop() {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}

func (s *Server) acceptLoop(ctx context.Context) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(ctx, conn)
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	// Track authentication state for this connection
	authenticated := !s.handler.RequiresAuth()

	for {
		select {
		case <-s.quit:
			return
		case <-ctx.Done():
			return
		default:
		}

		// Read command
		cmd, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("Read error: %v", err)
			return
		}

		// Check authentication before processing commands
		var response resp.Value
		if cmd.Type == resp.Array && len(cmd.Array) > 0 {
			cmdName := strings.ToUpper(cmd.Array[0].Bulk)

			// Handle AUTH command specially
			if cmdName == "AUTH" {
				response = s.handler.Handle(ctx, cmd)
				if response.Type == resp.SimpleString && response.Str == "OK" {
					authenticated = true
				}
			} else if !authenticated {
				// Allow only PING, QUIT, and COMMAND without auth
				if cmdName == "PING" || cmdName == "QUIT" || cmdName == "COMMAND" {
					response = s.handler.Handle(ctx, cmd)
				} else {
					response = resp.Value{Type: resp.Error, Str: "NOAUTH Authentication required."}
				}
			} else {
				response = s.handler.Handle(ctx, cmd)
			}
		} else {
			response = s.handler.Handle(ctx, cmd)
		}

		// Write response
		if err := writer.WriteValue(response); err != nil {
			log.Printf("Write error: %v", err)
			return
		}
		if err := writer.Flush(); err != nil {
			log.Printf("Flush error: %v", err)
			return
		}
	}
}
