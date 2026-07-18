package aegis

import (
	"fmt"
	"net"
	"sync"
)

// Server represents the high-performance Aegis TCP network engine.
type Server struct {
	config   *Config
	listener net.Listener
	wg       sync.WaitGroup
	quit     chan struct{}
}

// NewServer initializes a new Server instance with the given configuration.
func NewServer(cfg *Config) *Server {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	return &Server{
		config: cfg,
		quit:   make(chan struct{}),
	}
}

// Start binds to the configured network address and begins listening for incoming nodes.
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("aegis-server: failed to bind address %s: %w", s.config.Address, err)
	}
	s.listener = l

	fmt.Printf("[AEGIS-SERVER] Engine successfully running on %s\n", s.config.Address)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Check if the server was intentionally stopped via the quit channel
			select {
			case <-s.quit:
				return nil
			default:
				fmt.Printf("[AEGIS-SERVER] Connection acceptance failure: %v\n", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection manages the lifecycle of an individual connected node.
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer func() { _ = conn.Close() }()

	fmt.Printf("[AEGIS-SERVER] Inbound node connected from: %s\n", conn.RemoteAddr().String())

	// v0.1: Send a basic protocol-level connection acknowledgment and close
	_, err := conn.Write([]byte("AEGIS_CORE_v0.1:READY\n"))
	if err != nil {
		fmt.Printf("[AEGIS-SERVER] Failed to write handshake confirmation to %s: %v\n", conn.RemoteAddr().String(), err)
	}
}

// Stop gracefully terminates the server listener and waits for active connections to finish.
func (s *Server) Stop() error {
	close(s.quit)
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
	fmt.Println("[AEGIS-SERVER] Engine shutdown complete. Clean state preserved.")
	return nil
}
