package aegis

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

// PacketHandler defines the event contract for processing incoming binary payloads.
type PacketHandler interface {
	HandlePacket(payload []byte) error
}

// PacketHandlerFunc adapts a standard signature function into a PacketHandler interface.
type PacketHandlerFunc func(payload []byte) error

// HandlePacket invokes the underlying handler function.
func (f PacketHandlerFunc) HandlePacket(payload []byte) error {
	return f(payload)
}

// Server represents the high-performance, enterprise-grade Aegis TCP network engine.
type Server struct {
	config    *Config
	listener  net.Listener
	wg        sync.WaitGroup
	quit      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	logger    *slog.Logger
	semaphore chan struct{} // Connection Manager: Semaphore pool bound strictly to dynamic config values
	handler   PacketHandler // Decoupled packet handler bridge
}

// NewServer initializes a new Server instance after validating config and setting up enterprise components.
func NewServer(cfg *Config) (*Server, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	cfg.sanitize()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("aegis-server: invalid configuration: %w", err)
	}

	// Initialize structured logging (slog) targeting standard output
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Create a root context to elegantly manage component cancellation lifecycles
	baseCtx, cancel := context.WithCancel(context.Background())

	return &Server{
		config:    cfg,
		quit:      make(chan struct{}),
		ctx:       baseCtx,
		cancel:    cancel,
		logger:    logger,
		semaphore: make(chan struct{}, cfg.MaxConnections), // Dynamic boundary allocation
	}, nil
}

// SetHandler assigns a PacketHandler interface to process decoded frame payloads.
func (s *Server) SetHandler(h PacketHandler) {
	s.handler = h
}

// OnPacket registers a standalone function handler to process decoded frame payloads.
func (s *Server) OnPacket(fn func(payload []byte) error) {
	s.handler = PacketHandlerFunc(fn)
}

// Start binds to the configured network address and listens for incoming nodes using context lifecycles.
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		s.logger.Error("Failed to bind network socket address", "address", s.config.Address, "error", err)
		return fmt.Errorf("aegis-server: failed to bind address %s: %w", s.config.Address, err)
	}
	s.listener = l

	s.logger.Info("Aegis Network Engine successfully running", "address", s.config.Address, "max_conn", s.config.MaxConnections)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil
			default:
				s.logger.Warn("Connection acceptance failure occurred", "error", err)
				continue
			}
		}

		// Connection Manager Safeguard: Enforce dynamic traffic ceiling check
		select {
		case s.semaphore <- struct{}{}:
			s.wg.Add(1)
			go s.handleConnection(s.ctx, conn)
		case <-s.quit:
			_ = conn.Close()
			return nil
		default:
			// Active resource exhaustion protection triggered dynamically based on current configuration limits
			s.logger.Warn("Max connection limit reached. Dropping inbound connection instantly", "remote_addr", conn.RemoteAddr().String())
			_ = conn.Close()
		}
	}
}

// handleConnection manages the stream processing lifecycle of an individual connected node under context awareness.
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		_ = conn.Close()
		<-s.semaphore // Release token back to the dynamic semaphore pool upon node exit
	}()

	s.logger.Debug("Inbound node connected successfully", "remote_addr", conn.RemoteAddr().String())

	// 64KB High-Throughput Buffer: Reduces kernel syscalls by batching TCP stream reads
	reader := bufio.NewReaderSize(conn, 65536)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Context cancellation signal received. Terminating connection handler gracefully", "remote_addr", conn.RemoteAddr().String())
			return
		default:
		}

		if s.config.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		}

		// Read frame using buffered reader instead of raw connection
		packet, err := ReadPacket(reader)
		if err != nil {
			if err == io.EOF {
				s.logger.Debug("Remote node closed connection gracefully", "remote_addr", conn.RemoteAddr().String())
				break
			}
			// Handle net.Error for timeout operations elegantly
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				s.logger.Debug("Network operation timed out", "remote_addr", conn.RemoteAddr().String())
				break
			}
			s.logger.Error("Error parsing incoming TCP stream frame", "remote_addr", conn.RemoteAddr().String(), "error", err)
			break
		}

		// Dispatch packet payload to registered consumer handler if configured
		if s.handler != nil {
			if err := s.handler.HandlePacket(packet.Payload); err != nil {
				s.logger.Error("Execution error encountered in payload handler", "remote_addr", conn.RemoteAddr().String(), "error", err)
			}
		}

		// Reduced log level to DEBUG during ultra-high throughput load tests
		s.logger.Debug("Valid Aegis Binary Protocol packet processed via Frame Decoder",
			"remote_addr", conn.RemoteAddr().String(),
			"payload_len", packet.Length,
			"checksum", packet.Checksum,
		)
	}
}

// Stop gracefully terminates the server listener, triggers context cancellation, and flushes connection pools.
func (s *Server) Stop() error {
	s.logger.Info("Initiating graceful engine shutdown sequence...")

	close(s.quit)
	s.cancel()

	if s.listener != nil {
		_ = s.listener.Close()
	}

	s.wg.Wait()
	s.logger.Info("Aegis Engine shutdown complete. Clean state preserved.")
	return nil
}
