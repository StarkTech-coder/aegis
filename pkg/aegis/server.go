package aegis

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

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

	s.logger.Info("Inbound node connected successfully", "remote_addr", conn.RemoteAddr().String())

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

		// Senior Katmani: Stream-safe frame decoding using io.ReadFull instead of raw byte slicing
		packet, err := ReadPacket(conn)
		if err != nil {
			if err == io.EOF {
				s.logger.Info("Remote node closed connection gracefully", "remote_addr", conn.RemoteAddr().String())
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

		s.logger.Info("Valid Aegis Binary Protocol packet processed via Frame Decoder",
			"remote_addr", conn.RemoteAddr().String(),
			"payload_len", packet.Length,
			"checksum", packet.Checksum,
			"data", string(packet.Payload),
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
