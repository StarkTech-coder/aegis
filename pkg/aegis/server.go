package aegis

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
)

// PacketHandler defines the callback used for decoded packet payloads.
type PacketHandler interface {
	HandlePacket(payload []byte) error
}

// PacketHandlerFunc adapts a function to PacketHandler.
type PacketHandlerFunc func(payload []byte) error

// HandlePacket invokes the wrapped packet handler function.
func (f PacketHandlerFunc) HandlePacket(payload []byte) error {
	if f == nil {
		return nil
	}

	return f(payload)
}

// Server represents the Aegis network engine.
type Server struct {
	config    *Config
	transport Transport

	wg        sync.WaitGroup
	quit      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	logger    *slog.Logger
	semaphore chan struct{}
	handler   PacketHandler

	stopOnce sync.Once
}

// NewServer creates a server with a validated configuration.
func NewServer(cfg *Config) (*Server, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	} else {
		cfg = cfg.clone()
	}

	cfg.sanitize()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf(
			"aegis-server: invalid configuration: %w",
			err,
		)
	}

	logger := slog.New(
		slog.NewTextHandler(
			os.Stdout,
			&slog.HandlerOptions{
				Level: slog.LevelInfo,
			},
		),
	)

	baseCtx, cancel := context.WithCancel(
		context.Background(),
	)

	server := &Server{
		config:    cfg,
		quit:      make(chan struct{}),
		ctx:       baseCtx,
		cancel:    cancel,
		logger:    logger,
		semaphore: make(chan struct{}, cfg.MaxConnections),
	}

	switch cfg.Protocol {
	case ProtocolTCP:
		server.transport = NewTCPTransport(server)

	case ProtocolUDP:
		server.transport = NewUDPTransport(server)

	case ProtocolRUDP:
		server.transport = NewRUDPTransport(server)
	}

	return server, nil
}

// Config returns a copy of the server configuration.
func (s *Server) Config() Config {
	if s == nil || s.config == nil {
		return Config{}
	}

	return *s.config
}

// SetHandler assigns a packet handler to the server.
func (s *Server) SetHandler(h PacketHandler) {
	if s == nil {
		return
	}

	s.handler = h
}

// OnPacket assigns a function as the packet handler.
func (s *Server) OnPacket(
	fn func(payload []byte) error,
) {
	if s == nil {
		return
	}

	s.handler = PacketHandlerFunc(fn)
}

// Start starts the configured network transport.
func (s *Server) Start() error {
	if s == nil || s.transport == nil {
		return fmt.Errorf(
			"%w: server is not initialized",
			ErrServerClosed,
		)
	}

	if s.ctx.Err() != nil {
		return ErrServerClosed
	}

	return s.transport.Start(s.ctx)
}

// Stop gracefully stops the server.
func (s *Server) Stop() error {
	if s == nil {
		return nil
	}

	s.stopOnce.Do(func() {
		s.logger.Info(
			"Initiating graceful engine shutdown sequence",
		)

		close(s.quit)
		s.cancel()

		if s.transport != nil {
			if err := s.transport.Close(); err != nil {
				s.logger.Warn(
					"Transport close returned an error",
					"error",
					err,
				)
			}
		}
	})

	s.wg.Wait()

	return nil
}