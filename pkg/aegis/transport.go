package aegis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

// Transport abstracts the underlying network transport.
type Transport interface {
	Start(ctx context.Context) error
	Close() error
}

// TCPTransport provides reliable stream transport over TCP.
type TCPTransport struct {
	server   *Server
	listener net.Listener
}

// NewTCPTransport creates a TCP transport for the server.
func NewTCPTransport(s *Server) *TCPTransport {
	return &TCPTransport{
		server: s,
	}
}

// Start binds the TCP socket and accepts incoming connections.
func (t *TCPTransport) Start(ctx context.Context) error {
	if t == nil || t.server == nil {
		return fmt.Errorf(
			"aegis-tcp: server is nil",
		)
	}

	if ctx.Err() != nil {
		return ErrServerClosed
	}

	listener, err := net.Listen(
		"tcp",
		t.server.config.Address,
	)
	if err != nil {
		t.server.logger.Error(
			"Failed to bind TCP socket",
			"address",
			t.server.config.Address,
			"error",
			err,
		)

		return fmt.Errorf(
			"aegis-tcp: failed to bind address %s: %w",
			t.server.config.Address,
			err,
		)
	}

	t.listener = listener

	defer func() {
		_ = listener.Close()
	}()

	t.server.logger.Info(
		"Aegis TCP Engine successfully running",
		"address",
		t.server.config.Address,
		"max_conn",
		t.server.config.MaxConnections,
	)

	for {
		conn, err := listener.Accept()

		if err != nil {
			if isClosedNetworkError(err) {
				return nil
			}

			if ctx.Err() != nil {
				return nil
			}

			t.server.logger.Warn(
				"TCP connection acceptance failed",
				"error",
				err,
			)

			continue
		}

		select {
		case t.server.semaphore <- struct{}{}:
			t.server.wg.Add(1)

			go t.handleTCPConnection(
				ctx,
				conn,
			)

		case <-ctx.Done():
			_ = conn.Close()
			return nil

		case <-t.server.quit:
			_ = conn.Close()
			return nil

		default:
			t.server.logger.Warn(
				"Max connection limit reached",
				"remote_addr",
				conn.RemoteAddr().String(),
			)

			_ = conn.Close()
		}
	}
}

func (t *TCPTransport) handleTCPConnection(
	ctx context.Context,
	conn net.Conn,
) {
	defer t.server.wg.Done()

	defer func() {
		_ = conn.Close()
		<-t.server.semaphore
	}()

	remoteAddr := conn.RemoteAddr().String()

	t.server.logger.Debug(
		"Inbound TCP node connected",
		"remote_addr",
		remoteAddr,
	)

	reader := bufio.NewReaderSize(
		conn,
		64*1024,
	)

	for {
		select {
		case <-ctx.Done():
			return

		default:
		}

		if t.server.config.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(
				time.Now().Add(
					t.server.config.ReadTimeout,
				),
			)
		}

		packet, err := ReadPacket(
			reader,
			t.server.config.MaxPayloadSize,
		)

		if err != nil {
			switch {
			case errors.Is(err, ErrConnectionClosed):
				t.server.logger.Debug(
					"TCP connection closed",
					"remote_addr",
					remoteAddr,
				)

			case isTimeout(err):
				t.server.logger.Debug(
					"TCP read timed out",
					"remote_addr",
					remoteAddr,
				)

			default:
				t.server.logger.Warn(
					"Failed to decode TCP packet",
					"remote_addr",
					remoteAddr,
					"error",
					err,
				)
			}

			return
		}

		if t.server.handler != nil {
			if err := t.server.handler.HandlePacket(
				packet.Payload,
			); err != nil {
				t.server.logger.Error(
					"TCP packet handler failed",
					"remote_addr",
					remoteAddr,
					"error",
					err,
				)
			}
		}

		t.server.logger.Debug(
			"Valid Aegis TCP packet processed",
			"remote_addr",
			remoteAddr,
			"payload_len",
			packet.Length,
			"checksum",
			packet.Checksum,
		)
	}
}

// Close closes the TCP listener.
func (t *TCPTransport) Close() error {
	if t == nil || t.listener == nil {
		return nil
	}

	return t.listener.Close()
}

// UDPTransport provides connectionless datagram transport over UDP.
type UDPTransport struct {
	server *Server
	conn   *net.UDPConn
}

// NewUDPTransport creates a UDP transport for the server.
func NewUDPTransport(s *Server) *UDPTransport {
	return &UDPTransport{
		server: s,
	}
}

// Start binds the UDP socket and processes incoming datagrams.
func (u *UDPTransport) Start(ctx context.Context) error {
	if u == nil || u.server == nil {
		return fmt.Errorf(
			"aegis-udp: server is nil",
		)
	}

	if ctx.Err() != nil {
		return ErrServerClosed
	}

	addr, err := net.ResolveUDPAddr(
		"udp",
		u.server.config.Address,
	)
	if err != nil {
		return fmt.Errorf(
			"aegis-udp: failed to resolve address %s: %w",
			u.server.config.Address,
			err,
		)
	}

	conn, err := net.ListenUDP(
		"udp",
		addr,
	)
	if err != nil {
		return fmt.Errorf(
			"aegis-udp: failed to bind socket: %w",
			err,
		)
	}

	u.conn = conn

	defer func() {
		_ = conn.Close()
	}()

	u.server.logger.Info(
		"Aegis UDP Engine successfully running",
		"address",
		u.server.config.Address,
	)

	buffer := make([]byte, maxUDPDatagramSize)

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-u.server.quit:
			return nil

		default:
		}

		if u.server.config.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(
				time.Now().Add(
					u.server.config.ReadTimeout,
				),
			)
		}

		n, remoteAddr, err := conn.ReadFromUDP(buffer)

		if err != nil {
			if isClosedNetworkError(err) {
				return nil
			}

			if isTimeout(err) {
				continue
			}

			return fmt.Errorf(
				"aegis-udp: failed to read datagram: %w",
				err,
			)
		}

		packet, err := Deserialize(
			buffer[:n],
			u.server.config.MaxPayloadSize,
		)

		if err != nil {
			u.server.logger.Warn(
				"Failed to deserialize UDP packet",
				"remote_addr",
				remoteAddr.String(),
				"error",
				err,
			)

			continue
		}

		if u.server.handler != nil {
			if err := u.server.handler.HandlePacket(
				packet.Payload,
			); err != nil {
				u.server.logger.Error(
					"UDP packet handler failed",
					"remote_addr",
					remoteAddr.String(),
					"error",
					err,
				)
			}
		}

		u.server.logger.Debug(
			"Valid Aegis UDP packet processed",
			"remote_addr",
			remoteAddr.String(),
			"payload_len",
			packet.Length,
			"checksum",
			packet.Checksum,
		)
	}
}

// Close closes the UDP socket.
func (u *UDPTransport) Close() error {
	if u == nil || u.conn == nil {
		return nil
	}

	return u.conn.Close()
}