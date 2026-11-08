package aegis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Client represents the Aegis network engine client for outbound node connections.
type Client struct {
	config *Config

	mu      sync.Mutex
	nextSeq uint32

	tcpConnGuards sync.Map
	rudpSessions  sync.Map
}

type tcpConnState struct {
	readMu  sync.Mutex
	writeMu sync.Mutex
}

type managedTCPConn struct {
	net.Conn
	owner *Client
}

type managedRUDPConn struct {
	net.Conn
	owner *Client
}

func (c *managedTCPConn) Close() error {
	if c.owner != nil {
		c.owner.deleteTCPConnState(c)
	}

	if c.Conn == nil {
		return nil
	}

	return c.Conn.Close()
}

func (c *managedRUDPConn) Close() error {
	if c.owner != nil {
		c.owner.deleteRUDPSession(c)
	}

	if c.Conn == nil {
		return nil
	}

	return c.Conn.Close()
}

// NewClient creates a client with a validated configuration.
func NewClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	} else {
		cfg = cfg.clone()
	}

	cfg.sanitize()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("aegis-client: invalid configuration: %w", err)
	}

	return &Client{
		config: cfg,
	}, nil
}

// Config returns a copy of the client's configuration.
func (c *Client) Config() Config {
	if c == nil || c.config == nil {
		return Config{}
	}

	return *c.config
}

// Connect establishes an outbound connection using the configured protocol.
func (c *Client) Connect(targetAddress string) (net.Conn, error) {
	if c == nil || c.config == nil {
		return nil, fmt.Errorf("aegis-client: connection failed: client configuration is nil")
	}

	if targetAddress == "" {
		targetAddress = c.config.Address
	}

	switch c.config.Protocol {
	case ProtocolTCP:
		conn, err := net.DialTimeout(
			"tcp",
			targetAddress,
			c.config.ConnectionTimeout,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"aegis-client: failed to connect to target node %s: %w",
				targetAddress,
				err,
			)
		}

		return &managedTCPConn{
			Conn:  conn,
			owner: c,
		}, nil

	case ProtocolUDP, ProtocolRUDP:
		raddr, err := net.ResolveUDPAddr("udp", targetAddress)
		if err != nil {
			return nil, fmt.Errorf(
				"aegis-client: failed to resolve UDP target address %s: %w",
				targetAddress,
				err,
			)
		}

		conn, err := net.DialUDP("udp", nil, raddr)
		if err != nil {
			return nil, fmt.Errorf(
				"aegis-client: failed to dial UDP target node %s: %w",
				targetAddress,
				err,
			)
		}

		if c.config.Protocol == ProtocolRUDP {
			return &managedRUDPConn{
				Conn:  conn,
				owner: c,
			}, nil
		}

		return conn, nil

	default:
		return nil, fmt.Errorf(
			"aegis-client: unsupported protocol %q",
			c.config.Protocol,
		)
	}
}

// SendPacket serializes and sends a packet over an established connection.
func (c *Client) SendPacket(
	ctx context.Context,
	conn net.Conn,
	payload []byte,
) error {
	if c == nil || c.config == nil {
		return fmt.Errorf("aegis-client: client configuration is nil")
	}

	if conn == nil {
		return fmt.Errorf("aegis-client: connection is nil")
	}

	if uint64(len(payload)) > uint64(c.config.MaxPayloadSize) {
		return fmt.Errorf(
			"%w: payload size %d exceeds configured limit %d",
			ErrPacketTooLarge,
			len(payload),
			c.config.MaxPayloadSize,
		)
	}

	packet := NewPacket(payload)

	data, err := packet.Serialize()
	if err != nil {
		return fmt.Errorf(
			"aegis-client: failed to serialize packet: %w",
			err,
		)
	}

	switch c.config.Protocol {
	case ProtocolTCP:
		connState := c.getTCPConnState(conn)
		connState.writeMu.Lock()
		defer connState.writeMu.Unlock()

		err := c.writeTCPStream(ctx, conn, data)
		if isConnectionClosedError(err) {
			c.deleteTCPConnState(conn)
		}

		return err

	case ProtocolUDP:
		if len(data) > maxUDPDatagramSize {
			return fmt.Errorf(
				"%w: UDP frame size %d exceeds datagram limit",
				ErrPacketTooLarge,
				len(data),
			)
		}

		return c.writeDatagram(ctx, conn, data)

	case ProtocolRUDP:
		return c.sendRUDP(ctx, conn, data)

	default:
		return fmt.Errorf(
			"aegis-client: unsupported protocol %q",
			c.config.Protocol,
		)
	}
}

// ReceivePacket reads and decodes a full packet payload from an established connection.
func (c *Client) ReceivePacket(
	ctx context.Context,
	conn net.Conn,
) ([]byte, error) {
	if c == nil || c.config == nil {
		return nil, fmt.Errorf("aegis-client: client configuration is nil")
	}

	if conn == nil {
		return nil, fmt.Errorf("aegis-client: connection is nil")
	}

	switch c.config.Protocol {
	case ProtocolTCP:
		connState := c.getTCPConnState(conn)
		connState.readMu.Lock()
		defer connState.readMu.Unlock()

		payload, err := c.readTCPStreamPacket(ctx, conn)
		if isConnectionClosedError(err) {
			c.deleteTCPConnState(conn)
		}

		return payload, err

	case ProtocolUDP:
		return c.readUDPDatagramPacket(ctx, conn)

	case ProtocolRUDP:
		return c.receiveRUDP(ctx, conn)

	default:
		return nil, fmt.Errorf(
			"aegis-client: receive is unsupported for protocol %q",
			c.config.Protocol,
		)
	}
}

func (c *Client) writeDatagram(
	ctx context.Context,
	conn net.Conn,
	data []byte,
) error {
	if err := setWriteDeadline(
		ctx,
		conn,
		c.config.WriteTimeout,
	); err != nil {
		return err
	}

	n, err := conn.Write(data)
	if err != nil {
		if isTimeout(err) {
			if ctx != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
			}

			return fmt.Errorf("%w: %v", ErrTimeout, err)
		}

		return fmt.Errorf(
			"aegis-client: failed to write packet: %w",
			err,
		)
	}

	if n != len(data) {
		return fmt.Errorf(
			"aegis-client: failed to write packet: %w",
			io.ErrShortWrite,
		)
	}

	return nil
}

func (c *Client) readUDPDatagramPacket(
	ctx context.Context,
	conn net.Conn,
) ([]byte, error) {
	stopCancelWatcher := startReadCancelWatcher(
		ctx,
		conn,
	)
	defer stopCancelWatcher()

	if err := setReadDeadline(
		ctx,
		conn,
		c.config.ReadTimeout,
	); err != nil {
		if isConnectionClosedError(err) {
			return nil, fmt.Errorf("%w: %v", ErrConnectionClosed, err)
		}

		return nil, err
	}

	buffer := make(
		[]byte,
		maxUDPDatagramSize+1,
	)

	n, err := conn.Read(buffer)
	if err != nil {
		if isTimeout(err) {
			if ctx != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, ctxErr
				}
			}

			return nil, fmt.Errorf("%w: %v", ErrTimeout, err)
		}

		if isConnectionClosedError(err) {
			return nil, fmt.Errorf("%w: %v", ErrConnectionClosed, err)
		}

		return nil, fmt.Errorf(
			"aegis-client: failed to read UDP datagram: %w",
			err,
		)
	}

	if n == 0 {
		return nil, fmt.Errorf(
			"%w: empty UDP datagram",
			ErrInvalidPacket,
		)
	}

	if n > maxUDPDatagramSize {
		return nil, fmt.Errorf(
			"%w: UDP datagram size %d exceeds limit %d",
			ErrPacketTooLarge,
			n,
			maxUDPDatagramSize,
		)
	}

	packet, err := Deserialize(
		buffer[:n],
		c.config.MaxPayloadSize,
	)
	if err != nil {
		return nil, err
	}

	return packet.Payload, nil
}

func (c *Client) readTCPStreamPacket(
	ctx context.Context,
	conn net.Conn,
) ([]byte, error) {
	stopCancelWatcher := startReadCancelWatcher(
		ctx,
		conn,
	)
	defer stopCancelWatcher()

	if err := setReadDeadline(
		ctx,
		conn,
		c.config.ReadTimeout,
	); err != nil {
		if classifiedErr := classifyStreamReadError(err); classifiedErr != nil {
			return nil, classifiedErr
		}

		return nil, err
	}

	packet, err := ReadPacket(
		conn,
		c.config.MaxPayloadSize,
	)
	if err != nil {
		if isTimeout(err) {
			if ctx != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, ctxErr
				}
			}

			return nil, fmt.Errorf("%w: %v", ErrTimeout, err)
		}

		return nil, err
	}

	return packet.Payload, nil
}

func (c *Client) writeTCPStream(
	ctx context.Context,
	conn net.Conn,
	data []byte,
) error {
	stopCancelWatcher := startWriteCancelWatcher(
		ctx,
		conn,
	)
	defer stopCancelWatcher()

	if err := setWriteDeadline(
		ctx,
		conn,
		c.config.WriteTimeout,
	); err != nil {
		return err
	}

	written := 0

	for written < len(data) {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}

		n, err := conn.Write(data[written:])
		if n > 0 {
			written += n
		}

		if err != nil {
			if isTimeout(err) {
				if ctx != nil {
					if ctxErr := ctx.Err(); ctxErr != nil {
						return ctxErr
					}
				}

				return fmt.Errorf("%w: %v", ErrTimeout, err)
			}

			return fmt.Errorf(
				"aegis-client: failed to write packet: %w",
				err,
			)
		}

		if n == 0 {
			return fmt.Errorf(
				"aegis-client: failed to write packet: %w",
				io.ErrShortWrite,
			)
		}
	}

	return nil
}

func startWriteCancelWatcher(
	ctx context.Context,
	conn net.Conn,
) func() {
	if ctx == nil || conn == nil {
		return func() {}
	}

	done := ctx.Done()
	if done == nil {
		return func() {}
	}

	stop := make(chan struct{})

	go func() {
		select {
		case <-done:
			_ = conn.SetWriteDeadline(time.Now())

		case <-stop:
		}
	}()

	return func() {
		close(stop)
	}
}

func (c *Client) getTCPConnState(conn net.Conn) *tcpConnState {
	if c == nil || conn == nil {
		return &tcpConnState{}
	}

	state, ok := c.tcpConnGuards.Load(conn)
	if ok {
		return state.(*tcpConnState)
	}

	newState := &tcpConnState{}
	actual, _ := c.tcpConnGuards.LoadOrStore(conn, newState)
	return actual.(*tcpConnState)
}

func (c *Client) deleteTCPConnState(conn net.Conn) {
	if c == nil || conn == nil {
		return
	}

	c.tcpConnGuards.Delete(conn)
}

func startReadCancelWatcher(
	ctx context.Context,
	conn net.Conn,
) func() {
	if ctx == nil || conn == nil {
		return func() {}
	}

	done := ctx.Done()
	if done == nil {
		return func() {}
	}

	stop := make(chan struct{})

	go func() {
		select {
		case <-done:
			_ = conn.SetReadDeadline(time.Now())

		case <-stop:
		}
	}()

	return func() {
		close(stop)
	}
}

func (c *Client) sendRUDP(
	ctx context.Context,
	conn net.Conn,
	data []byte,
) error {
	return c.sendRUDPWithSession(ctx, conn, data)
}

func setWriteDeadline(
	ctx context.Context,
	conn net.Conn,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)

	if ctx != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if ctxDeadline, ok := ctx.Deadline(); ok &&
			ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}
	}

	return conn.SetWriteDeadline(deadline)
}

func setReadDeadline(
	ctx context.Context,
	conn net.Conn,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)

	if ctx != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if ctxDeadline, ok := ctx.Deadline(); ok &&
			ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}
	}

	return conn.SetReadDeadline(deadline)
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}

	netErr, ok := err.(net.Error)
	return ok && netErr.Timeout()
}

func isConnectionClosedError(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, ErrConnectionClosed) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.ErrClosedPipe)
}
