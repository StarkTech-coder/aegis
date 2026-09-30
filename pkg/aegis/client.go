package aegis

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// Client represents the Aegis network engine client for outbound node connections.
type Client struct {
	config *Config

	mu      sync.Mutex
	nextSeq uint32
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

		return conn, nil

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
		return c.write(ctx, conn, data)

	case ProtocolUDP:
		if len(data) > maxUDPDatagramSize {
			return fmt.Errorf(
				"%w: UDP frame size %d exceeds datagram limit",
				ErrPacketTooLarge,
				len(data),
			)
		}

		return c.write(ctx, conn, data)

	case ProtocolRUDP:
		return c.sendRUDP(ctx, conn, data)

	default:
		return fmt.Errorf(
			"aegis-client: unsupported protocol %q",
			c.config.Protocol,
		)
	}
}

func (c *Client) write(
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

	if _, err := conn.Write(data); err != nil {
		if isTimeout(err) {
			return fmt.Errorf("%w: %v", ErrTimeout, err)
		}

		return fmt.Errorf(
			"aegis-client: failed to write packet: %w",
			err,
		)
	}

	return nil
}

func (c *Client) sendRUDP(
	ctx context.Context,
	conn net.Conn,
	data []byte,
) error {
	if ctx == nil {
		ctx = context.Background()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.nextSeq++
	seq := c.nextSeq

	frame := encodeRUDPData(seq, data)

	if len(frame) > maxUDPDatagramSize {
		return fmt.Errorf(
			"%w: RUDP frame size %d exceeds UDP limit",
			ErrPacketTooLarge,
			len(frame),
		)
	}

	deadline := time.Now().Add(c.config.WriteTimeout)

	if ctxDeadline, ok := ctx.Deadline(); ok &&
		ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	for attempt := 0; attempt < rudpMaxRetries; attempt++ {
		if err := conn.SetWriteDeadline(deadline); err != nil {
			return fmt.Errorf(
				"aegis-client: failed to set RUDP write deadline: %w",
				err,
			)
		}

		if _, err := conn.Write(frame); err != nil {
			if isTimeout(err) {
				continue
			}

			return fmt.Errorf(
				"aegis-client: failed to send RUDP frame: %w",
				err,
			)
		}

		ack, err := readRUDPAck(
			ctx,
			conn,
			c.config.ReadTimeout,
		)

		if err == nil && ack == seq {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	return fmt.Errorf(
		"%w: RUDP acknowledgement timeout for sequence %d",
		ErrTimeout,
		seq,
	)
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

func isTimeout(err error) bool {
	if err == nil {
		return false
	}

	netErr, ok := err.(net.Error)
	return ok && netErr.Timeout()
}