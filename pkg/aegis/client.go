package aegis

import (
	"fmt"
	"net"
	"time"
)

// Client represents the Aegis network engine client for outbound node connections.
type Client struct {
	config *Config
}

// NewClient initializes a new Client instance after sanitizing and validating the configuration.
func NewClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	// 1. Sanitize input to enforce safe default configuration fallback values
	cfg.sanitize()

	// 2. Validate structural integrity to ensure parameters stay within engine limits
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("aegis-client: invalid configuration: %w", err)
	}

	return &Client{config: cfg}, nil
}

// Config returns a strictly isolated, read-only value copy of the client's internal parameters.
// This prevents external mutations from compromising the running engine's live state (Defensive Copying).
func (c *Client) Config() Config {
	if c == nil || c.config == nil {
		return Config{} // Return an empty config instance instead of panicking on nil pointer dereference
	}
	return *c.config // Go automatically makes a full structural value copy here
}

// Connect establishes a TCP connection to a target node address using pre-configured timeouts.
func (c *Client) Connect(targetAddress string) (net.Conn, error) {
	if c == nil || c.config == nil {
		return nil, fmt.Errorf("aegis-client: connection failed: client configuration is nil")
	}

	if targetAddress == "" {
		targetAddress = c.config.Address
	}

	// 1. Dial Timeout enables our client to stop waiting if the target node is dead
	conn, err := net.DialTimeout("tcp", targetAddress, c.config.ConnectionTimeout)
	if err != nil {
		return nil, fmt.Errorf("aegis-client: failed to connect to target node %s: %w", targetAddress, err)
	}

	// 2. SENIOR TOUCH: Apply socket-level I/O deadlines to prevent hanging connections
	now := time.Now()
	if err := conn.SetReadDeadline(now.Add(c.config.ReadTimeout)); err != nil {
		_ = conn.Close() // Close connection immediately to prevent resource leak
		return nil, fmt.Errorf("aegis-client: failed to set read deadline: %w", err)
	}

	if err := conn.SetWriteDeadline(now.Add(c.config.WriteTimeout)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("aegis-client: failed to set write deadline: %w", err)
	}

	return conn, nil
}
