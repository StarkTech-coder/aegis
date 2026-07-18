package aegis

import (
	"fmt"
	"net"
)

// Client represents the Aegis network engine client for outbound node connections.
type Client struct {
	config *Config
}

// NewClient initializes a new Client instance with the given configuration.
func NewClient(cfg *Config) *Client {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	return &Client{config: cfg}
}

// Connect establishes a TCP connection to a target node address.
func (c *Client) Connect(targetAddress string) (net.Conn, error) {
	// Dial Timeout enables our client to stop waiting if the target node is dead
	conn, err := net.DialTimeout("tcp", targetAddress, c.config.ReadTimeout)
	if err != nil {
		return nil, fmt.Errorf("aegis-client: failed to connect to target node %s: %w", targetAddress, err)
	}

	return conn, nil
}
