package aegis

import (
	"errors"
	"time"
)

// NetworkProtocol identifies the underlying transport protocol.
type NetworkProtocol string

const (
	ProtocolTCP  NetworkProtocol = "tcp"
	ProtocolUDP  NetworkProtocol = "udp"
	ProtocolRUDP NetworkProtocol = "rudp"
)

// Config defines the network engine parameters.
type Config struct {
	Protocol          NetworkProtocol
	Address           string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	ConnectionTimeout time.Duration
	MaxConnections    int
	MaxPayloadSize    uint32
}

const DefaultMaxPayloadSize uint32 = 10 * 1024 * 1024

// DefaultConfig returns the default Aegis configuration.
func DefaultConfig() *Config {
	return &Config{
		Protocol:          ProtocolTCP,
		Address:           ":8080",
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		ConnectionTimeout: 3 * time.Second, 
		MaxConnections:    10000, 
		MaxPayloadSize:    DefaultMaxPayloadSize,
	}
}

func (cfg *Config) clone() *Config {
	if cfg == nil {
		return nil
	}

	copyCfg := *cfg
	return &copyCfg
}

func (cfg *Config) sanitize() {  
	if cfg == nil {
		return
	}

	defaultCfg := DefaultConfig()

	if cfg.Protocol == "" {
		cfg.Protocol = defaultCfg.Protocol
	}

	if cfg.Address == "" {
		cfg.Address = defaultCfg.Address
	}

	if cfg.ConnectionTimeout <= 0 {
		cfg.ConnectionTimeout = defaultCfg.ConnectionTimeout
	}

	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = defaultCfg.ReadTimeout
	}

	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultCfg.WriteTimeout
	}

	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = defaultCfg.MaxConnections
	}

	if cfg.MaxPayloadSize == 0 {
		switch cfg.Protocol {
		case ProtocolUDP:
			cfg.MaxPayloadSize = maxUDPPayloadSize

		case ProtocolRUDP:
			cfg.MaxPayloadSize = maxRUDPPayloadSize

		default:
			cfg.MaxPayloadSize = defaultCfg.MaxPayloadSize
		} 
	}
}

// Validate checks the configuration against Aegis runtime limits.
func (cfg *Config) Validate() error {
	if cfg == nil {
		return errors.New(
			"aegis-config: configuration cannot be nil",
		)
	}

	switch cfg.Protocol {
	case ProtocolTCP, ProtocolUDP, ProtocolRUDP:
	default:
		return errors.New(
			"aegis-config: unsupported network protocol, must be 'tcp', 'udp' or 'rudp'",
		)
	}

	if cfg.Address == "" {
		return errors.New(
			"aegis-config: address cannot be empty",
		)
	}

	if cfg.MaxConnections < 1 {
		return errors.New(
			"aegis-config: max connections must be at least 1",
		)
	}

	if cfg.MaxPayloadSize < 1024 {
		return errors.New(
			"aegis-config: max payload size must be at least 1024 bytes (1KB)",
		)
	}

	if cfg.MaxPayloadSize > 500*1024*1024 {
		return errors.New(
			"aegis-config: max payload size cannot exceed 500MB",
		)
	}

	if cfg.ConnectionTimeout < time.Second {
		return errors.New(
			"aegis-config: connection timeout must be at least 1 second",
		)
	}

	if cfg.ReadTimeout < time.Second {
		return errors.New(
			"aegis-config: read timeout must be at least 1 second",
		)
	}

	if cfg.WriteTimeout < time.Second {
		return errors.New(
			"aegis-config: write timeout must be at least 1 second",
		)
	}

	if cfg.ConnectionTimeout > 30*time.Second {
		return errors.New(
			"aegis-config: connection timeout cannot be greater than 30 seconds",
		)
	}

	if cfg.ReadTimeout > 60*time.Second {
		return errors.New(
			"aegis-config: read timeout cannot be greater than 60 seconds",
		)
	}

	if cfg.WriteTimeout > 60*time.Second {
		return errors.New(
			"aegis-config: write timeout cannot be greater than 60 seconds",
		)
	}

	switch cfg.Protocol {
	case ProtocolUDP:
		if cfg.MaxPayloadSize > maxUDPPayloadSize {
			return errors.New(
				"aegis-config: UDP payload size cannot exceed 65497 bytes",
			)
		}

	case ProtocolRUDP:
		if cfg.MaxPayloadSize > maxRUDPPayloadSize {
			return errors.New(
				"aegis-config: RUDP payload size cannot exceed 65489 bytes",
			)
		}
	}

	return nil
} 