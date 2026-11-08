package aegis

import (
	"crypto/tls"
	"errors"
	"time"
)

// NetworkProtocol identifies the underlying transport protocol.
type NetworkProtocol string

const (
	ProtocolTCP  NetworkProtocol = "tcp"
	ProtocolUDP  NetworkProtocol = "udp"
	ProtocolRUDP NetworkProtocol = "rudp"
	ProtocolQUIC NetworkProtocol = "quic"
)

// Config defines the network engine parameters.
type Config struct {
	Protocol                     NetworkProtocol
	Address                      string
	ReadTimeout                  time.Duration
	WriteTimeout                 time.Duration
	ConnectionTimeout            time.Duration
	MaxConnections               int
	MaxPayloadSize               uint32
	RUDPInitialRetransmitTimeout time.Duration
	RUDPMaxRetransmitTimeout     time.Duration
	RUDPMaxRetries               int
	RUDPMaxInFlight              int
	RUDPReceiveQueueSize         int
	QUICHandshakeTimeout         time.Duration
	QUICIdleTimeout              time.Duration
	QUICKeepAlivePeriod          time.Duration
	QUICResponseQueueSize        int
	QUICMaxIncomingStreams       int64
	QUICCertFile                 string
	QUICKeyFile                  string
	QUICCACertFile               string
	QUICClientCAFile             string
	QUICRequireClientCert        bool
	QUICServerName               string
	QUICALPN                     string
	QUICTLSConfig                *tls.Config
}

const DefaultMaxPayloadSize uint32 = 10 * 1024 * 1024

// DefaultConfig returns the default Aegis configuration.
func DefaultConfig() *Config {
	return &Config{
		Protocol:                     ProtocolTCP,
		Address:                      ":8080",
		ReadTimeout:                  5 * time.Second,
		WriteTimeout:                 5 * time.Second,
		ConnectionTimeout:            3 * time.Second,
		MaxConnections:               10000,
		MaxPayloadSize:               DefaultMaxPayloadSize,
		RUDPInitialRetransmitTimeout: 200 * time.Millisecond,
		RUDPMaxRetransmitTimeout:     2 * time.Second,
		RUDPMaxRetries:               5,
		RUDPMaxInFlight:              128,
		RUDPReceiveQueueSize:         256,
		QUICHandshakeTimeout:         5 * time.Second,
		QUICIdleTimeout:              30 * time.Second,
		QUICKeepAlivePeriod:          10 * time.Second,
		QUICResponseQueueSize:        256,
		QUICMaxIncomingStreams:       256,
		QUICALPN:                     "aegis-quic-v1",
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

	if cfg.RUDPInitialRetransmitTimeout <= 0 {
		cfg.RUDPInitialRetransmitTimeout = defaultCfg.RUDPInitialRetransmitTimeout
	}

	if cfg.RUDPMaxRetransmitTimeout <= 0 {
		cfg.RUDPMaxRetransmitTimeout = defaultCfg.RUDPMaxRetransmitTimeout
	}

	if cfg.RUDPMaxRetries <= 0 {
		cfg.RUDPMaxRetries = defaultCfg.RUDPMaxRetries
	}

	if cfg.RUDPMaxInFlight <= 0 {
		cfg.RUDPMaxInFlight = defaultCfg.RUDPMaxInFlight
	}

	if cfg.RUDPReceiveQueueSize <= 0 {
		cfg.RUDPReceiveQueueSize = defaultCfg.RUDPReceiveQueueSize
	}

	if cfg.QUICHandshakeTimeout <= 0 {
		cfg.QUICHandshakeTimeout = defaultCfg.QUICHandshakeTimeout
	}

	if cfg.QUICIdleTimeout <= 0 {
		cfg.QUICIdleTimeout = defaultCfg.QUICIdleTimeout
	}

	if cfg.QUICKeepAlivePeriod <= 0 {
		cfg.QUICKeepAlivePeriod = defaultCfg.QUICKeepAlivePeriod
	}

	if cfg.QUICResponseQueueSize <= 0 {
		cfg.QUICResponseQueueSize = defaultCfg.QUICResponseQueueSize
	}

	if cfg.QUICMaxIncomingStreams <= 0 {
		cfg.QUICMaxIncomingStreams = defaultCfg.QUICMaxIncomingStreams
	}

	if cfg.QUICALPN == "" {
		cfg.QUICALPN = defaultCfg.QUICALPN
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
	case ProtocolTCP, ProtocolUDP, ProtocolRUDP, ProtocolQUIC:
	default:
		return errors.New(
			"aegis-config: unsupported network protocol, must be 'tcp', 'udp', 'rudp' or 'quic'",
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

	if cfg.RUDPInitialRetransmitTimeout < 10*time.Millisecond {
		return errors.New(
			"aegis-config: RUDP initial retransmission timeout must be at least 10ms",
		)
	}

	if cfg.RUDPMaxRetransmitTimeout < cfg.RUDPInitialRetransmitTimeout {
		return errors.New(
			"aegis-config: RUDP max retransmission timeout must be greater than or equal to initial timeout",
		)
	}

	if cfg.RUDPMaxRetransmitTimeout > 30*time.Second {
		return errors.New(
			"aegis-config: RUDP max retransmission timeout cannot exceed 30 seconds",
		)
	}

	if cfg.RUDPMaxRetries < 1 {
		return errors.New(
			"aegis-config: RUDP max retries must be at least 1",
		)
	}

	if cfg.RUDPMaxRetries > 32 {
		return errors.New(
			"aegis-config: RUDP max retries cannot exceed 32",
		)
	}

	if cfg.RUDPMaxInFlight < 1 {
		return errors.New(
			"aegis-config: RUDP max in-flight packets must be at least 1",
		)
	}

	if cfg.RUDPReceiveQueueSize < 1 {
		return errors.New(
			"aegis-config: RUDP receive queue size must be at least 1",
		)
	}

	if cfg.QUICHandshakeTimeout < time.Second {
		return errors.New(
			"aegis-config: QUIC handshake timeout must be at least 1 second",
		)
	}

	if cfg.QUICHandshakeTimeout > 30*time.Second {
		return errors.New(
			"aegis-config: QUIC handshake timeout cannot exceed 30 seconds",
		)
	}

	if cfg.QUICIdleTimeout < 2*time.Second {
		return errors.New(
			"aegis-config: QUIC idle timeout must be at least 2 seconds",
		)
	}

	if cfg.QUICIdleTimeout > 5*time.Minute {
		return errors.New(
			"aegis-config: QUIC idle timeout cannot exceed 5 minutes",
		)
	}

	if cfg.QUICKeepAlivePeriod < time.Second {
		return errors.New(
			"aegis-config: QUIC keepalive period must be at least 1 second",
		)
	}

	if cfg.QUICKeepAlivePeriod > cfg.QUICIdleTimeout {
		return errors.New(
			"aegis-config: QUIC keepalive period cannot exceed QUIC idle timeout",
		)
	}

	if cfg.QUICResponseQueueSize < 1 {
		return errors.New(
			"aegis-config: QUIC response queue size must be at least 1",
		)
	}

	if cfg.QUICMaxIncomingStreams < 1 {
		return errors.New(
			"aegis-config: QUIC max incoming streams must be at least 1",
		)
	}

	if cfg.QUICALPN == "" {
		return errors.New(
			"aegis-config: QUIC ALPN cannot be empty",
		)
	}

	if cfg.QUICTLSConfig != nil && cfg.QUICTLSConfig.InsecureSkipVerify {
		return errors.New(
			"aegis-config: QUIC TLS config must not enable InsecureSkipVerify",
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
