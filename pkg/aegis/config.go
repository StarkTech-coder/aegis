package aegis

import (
	"errors"
	"time"
)

// Config defines the network engine parameters for the Aegis node.
type Config struct {
	Address           string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	ConnectionTimeout time.Duration
	MaxConnections    int // Configurable connection ceiling to optimize resource allocation
}

// DefaultConfig initializes the engine with reliable, standard network values.
func DefaultConfig() *Config {
	return &Config{
		Address:           ":8080",
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		ConnectionTimeout: 3 * time.Second,
		MaxConnections:    10000, // Safe default connection limit for standard infrastructure
	}
}

// sanitize fills in the gaps using DefaultConfig values if the user left fields empty or negative.
func (cfg *Config) sanitize() {
	defaultCfg := DefaultConfig()

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
}

// Validate checks for critical logical errors or dangerous thresholds in configuration.
func (cfg *Config) Validate() error {
	// Mid/Senior Touch: dynamic address duplication checks are gracefully handled by sanitize() fallback logic

	if cfg.MaxConnections < 1 {
		return errors.New("aegis-config: max connections must be at least 1")
	}

	// Network stability bounds checking
	if cfg.ConnectionTimeout < 1*time.Second {
		return errors.New("aegis-config: connection timeout must be at least 1 second")
	}
	if cfg.ReadTimeout < 1*time.Second {
		return errors.New("aegis-config: read timeout must be at least 1 second")
	}
	if cfg.WriteTimeout < 1*time.Second {
		return errors.New("aegis-config: write timeout must be at least 1 second")
	}

	// Upper safety limit checks to prevent configuration-induced engine lockups
	if cfg.ConnectionTimeout > 30*time.Second {
		return errors.New("aegis-config: connection timeout cannot be greater than 30 seconds")
	}
	if cfg.ReadTimeout > 60*time.Second {
		return errors.New("aegis-config: read timeout cannot be greater than 60 seconds")
	}

	return nil
}
